#!/usr/bin/env python

import os
import logging
import requests
import json
from re import I as IgnoreCase
from re import compile
from bs4 import BeautifulSoup
import tempfile
import shutil
import pathlib
from urllib.parse import urlparse
from pathlib import PurePosixPath

from le_utils.constants import content_kinds, licenses
from le_utils.constants.languages import getlang_by_native_name, getlang_by_name
from ricecooker.chefs import JsonTreeChef
from ricecooker.classes.licenses import get_license
from ricecooker.utils.caching import CacheForeverHeuristic, FileCache, CacheControlAdapter, InvalidatingCacheControlAdapter
from ricecooker.utils.html import download_file
from ricecooker.utils.jsontrees import write_tree_to_json_tree
from ricecooker.utils.zip import create_predictable_zip
from ricecooker.classes.nodes import HTML5AppNode, AudioNode
from ricecooker.classes import files

# Logging settings
def create_logger():
    logging.getLogger("cachecontrol.controller").setLevel(logging.WARNING)
    logging.getLogger("requests.packages").setLevel(logging.WARNING)
    from ricecooker.config import LOGGER
    LOGGER.setLevel(logging.DEBUG)
    return LOGGER

def create_http_session(hostname):
    sess = requests.Session()
    cache = FileCache('.webcache')
    basic_adapter = CacheControlAdapter(cache=cache)
    forever_adapter = CacheControlAdapter(heuristic=CacheForeverHeuristic(), cache=cache)
    sess.mount('http://', basic_adapter)
    sess.mount('https://', basic_adapter)
    sess.mount('http://www.' + hostname, forever_adapter)
    sess.mount('https://www.' + hostname, forever_adapter)
    return sess

class Html:
    def __init__(self, http_session, logger):
        self._http_session = http_session
        self._logger = logger

    def get(self, url, *args, **kwargs):
        response = self._http_session.get(url, *args, **kwargs)
        if response.status_code != 200:
            self._logger.error("STATUS: {}, URL: {}", response.status_code, url)
        elif not response.from_cache:
            self._logger.debug("NOT CACHED:", url)
        return BeautifulSoup(response.content, "html.parser")

    def get_image(self, url):
        return self._http_session.get(url, stream=True)

    def get_xml(self, url):
        return BeautifulSoup(self._http_session.get(url).content, 'xml')

    def head(self, url):
        return self._http_session.head(url)

# Chef
class NalibaliChef(JsonTreeChef):

    # Constants
    HOSTNAME = 'nalibali.org'
    ROOT_URL = f'http://{HOSTNAME}/story-library'
    DATA_DIR = 'chefdata'
    TREES_DATA_DIR = os.path.join(DATA_DIR, 'trees')
    CRAWLING_STAGE_OUTPUT = 'web_resource_tree.json'
    SCRAPING_STAGE_OUTPUT = 'ricecooker_json_tree.json'
    ZIP_FILES_TMP_DIR = os.path.join(DATA_DIR, 'zipfiles')
    LICENSE = get_license(licenses.CC_BY_NC_ND, copyright_holder="Nal'ibali").as_dict()

    # Matching regexes
    STORY_PAGE_LINK_RE = compile(r'^.+page=(?P<page>\d+)$')
    SUPPORTED_THUMBNAIL_EXTENSIONS = compile(r'\.(png|jpg|jpeg)')
    AUTHOR_RE = compile(r'author:', IgnoreCase)
    AUDIO_STORIES_RE = compile(r'Audio Stories', IgnoreCase)
    AUDIO_STORY_ANCHOR_RE = compile(r'story-library/audio-stories')
    IONO_FM_RE = compile(f'iono.fm')
    RSS_FEED_RE = compile('/rss/chan')

    def __init__(self, html, logger):
        super(NalibaliChef, self).__init__(None, None)
        self._html = html
        self._logger = logger


    def __absolute_url(self, url):
        if url.startswith("//"):
            return "https:" + url
        elif url.startswith("/"):
            return f'http://{NalibaliChef.HOSTNAME}{url}'
        return url

    def __get_text(self, elem):
        return "" if elem is None else elem.get_text().replace('\r', '').replace('\n', ' ').strip()

    def __sanitize_author(self, text):
        if not text:
            return text
        new_text, _ = NalibaliChef.AUTHOR_RE.subn('', text)
        return new_text.strip()

    def __to_story_hierarchy(self, div):
        title = self.__get_text(div.find('h2'))
        image_url = div.find('img', class_='img-responsive')['src']
        body_text = self.__get_text(div.find('div', class_='body'))
        stories_url = self.__absolute_url(div.find('div', class_='views-field').find('a', class_='btn link')['href'])
        return dict(
            kind='NalibaliHierarchy',
            title=title,
            thumbnail=image_url,
            description=body_text,
            url=stories_url,
        )

    def _crawl_story_hierarchies(self, page):
        content_div = page.find('div', class_='region-content')
        vocabulary_div = content_div.find('div', class_='view-vocabulary')
        stories_divs = vocabulary_div.find_all('div', 'views-row')
        story_hierarchies = [h for h in map(self.__to_story_hierarchy, stories_divs)]
        stories_dict = dict(map(self._crawl_story_hierarchy, story_hierarchies))
        for h in story_hierarchies:
            stories = stories_dict.get(h['url'], {})
            h['children'] = stories
        return story_hierarchies

    def _to_pagination(self, anchor):
        href = anchor['href']
        m = NalibaliChef.STORY_PAGE_LINK_RE.match(href)
        if not m:
            raise Exception('STORY_PAGE_LINK_RE could not match')
        groups = m.groupdict()
        pagination=dict(
            kind='NalibaliPagination',
            url=self.__absolute_url(href),
            page=groups['page'],
            name=self.__get_text(anchor),
        )
        return pagination

    def _crawl_pagination(self, url):
        page = self._html.get(url)
        pagination_ul = page.find('ul', class_='pagination')

        if not pagination_ul:
            return []

        anchors = pagination_ul.find_all('a', attrs={'href': NalibaliChef.STORY_PAGE_LINK_RE})
        paginations = list(map(self._to_pagination, anchors))
        paginations_dict = {p['page']: p for p in paginations}
        actual_paginations = [p for p in paginations if ('next' not in p['name']  and 'last' not in p['name'] and 'first' not in p['name'] and 'previous' not in p['name'] and '>' not in p['name'] and p['name'] != '')]
        last = paginations_dict.get('last')
        if not last:
            return actual_paginations
        current_last = actual_paginations[-1]
        if current_last['page'] == last['page']:
            return actual_paginations
        return actual_paginations.extend(self._crawl_pagination(current_last['url']))

    def __process_language(self, language):
        lang = language.lower()
        if lang == 'sotho':
            return 'Sesotho'
        elif lang == 'ndebele':
            return 'North Ndebele'
        elif lang == 'tsivenda':
            return 'Tshivenda'
        else:
            return language

    def _to_story(self, div):
        title_elem = div.find('span', property='dc:title')
        title = ''
        if title_elem:
            title = title_elem['content']
        else:
            title_elem = div.find('div', class_='content')
            if not title_elem:
                return None
            title = self.__get_text(title_elem.find('h3'))

        if not title:
            return None

        posted_date = self.__get_text(div.find('div', class_='field-date'))
        author = self.__sanitize_author(self.__get_text(div.find('div', class_='field-author')))
        links = div.find('div', class_='links')
        anchors = links.find_all('a') if links else []
        image = div.find('img', class_='img-responsive') or div.find('img')
        image_src = image['src'] if image else ''
        thumbnail = image_src.split('?')[0] if NalibaliChef.SUPPORTED_THUMBNAIL_EXTENSIONS.search(image_src) else None

        language_and_hrefs = [None] * len(anchors)
        for i, (tentative_lang, href) in enumerate([(self.__process_language(self.__get_text(anchor)), anchor['href']) for anchor in anchors]):
            lang = tentative_lang if getlang_by_name(tentative_lang) or getlang_by_native_name(tentative_lang) else 'English'
            language_and_hrefs[i] = (lang, href)

        story_by_language = {
            language: dict(
                kind='NalibaliLocalizedStory',
                title=title,
                posted_date=posted_date,
                author=author,
                language=language,
                url=self.__absolute_url(href),
                thumbnail=thumbnail,
            )
            for language, href in language_and_hrefs
        }
        return dict(
            kind='NalibaliStory',
            title=title,
            posted_date=posted_date,
            author=author,
            supported_languages=story_by_language,
        )

    def _crawl_pagination_stories(self, pagination):
        url = pagination['url']
        page = self._html.get(url)
        content_views = page.find_all('div', class_='view-content')
        stories = []
        for content in content_views:
            stories.extend([story for story in map(self._to_story, content.find_all('div', class_='views-row')) if story])
        return stories

    def _crawl_audio_stories_hierarchy(self, hierarchy):
        stories_url = hierarchy['url']
        page  = self._html.get(stories_url)
        content = page.find('section', id='section-main').find('div', class_='region-content')
        language_info = [(self.__process_language(self.__get_text(anchor)), anchor['href']) for anchor in content.find_all('a', attrs={'href': NalibaliChef.AUDIO_STORY_ANCHOR_RE}) if not anchor.get('class') and len(self.__get_text(anchor)) > 2]
        stories_by_language = {}

        for lang, url in language_info:
            language_page = self._html.get(self.__absolute_url(url))
            language_iono_fm_url = language_page.find('a', attrs={'href': NalibaliChef.IONO_FM_RE })['href']
            language_iono_fm_page = self._html.get(language_iono_fm_url)
            rss_url = language_iono_fm_page.find('link', attrs={'href': NalibaliChef.RSS_FEED_RE })['href']
            rss_page = self._html.get_xml(rss_url)
            items = rss_page.find_all('item')
            stories = [None] * len(items)

            for i, item in enumerate(items):
                url = item.enclosure['url'].split('?')[0]
                filename = os.path.basename(url)
                filename_posix = PurePosixPath(filename)
                filename_no_extension = filename_posix.stem
                mp3_url = os.path.join(os.path.dirname(url), filename_no_extension) + '.mp3'
                mp3_version_exists = self._html.head(mp3_url).status_code == 200
                if not mp3_version_exists:
                    raise Exception(f'No mp3 version available for {url}')
                audio_node_url = mp3_url if mp3_version_exists else url
                parsed_url = urlparse(audio_node_url)

                stories[i] = dict(
                    title=self.__get_text(item.title),
                    source_id=parsed_url.path,
                    url=audio_node_url,
                    content_type=item.enclosure['type'],
                    description=self.__get_text(item.summary),
                    pub_date=self.__get_text(item.pubDate),
                    author=self.__get_text(item.author),
                    language=lang,
                    thumbnail=item.thumbnail['href'],
                )
            stories_by_language[lang] = stories
        return stories_url, stories_by_language

    def _crawl_story_hierarchy(self, hierarchy):
        if NalibaliChef.AUDIO_STORIES_RE.search(hierarchy['title']):
            return self._crawl_audio_stories_hierarchy(hierarchy)

        stories_url = hierarchy['url']
        paginations = self._crawl_pagination(stories_url)
        paginations.insert(0, dict(
                kind='NalibaliPagination',
                url=stories_url,
                page=0,
                name='1',
            ))
        all_stories_by_bucket = list(map(self._crawl_pagination_stories, paginations))
        stories_by_language = {}
        for stories_bucket in all_stories_by_bucket:
            for story in stories_bucket:
                for lang, story in story['supported_languages'].items():
                    by_language = stories_by_language.get(lang)
                    if not by_language:
                        by_language = (set(), [])
                        stories_by_language[lang] = by_language
                    uniques, stories = by_language
                    url = story['url']
                    if url not in uniques:
                        stories.append(story)
                    uniques.add(url)
        for lang, (uniques, stories) in stories_by_language.items():
            stories_by_language[lang] = stories
        return stories_url, stories_by_language

    # Crawling
    def crawl(self, args, options):
        root_page = self._html.get(NalibaliChef.ROOT_URL)
        story_hierarchies = self._crawl_story_hierarchies(root_page)
        web_resource_tree = dict(
            kind='NalibaliWebResourceTree',
            title="Nal'ibali Web Resource Tree",
            language='en',
            children=story_hierarchies,
        )
        json_file_name = os.path.join(NalibaliChef.TREES_DATA_DIR, NalibaliChef.CRAWLING_STAGE_OUTPUT)
        with open(json_file_name, 'w') as json_file:
            json.dump(web_resource_tree, json_file, indent=2)
            self._logger.info('Crawling results stored in ' + json_file_name)
        return story_hierarchies

    # Scraping
    def scrape(self, args, options):
        kwargs = {}     # combined dictionary of argparse args and extra options
        kwargs.update(args)
        kwargs.update(options)

        with open(os.path.join(NalibaliChef.TREES_DATA_DIR, NalibaliChef.CRAWLING_STAGE_OUTPUT), 'r') as json_file:
            web_resource_tree = json.load(json_file)
            assert web_resource_tree['kind'] == 'NalibaliWebResourceTree'

        ricecooker_json_tree = dict(
            source_domain=NalibaliChef.HOSTNAME,
            source_id="nal'ibali",
            title=web_resource_tree['title'],
            description="""Nal'ibali (isiXhosa for "here's the story") is a national reading-for-enjoyment campaign to spark children's potential through storytelling and reading.""",
            language='en',
            thumbnail='http://nalibali.org/sites/default/files/nalibali_logo.png',
            children=[],
        )
        hierarchies_map = { h['title']: h for h in web_resource_tree['children'] }
        children = [None] * len(hierarchies_map.keys())
        children[0] = self._scrape_hierarchy(hierarchies_map.get('Multilingual stories'), self._scrape_multilingual_story)
        children[1] = self._scrape_hierarchy(hierarchies_map.get('Audio stories'), self._scrape_audio_story)
        children[2] = self._scrape_hierarchy(hierarchies_map.get('Story cards'), self._scrape_story_card)
        children[3] = self._scrape_hierarchy(hierarchies_map.get('Story seeds'), self._scrape_story_seed)
        children[4] = self._scrape_hierarchy(hierarchies_map.get('Your stories'), self._scrape_your_story)
        ricecooker_json_tree['children'] = children
        write_tree_to_json_tree(os.path.join(NalibaliChef.TREES_DATA_DIR, NalibaliChef.SCRAPING_STAGE_OUTPUT) , ricecooker_json_tree)
        return ricecooker_json_tree

    def _scrape_hierarchy(self, hierarchy, story_scraping_func):
        assert hierarchy['kind'] == 'NalibaliHierarchy'
        items = hierarchy.get('children', {}).items()
        hierarchy_name = hierarchy['title'].replace(' ', '_')
        hierarchy_by_language = [None] * len(items)
        for i, (language, stories) in enumerate(items):
            stories_nodes = [story for story in map(story_scraping_func, stories) if story]
            topic_node = dict(
                kind=content_kinds.TOPIC,
                source_id=f'{hierarchy_name}_{language}',
                title=language,
                description=f'Stories in {language}',
                children=stories_nodes,
            )
            hierarchy_by_language[i] = topic_node
        hierarchy_title = hierarchy['title']
        return dict(
            kind=content_kinds.TOPIC,
            source_id=hierarchy_title,
            title=hierarchy_title,
            description=hierarchy['description'],
            children=hierarchy_by_language,
            thumbnail=hierarchy['thumbnail'],
        )


    def _scrape_download_image(self, base_path, img):
        url = img['src']

        if not url:
            return

        if url.startswith('http') or url.startswith('https'):
            absolute_url = url
            parsed_url = urlparse(url)
            relative_url = parsed_url.path
        else:
            absolute_url = self.__absolute_url(url)
            relative_url = url

        self._scrape_download_image_helper(base_path, img, absolute_url, relative_url)

    def _scrape_download_image_helper(self, base_path, img, absolute_url, relative_url):
        image_response = self._html.get_image(absolute_url)
        if image_response.status_code != 200:
            return
        filename = os.path.basename(relative_url)
        subdirs = os.path.dirname(relative_url).split('/')
        image_dir = os.path.join(base_path, *subdirs)
        pathlib.Path(image_dir).mkdir(parents=True, exist_ok=True)
        image_path = os.path.join(image_dir, filename)
        with open(image_path, 'wb') as f:
            image_response.raw.decode_content = True
            shutil.copyfileobj(image_response.raw, f)
        img['src'] = relative_url[1:] if relative_url[0] == '/' else relative_url

    def _scrape_story_html5(self, story):
        url = story['url']
        page = self._html.get(url)
        story_section = page.find('section', id='section-main')
        links_section = story_section.find('div', class_='languages-links')

        # Is there a way to cross link HTML5AppNode?
        if links_section:
            links_section.extract()

        title = self.__get_text(story_section.find('h1', class_='page-header'))
        language_code = getlang_by_native_name(story['language']).code
        dest_path = tempfile.mkdtemp(dir=NalibaliChef.ZIP_FILES_TMP_DIR)

        for img in story_section.find_all('img'):
            self._scrape_download_image(dest_path, img)

        basic_page_str = """
        <!DOCTYPE html>
        <html>
          <head>
            <meta charset="utf-8">
            <title></title>
          </head>
          <body>
          </body>
        </html>"""
        basic_page = BeautifulSoup(basic_page_str, "html.parser")
        body = basic_page.find('body')
        body.append(story_section)
        with open(os.path.join(dest_path, 'index.html'), 'w', encoding="utf8") as index_html:
            index_html.write(str(basic_page))
        zip_path = create_predictable_zip(dest_path)
        parsed_story_url = urlparse(url)
        return dict(
            kind=content_kinds.HTML5,
            source_id=parsed_story_url.path if parsed_story_url else url,
            title=title,
            language=language_code,
            description='',
            license=NalibaliChef.LICENSE,
            thumbnail=story['thumbnail'],
            files=[dict(
                file_type=content_kinds.HTML5,
                path=zip_path,
                language=language_code,
            )],
        )

    def _scrape_multilingual_story(self, story):
        return self._scrape_story_html5(story)

    def _scrape_audio_story(self, story):
        return dict(
            kind=content_kinds.AUDIO,
            source_id=story['source_id'],
            title=story['title'],
            license=NalibaliChef.LICENSE,
            author=story['author'],
            description=story['description'],
            domain_ns=NalibaliChef.HOSTNAME,
            thumbnail=story['thumbnail'],
            files=[
                dict(
                    file_type=content_kinds.AUDIO,
                    path=story['url'],
                    language=self.__get_language_code(story['language']),
                )
            ]
        )

    def __get_language_code(self, language_str):
        language = getlang_by_name(language_str) or getlang_by_native_name(language_str)
        lang_code = None
        if language:
            lang_code = language.code
        else:
            lang_code = getlang_by_name('English').code
            print('Unknown language:', language_str)
        return lang_code


    def _scrape_story_card(self, story):
        url = story['url']
        language_str = story['language']
        lang_code = self.__get_language_code(language_str)

        if url and url.endswith('.pdf'):
            parsed_url = urlparse(url)
            return dict(
                source_id=parsed_url.path,
                kind=content_kinds.DOCUMENT,
                title=story['title'],
                # description=story['description'],
                license=NalibaliChef.LICENSE,
                author=story['author'],
                thumbnail=story['thumbnail'],
                language=lang_code,
                files=[
                    dict(
                        file_type=content_kinds.DOCUMENT,
                        path=url,
                    )
                ]
            )
        raise Exception('Non-PDF version not implemented')

    def _scrape_story_seed(self, story):
        return self._scrape_story_html5(story)

    def _scrape_your_story(self, story):
        return self._scrape_story_html5(story)

    def pre_run(self, args, options):
        self.crawl(args, options)
        self.scrape(args, options)


def __get_testing_chef():
    http_session = create_http_session(NalibaliChef.HOSTNAME)
    logger = create_logger()
    return NalibaliChef(Html(http_session, logger), logger)

if __name__ == '__main__':
    http_session = create_http_session(NalibaliChef.HOSTNAME)
    logger = create_logger()
    chef = NalibaliChef(Html(http_session, logger), logger)
    chef.main()
