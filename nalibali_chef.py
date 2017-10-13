#!/usr/bin/env python

import os
import logging
import requests
import json
from re import compile
from bs4 import BeautifulSoup

from le_utils.constants import content_kinds, licenses
from ricecooker.chefs import JsonTreeChef
from ricecooker.classes.licenses import get_license
from ricecooker.utils.caching import CacheForeverHeuristic, FileCache, CacheControlAdapter, InvalidatingCacheControlAdapter
from ricecooker.utils.html import download_file
from ricecooker.utils.jsontrees import write_tree_to_json_tree
from ricecooker.utils.zip import create_predictable_zip

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

# Chef
class NalibaliChef(JsonTreeChef):

    # Constants
    HOSTNAME = 'nalibali.org'
    ROOT_URL = f'http://{HOSTNAME}/story-library'
    DATA_DIR = 'chefdata'
    TREES_DATA_DIR = os.path.join(DATA_DIR, 'trees')
    CRAWLING_STAGE_OUTPUT = 'web_resource_tree.json'
    SCRAPING_STAGE_OUTPUT = 'ricecooker_json_tree.json'

    # Matching regexes
    STORY_PAGE_LINK_RE = compile(r'^.+page=(?P<page>\d+)$')

    def __init__(self, html, logger):
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

    def __to_story_hierarchy(self, div):
        title = self.__get_text(div.find('h2'))
        image_url = div.find('img', class_='img-responsive')['src']
        body_text = self.__get_text(div.find('div', class_='body'))
        stories_url = self.__absolute_url(div.find('div', class_='views-field').find('a', class_='btn link')['href'])
        return dict(
            title=title,
            image_url=image_url,
            body_text=body_text,
            stories_url=stories_url,
        )

    def _crawl_story_hierarchies(self, page):
        content_div = page.find('div', class_='region-content')
        vocabulary_div = content_div.find('div', class_='view-vocabulary')
        stories_divs = vocabulary_div.find_all('div', 'views-row')
        story_hierarchies  = [h for h in map(self.__to_story_hierarchy, stories_divs) if h['title'] == "Multilingual stories"]
        stories_dict = dict(map(self._crawl_story_hierarchy, story_hierarchies))
        for h in story_hierarchies:
            stories = stories_dict.get(h['stories_url'])
            if stories:
                h['children'] = stories
        return story_hierarchies

    def _to_pagination(self, anchor):
        href = anchor['href']
        m = NalibaliChef.STORY_PAGE_LINK_RE.match(href)
        if not m:
            raise Exception('STORY_PAGE_LINK_RE could not match')
        groups = m.groupdict()
        pagination=dict(
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
        author = self.__get_text(div.find('div', class_='field-author'))
        links = div.find('div', class_='links')
        anchors = links.find_all('a') if links else []
        story_by_language = {
            self.__get_text(anchor).lower(): self.__absolute_url(anchor['href'])
            for anchor in anchors
        }
        return dict(
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

    def _crawl_story_hierarchy(self, hierarchy):
        stories_url = hierarchy['stories_url']
        paginations = self._crawl_pagination(stories_url)
        paginations.insert(0, dict(
                url=stories_url,
                page=0,
                name='1',
            ))
        all_stories_by_bucket = list(map(self._crawl_pagination_stories, paginations))
        stories_by_language = {}
        for stories_bucket in all_stories_by_bucket:
            for story in stories_bucket:
                for lang, url in story['supported_languages'].items():
                    stories = stories_by_language.get(lang)
                    if not stories:
                        stories = []
                        stories_by_language[lang] = stories
                    stories.append(url)
        return stories_url, stories_by_language

    # Crawling
    def crawl(self, args, options):
        root_page = self._html.get(NalibaliChef.ROOT_URL)
        story_hierarchies = self._crawl_story_hierarchies(root_page)
        web_resource_tree = dict(
            kind="NalibaliWebResourceTree",
            title="Nalibali Web Resource Tree",
            language='en',
            children=story_hierarchies,
        )
        json_file_name = os.path.join(NalibaliChef.TREES_DATA_DIR, NalibaliChef.CRAWLING_STAGE_OUTPUT)
        with open(json_file_name, 'w') as json_file:
            json.dump(web_resource_tree, json_file, indent=2)
            self._logger.info('Crawling results stored in ' + json_file_name)
        return story_hierarchies

    def scrape(self, args, options):
        kwargs = {}     # combined dictionary of argparse args and extra options
        kwargs.update(args)
        kwargs.update(options)
        json_tree_path = self.get_json_tree_path(**kwargs)
        pass

    def pre_run(self, args, options):
        self.crawl(args, options)
        self.scrape(args, options)


    def get_json_tree_path(self, **kwargs):
        json_tree_path = os.path.join(NalibaliChef.TREES_DATA_DIR, NalibaliChef.SCRAPING_STAGE_OUTPUT)
        return json_tree_path

def __get_testing_chef():
    http_session = create_http_session(NalibaliChef.HOSTNAME)
    logger = create_logger()
    return NalibaliChef(Html(http_session, logger), logger)

if __name__ == '__main__':
    http_session = create_http_session(NalibaliChef.HOSTNAME)
    logger = create_logger()
    NalibaliChef(Html(http_session, logger), logger).main()
