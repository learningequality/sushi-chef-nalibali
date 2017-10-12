#!/usr/bin/env python

import os
import logging
import requests
import json
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
    HOSTNAME = 'nalibali.org'
    ROOT_URL = f'http://{HOSTNAME}/story-library'
    DATA_DIR = 'chefdata'
    TREES_DATA_DIR = os.path.join(DATA_DIR, 'trees')
    CRAWLING_STAGE_OUTPUT = 'web_resource_tree.json'
    SCRAPING_STAGE_OUTPUT = 'ricecooker_json_tree.json'

    def __init__(self, html, logger):
        self._html = html
        self._logger = logger

    def __absolute_url(self, url):
        if url.startswith("//"):
            return "https:" + url
        elif url.startswith("/"):
            return f'http://{NalibaliChef.HOSTNAME}{url}'
        return url


    def __to_story_hierarchy(self, div):
        title = div.find('h2').get_text().strip()
        image_url = div.find('img', class_='img-responsive')['src']
        body_text = div.find('div', class_='body').get_text()
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
        story_hierarchies = list(map(self.__to_story_hierarchy, stories_divs))
        return story_hierarchies
        # return list(map(self._crawl_story_hierarchy, story_hierarchies))

    def _crawl_all_pages(page):
        pass

    def _crawl_story_hierarchy(self, hierarchy):
        stories_url = hierarchy['stories_url']
        stories_first_page =self._html.get(stories_url)
        self._crawl_all_pages(stories_first_page)

    # Crawling
    # For every story hierarchy:
    #   Starting at the root page
    #     For every page:
    #       For every story:
    #         For every language the story is in:
    #           Keep language->[story URL]
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
