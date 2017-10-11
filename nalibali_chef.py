#!/usr/bin/env python

import os
import logging

from le_utils.constants import content_kinds, licenses
from ricecooker.chefs import JsonTreeChef
from ricecooker.classes.licenses import get_license
from ricecooker.utils.caching import CacheForeverHeuristic, FileCache, CacheControlAdapter, InvalidatingCacheControlAdapter
from ricecooker.utils.html import download_file
from ricecooker.utils.jsontrees import write_tree_to_json_tree
from ricecooker.utils.zip import create_predictable_zip

# Chef settings
DATA_DIR = 'chefdata'
TREES_DATA_DIR = os.path.join(DATA_DIR, 'trees')
CRAWLING_STAGE_OUTPUT = 'web_resource_tree.json'
SCRAPING_STAGE_OUTPUT = 'ricecooker_json_tree.json'

# Logging settings
logging.getLogger("cachecontrol.controller").setLevel(logging.WARNING)
logging.getLogger("requests.packages").setLevel(logging.WARNING)
from ricecooker.config import LOGGER
LOGGER.setLevel(logging.DEBUG)

# Crawling
def crawling_part(args, options):
    pass

# Scrapping
def scraping_part(args, options):
    pass

# Chef
class NalibaliChef(JsonTreeChef):
    def crawl(self, args, options):
        crawling_part(args, options)

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
        json_tree_path = os.path.join(TREES_DATA_DIR, SCRAPING_STAGE_OUTPUT)
        return json_tree_path


if __name__ == '__main__':
    NalibaliChef().main()
