#!/usr/bin/env python3
# -*- coding: utf-8 -*-

from pathlib import Path
import urllib.request
import urllib.parse
import json
import os
from itertools import product
import pandas as pd
from tqdm import tqdm
import time
from utils import touch

ENDPOINT = 'https://core.ac.uk/api-v2'
SEARCH_METHOD = '/articles/search'
SLEEP_BETWEEN_REQUESTS_S = 1 # to avoid overloading api

def convert_to_type_str(var):
    if type(var) == int:
        return str(var)
    elif type(var) == str:
        return ('"%s"' % var)
    else:
        raise ValueError('%s variable type not supported' % (str(type(var))))

def gen_query_string(q_tup):
        params = [('%s:%s' % (k,convert_to_type_str(v))) for k,v in q_tup]
        return '(%s)' % (' AND '.join(params)) # currently supporting AND only

# Adapted from https://github.com/oacore/or2016-api-demo/blob/master/OR2016%20API%20demo.ipynb  
class CoreApiRequestor:

    def __init__(self, endpoint, api_key):
        self.endpoint = endpoint
        self.api_key = api_key
        #defaults
        self.pagesize = 100
        self.page = 1

        
    def parse_response(self, decoded):
        res = []
        for item in decoded['data']:
            doi = None
            if 'identifiers' in item:
                for identifier in item['identifiers']:
                    if identifier and identifier.startswith('doi:'):
                        doi = identifier
                        break
            res.append([item['title'], doi])
        return res

    def request_url(self, url):
        with urllib.request.urlopen(url) as response:
            html = response.read()
        return html

    # use get method to request a url of specified page, with or without paper fulltext
    def get_method_query_request_url(self, method, query, fullText, page):
        if (fullText):
            fullText = 'true'
        else:
            fullText = 'false'
        params = {
            'apiKey':self.api_key,
            'page':page,
            'pageSize':self.pagesize,
            'fulltext':fullText
        }
        return self.endpoint + method + '/' + urllib.parse.quote(query) + '?' + urllib.parse.urlencode(params)

    # perform single query and return all resulting pages
    def get_base_query(self,method,query,fulltext, max_pages=100):
        url = self.get_method_query_request_url(method, query, fulltext, 1)
        all_articles=[]
        resp = self.request_url(url)
        result = json.loads(resp.decode('utf-8'))
        print("\rFetching %d papers..." % (result['totalHits'])),
        all_articles.append(result)
        if (result['totalHits'] > self.pagesize):
            num_pages = int(result['totalHits'] / self.pagesize) + (result['totalHits'] % self.pagesize != 0) #rounds down
            if (num_pages > max_pages):
                print("Search exceeded maximum pages (%d > %d), consider breaking it down into finer grained parts" % (num_pages, max_pages))
                num_pages = max_pages
            for i in range(2, num_pages + 1): # until num pages inclusive
                url = self.get_method_query_request_url(method,query, fulltext, i)
                resp =self.request_url(url)
                all_articles.append(json.loads(resp.decode('utf-8')))
                time.sleep(SLEEP_BETWEEN_REQUESTS_S)
        return all_articles
    
    # Handle a query over multiple parameters (year, topics, repositories, etc..)
    def handle_query(self, q_output_dir, query_params, method):
        print ('Saving query to  %s... (This may take a while)' % (q_output_dir))
        try:
            os.makedirs(q_output_dir)
        except:
            pass
        arrays = [v for (k,v) in query_params.values()]
        sub_query_keys = [k for (k,v) in query_params.values()]
        all_sub_query_params = list(product(*arrays))
        # convert the full query into all composing base queries
        for i,params in tqdm(enumerate(all_sub_query_params), total=len(all_sub_query_params)):
            sub_query_string = gen_query_string(zip(sub_query_keys, params))
            base_output_name = q_output_dir / ('_'.join([str(x) for x in params]) + '_' + str(i))
            output_lck = Path(str(base_output_name) + '.lck')
            # simple lock mechanism to enable running concurrently in multiple processes
            if output_lck.exists():
                continue
            touch(output_lck)
            pages = self.get_base_query(method, sub_query_string, fulltext=True)
            for p_num,page in enumerate(pages):
                self.save_page(page, Path(str(base_output_name) + '_' + str(p_num) + '.json'))
        # remove temporary files
        for lck_file in q_output_dir.glob('*.lck'):
            os.remove(lck_file)
        
    def save_page(self, page, output_name):
        if page['data']:
            data_df = pd.DataFrame(page['data'])
            data_df.to_json(str(output_name))