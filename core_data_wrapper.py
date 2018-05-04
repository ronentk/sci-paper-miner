#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import json
import re
from pathlib import Path
import pandas as pd
from tqdm import tqdm
import numpy as np
import os
import traceback
import utils
from utils import preprocess_text_by_config
from textacy import preprocess
import shutil


num_re = re.compile('fulltext_(\d+).json')


class CoreDataWrapper(object):
    """
    Wraps raw data fetched using core api and allows access to it to a more manageable form. 
    
    TODO handle preprocessing data and saving to disk
    """
    def __init__(self, root_data_path, lines_per_ft_file=10000):
        self.data_path = Path(root_data_path)
        self.metadata_path = self.data_path / ('metadata.json')
        self.lines_per_ft_file = lines_per_ft_file
        self.metadata = pd.DataFrame()
        self.data_files = []
        self.textacy_defs = {
                            "fix_unicode": True,
                            "lowercase": True,
                            "transliterate": True,
                            "no_urls": True,
                            "no_emails": True,
                            "no_phone_numbers": True,
                            "no_numbers": True,
                            "no_currency_symbols": True,
                            "no_punct": False,
                            "no_contractions": True,
                            "no_accents": True
                            }
        

    def set_preprocess_defs(self, defs_dict):
        self.textacy_defs = defs_dict
    
    # Process single paper from raw json form. Supply a filter function to
    # skip papers not meeting specified criteria, and an extractor function to
    # handle what content will be extracted from the raw record.
    def process_line(self, line, filter_fn=None, extractor_fn=None):
        if not extractor_fn: extractor_fn = self.identity_extractor
        paper_dict = json.loads(line)
        if filter_fn:
            if not filter_fn(paper_dict):
                return None
            else:
                return extractor_fn(paper_dict)
        else:
            return extractor_fn(paper_dict)
        
        
    def raw_query_to_dataset(self, raw_query_root):
        metadata_dict = {}
        fulltext_dict = {}
        num_record = 0
        ft_file_num = 0
        output_dir = self.data_path
        try:
            os.makedirs(output_dir)
        except:
            print("Dataset folder %s already exists... exiting" % (output_dir))
            return
        for fname in tqdm(Path(raw_query_root).glob('*.json'), total=sum(1 for i in Path(raw_query_root).glob('*.json'))):
            try:
                df = pd.read_json(fname)
                df['repo_id'] = int(df.loc[1, df.columns == 'repositories'][0][0]['id'])
                df = df.drop(labels=['repositories'], axis=1)
                for i,row in df.iterrows():
                    try:
                        data_dict = row.to_dict()
                        line_num = num_record % self.lines_per_ft_file
                        data_dict['ft_file_num'] = ft_file_num
                        data_dict['ft_line_num'] = line_num
                        data_dict['num_record'] = num_record
                        metadata = {k:v for k,v in data_dict.items() if k != 'fullText'}
                        metadata_dict[num_record] = metadata
                        fulltext_dict[num_record] = data_dict
                        num_record += 1
                        if line_num == (self.lines_per_ft_file - 1):
                            pd.DataFrame.from_dict(fulltext_dict, orient='index').to_json(str(output_dir /( 'fulltext_%d.json' % (ft_file_num))), lines=True, orient="records")
                            fulltext_dict = {}
                            ft_file_num += 1
                            
                    except Exception as e:
                        print ("Error: ", str(e))
                        print(traceback.format_exc())
            except Exception as e:
                print ("Error with %s: %s" % (fname, str(e)))
                
        # Remove duplicate records (papers appearing under multiple topics, for example)
        md_df = pd.DataFrame.from_dict(metadata_dict, orient='index')
        md_df.drop_duplicates(subset=['id'], keep='first', inplace=True)
        md_df.drop_duplicates(subset=['oai'], keep='first', inplace=True)
        md_df.to_json(str(output_dir / ('metadata.json')), lines=True, orient="records")
        ft_df = pd.DataFrame.from_dict(fulltext_dict, orient='index')
        ft_df.drop_duplicates(subset=['id'], keep='first', inplace=True)
        ft_df.drop_duplicates(subset=['oai'], keep='first', inplace=True)
        ft_df.to_json(str(output_dir / ('fulltext_%d.json' % (ft_file_num))), lines=True, orient="records")
        
        # Remove temp. query files
        shutil.rmtree(raw_query_root)
        
            
    def identity_extractor(self, d):
        return d
    
    def fulltext_extractor(self, d, clean_text=True):
        if 'fullText' in d:
            fulltext = d['fullText']
            if clean_text:
                fulltext = preprocess.normalize_whitespace(fulltext)
                fulltext = preprocess_text_by_config(fulltext, self.textacy_defs)
            return fulltext
        else:
            return d
        
    def metadata_fulltext_pair_extractor(self, d, clean_text=True):
        ft = self.fulltext_extractor(d)
        d.pop('fullText',None)
        return (d,ft)
        
    def load_dataset(self):
        if not self.metadata_path.exists():
            print("No dataset exists at ", self.metadata_path)
            return
        print("Loading metadata file...")
        self.metadata = pd.read_json(self.metadata_path, lines=True)
        print("Loading data files...")
        self.data_files_info = [(data_fpath,utils.get_file_line_offsets(data_fpath)) for data_fpath in self.data_path.glob('fulltext_*')]
        self.data_files_info.sort(key=lambda x: int(num_re.findall(str(x))[0])) # sort by ascending order

 
    def fetch_record(self, record_num):
        file_num = self.metadata.loc[record_num,'ft_file_num']
        line_num = self.metadata.loc[record_num,'ft_line_num']
        return self.fetch_line(file_num, line_num)
        

    def fetch_line(self, file_num, line_num):
        fname, offsets = self.data_files_info[file_num]
        with open(fname, 'r') as f:
            f.seek(offsets[line_num])
            return f.readline()
        
    def fetch_paper(self, record_num, filter_fn=None, extractor_fn=None):
        file_num = self.metadata.loc[record_num,'ft_file_num']
        line_num = self.metadata.loc[record_num,'ft_line_num']
        raw_record = self.fetch_line(file_num, line_num)
        processed = self.process_line(raw_record, filter_fn, extractor_fn)
        return processed
    
    # Yields papers with specified preprocessing.
    # To generate a subset of the data, supply records idxs in form (start_idx,end_idx)
    def data_generator(self, filter_fn=None, extractor_fn=None, shuffle=False, records=None):
        order = np.arange(self.metadata.shape[0])
        if records:
            order = order[records[0]:records[1] + 1]
        if shuffle:
            np.random.shuffle(order)
        for i in order:
            try:
                processed = self.fetch_paper(i, filter_fn, extractor_fn)
                yield processed
            except KeyError:
                print("Error fetching paper %d" % (i))
                continue
