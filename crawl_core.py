#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import configs
import core_requestor
from core_requestor import CoreApiRequestor
from core_data_wrapper import CoreDataWrapper
import argparse


if __name__ == "__main__":
    
    description = ('Script to query CORE datasets. Papers fetched in query will be stored in metadata.json file and multiple fulltext json files containing the paper fulltexts.')
        
    parser = argparse.ArgumentParser(description=description)
    parser.add_argument(
        "api_key",
        help="api key for requests",
        type=str
    )
    
    args = parser.parse_args()
    
    ## create requestor for key supplied as argument
    core_req = CoreApiRequestor(core_requestor.ENDPOINT, args.api_key)
    
    ###### Query Params
    ## Include all CS topics - note that these are arXiv specific and not necessarily available for other paper repositories
    topics = ['Computer Science - Sound', 'Computer Science - General Literature', 'Computer Science - Neural and Evolutionary Computing', 'Computer Science - Mathematical Software', 'Computer Science - Computational Complexity', 'Computer Science - Artificial Intelligence', 'Computer Science - Multiagent Systems', 'Computer Science - Human-Computer Interaction', 'Computer Science - Cryptography and Security', 'Computer Science - Logic in Computer Science', 'Computer Science - Computer Vision and Pattern Recognition', 'Computer Science - Numerical Analysis', 'Computer Science - Discrete Mathematics', 'Computer Science - Computer Science and Game Theory', 'Computer Science - Information Retrieval', 'Computer Science - Social and Information Networks', 'Computer Science - Graphics', 'Computer Science - Emerging Technologies', 'Computer Science - Formal Languages and Automata Theory', 'Computer Science - Performance', 'Computer Science - Software Engineering', 'Computer Science - Databases', 'Computer Science - Hardware Architecture', 'Computer Science - Distributed, Parallel, and Cluster Computing', 'Computer Science - Information Theory', 'Computer Science - Networking and Internet Architecture', 'Computer Science - Computational Engineering, Finance, and Science', 'Computer Science - Other Computer Science', 'Computer Science - Learning', 'Computer Science - Multimedia', 'Computer Science - Robotics', 'Computer Science - Digital Libraries', 'Computer Science - Operating Systems', 'Computer Science - Systems and Control', 'Computer Science - Programming Languages', 'Computer Science - Computers and Society', 'Computer Science - Computational Geometry', 'Computer Science - Computation and Language', 'Computer Science - Symbolic Computation', 'Computer Science - Data Structures and Algorithms']
    years = range(2006,2018)
    repos = [144] # arXiv
    
    query_params = { 
                'repos': ('repositories.id', repos),    
                'topics': ('topics', topics),
                'years': ('year', years)
                }
    
    # Fetch query
    core_req.handle_query(configs.core_cs_query_path, query_params, core_requestor.SEARCH_METHOD)
    # And convert it to dataset form. Will be stored in metadata.json file 
    # and multiple fulltext json files containing the paper fulltexts.
    # Currently, access and preprocessing is on the fly for flexibilty in creating pipelines.
    core_data = CoreDataWrapper(configs.core_cs_db_path, lines_per_ft_file=10000)
    core_data.raw_query_to_dataset(configs.core_cs_query_path)

