#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Miscellaneous utility funcs.
"""

import re
import os
from textacy.preprocess import preprocess_text

# Return only ascii chars
def clean_text(text):
    return ''.join([i if ord(i) < 128 else ' ' for i in text])

def normalize(text):
    res = ' '.join([ x.lower() for x in re.findall(r"[\w'-]+", text)])
    return res

def powerset(seq):
    """
    Returns all the subsets of this set. This is a generator.
    """
    if len(seq) <= 1:
        yield seq
        yield []
    else:
        for item in powerset(seq[1:]):
            yield [seq[0]]+item
            yield item

def count_lines(fname):
    with open(fname) as file:
        num_lines = sum(1 for line in file)
    return num_lines

def yield_lines(file, whatlines):
    return (x for i, x in enumerate(file) if i in whatlines)

def touch(fname, times=None):
    with open(fname, 'a'):
        os.utime(fname, times)
        
def get_file_line_offsets(fname):
    line_offsets = []
    offset = 0
    with open(fname, 'r') as f:
        for line in f:
            line_offsets.append(offset)
            offset += len(line)
    return line_offsets
 
def preprocess_text_by_config(text, textacy_defs):
    return preprocess_text(text, fix_unicode=textacy_defs['fix_unicode'], lowercase=textacy_defs['lowercase'], transliterate=textacy_defs['transliterate'], no_urls=textacy_defs['no_urls'], no_emails=textacy_defs['no_emails'], no_phone_numbers=textacy_defs['no_phone_numbers'], no_numbers=textacy_defs['no_numbers'], no_currency_symbols=textacy_defs['no_currency_symbols'], no_punct=textacy_defs['no_punct'], no_contractions=textacy_defs['no_contractions'], no_accents=textacy_defs['no_accents'])

