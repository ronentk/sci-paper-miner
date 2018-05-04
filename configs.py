#!/usr/bin/env python3
# -*- coding: utf-8 -*-

from pathlib import Path
root = Path(__file__).parent.absolute()
data_root = root / Path("data")

core_dataset_name = data_root / Path('arxiv_2006-2017_cs')
core_cs_query_path = core_dataset_name / Path('raw_query')
core_cs_db_path = core_dataset_name / Path('db')


textacy_preprocess_defs = {
                "fix_unicode": True,
                "lowercase": False,
                "transliterate": True,
                "no_urls": True,
                "no_emails": True,
                "no_phone_numbers": True,
                "no_numbers": False,
                "no_currency_symbols": True,
                "no_punct": False,
                "no_contractions": True,
                "no_accents": True
                }