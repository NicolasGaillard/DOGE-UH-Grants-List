import os
from datetime import datetime
from time import sleep

import numpy as np
import pandas as pd
import requests as req
import validators
from bs4 import BeautifulSoup
from ratelimit import limits, sleep_and_retry
from selenium.webdriver import Firefox
from selenium.webdriver.common.by import By
from selenium.webdriver.firefox.options import Options
from tqdm import tqdm

# Set up absolute path to data directory
SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
DATA_DIR = os.path.join(SCRIPT_DIR, 'data')
os.makedirs(DATA_DIR, exist_ok=True)

N_REQ = 10
LIMIT_S = 3

# ... your full data_key_dict here ...

# (other unchanged functions: clean_pre_df, all your scraping, processing, diffing functions)

def clean_pre_df(df):
    df = df.fillna('')
    return df

def safe_load_csv(filename):
    filepath = os.path.join(DATA_DIR, filename)
    df = pd.read_csv(filepath) if os.path.exists(filepath) else pd.DataFrame([])
    if 'uploaded_dt' in df.keys():
        df['uploaded_dt'] = pd.to_datetime(df['uploaded_dt'])
    return df

def load_pre_data():
    pre_contract_df = safe_load_csv('doge-contract.csv')
    pre_contract_df = clean_pre_df(pre_contract_df)
    pre_grant_df = safe_load_csv('doge-grant.csv')
    pre_grant_df = clean_pre_df(pre_grant_df)
    pre_property_df = safe_load_csv('doge-property.csv')
    pre_property_df = clean_pre_df(pre_property_df)
    return pre_contract_df, pre_grant_df, pre_property_df

def save_doge_data(contract_df, grant_df, property_df, stub_contract_df, stub_grant_df, stub_property_df):
    contract_df.to_csv(os.path.join(DATA_DIR, 'doge-contract.csv'), index=False)
    grant_df.to_csv(os.path.join(DATA_DIR, 'doge-grant.csv'), index=False)
    property_df.to_csv(os.path.join(DATA_DIR, 'doge-property.csv'), index=False)
    stub_contract_df.to_csv(os.path.join(DATA_DIR, 'doge-contract-stub.csv'), index=False)
    stub_grant_df.to_csv(os.path.join(DATA_DIR, 'doge-grant-stub.csv'), index=False)
    stub_property_df.to_csv(os.path.join(DATA_DIR, 'doge-property-stub.csv'), index=False)

# ... ALL your other functions from the original (scrape_doge, extend_grant_data, etc.) ...

def update_doge_data():
    # ... FULL CONTENTS from your original function ...
    # use the same logic as before!

def main():
    contract_df, grant_df, property_df, stub_contract_df, stub_grant_df, stub_property_df = update_doge_data()
    save_doge_data(contract_df, grant_df, property_df, stub_contract_df, stub_grant_df, stub_property_df)

if __name__ == '__main__':
    main()
