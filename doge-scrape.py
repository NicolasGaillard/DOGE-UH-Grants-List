import os
from datetime import datetime

import numpy as np
import pandas as pd
import requests as req
import validators
from bs4 import BeautifulSoup
from ratelimit import limits, sleep_and_retry
from tqdm import tqdm

# Set up absolute path to data directory
SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
DATA_DIR = os.path.join(SCRIPT_DIR, 'data')
os.makedirs(DATA_DIR, exist_ok=True)

N_REQ = 10
LIMIT_S = 3

# --- Only needed for grant extension ---
def extend_grant_data(grant_df, dt):
    api_root = 'https://api.usaspending.gov/api/v2/awards/'
    usas_df = pd.DataFrame([])
    rh = req.utils.default_headers()
    for link in tqdm(grant_df.link.values):
        if validators.url(link):
            try:
                grant_id = os.path.basename(link)
                usas_req_url = os.path.join(api_root, grant_id)
                r = limit_req(usas_req_url, headers=rh)
                grant_row_df = pd.json_normalize(r.json(), sep='_')
                grant_row_df = grant_row_df.rename(columns={'description': 'description_usas'})
                usas_df = pd.concat([usas_df, grant_row_df], ignore_index=True)
            except:
                usas_df = pd.concat([usas_df, pd.DataFrame([], index=[0])], ignore_index=True)
        else:
            usas_df = pd.concat([usas_df, pd.DataFrame([], index=[0])], ignore_index=True)
    return pd.concat([grant_df.reset_index().drop('index', axis=1), usas_df], axis=1)

@sleep_and_retry
@limits(calls=N_REQ, period=LIMIT_S)
def limit_req(url, headers={}):
    r = req.get(url, headers=headers)
    if r.status_code != 200:
        raise Exception('API response: {}'.format(r.status_code))
    return r

def safe_load_csv(filename):
    filepath = os.path.join(DATA_DIR, filename)
    if os.path.exists(filepath) and os.path.getsize(filepath) > 0:
        try:
            df = pd.read_csv(filepath)
        except Exception as e:
            print(f"Could not read {filepath}: {e}")
            df = pd.DataFrame([])
    else:
        df = pd.DataFrame([])
    if 'uploaded_dt' in df.keys():
        df['uploaded_dt'] = pd.to_datetime(df['uploaded_dt'])
    return df

def clean_pre_df(df):
    return df.fillna('')

def load_pre_data():
    pre_grant_df = safe_load_csv('doge-grant.csv')
    pre_grant_df = clean_pre_df(pre_grant_df)
    return pre_grant_df

def scrape_doge_endpoint(api_root, endpoint_str, params):
    endpoint_json_list = []
    p_scrape = True
    page = 1
    while p_scrape:
        r = req.get(os.path.join(api_root, endpoint_str), params={**params, "page": page})
        _json_list = r.json()['result'][endpoint_str]
        p_scrape = page < r.json()['meta']['pages']
        endpoint_json_list.extend(_json_list)
        page += 1
    df = pd.DataFrame(endpoint_json_list)
    df = df.rename(columns={'description': 'description_doge'})
    return df

def scrape_doge():
    api_root = 'https://api.doge.gov/savings/'
    params = {
        "sort_by": "date",
        "sort_order": "desc",
        "per_page": 500
    }
    grant_df = scrape_doge_endpoint(api_root, 'grants', params)
    return grant_df

def safe_to_dt(dtstr):
    try:
        dt = pd.to_datetime(dtstr)
    except:
        dt = None
    return dt

def df_row_diff_2(old_df, stub_df):
    # If columns mismatch, skip and return stub_df (all as new)
    if set(stub_df.columns) - set(old_df.columns):
        print("WARNING: Column mismatch detected. All rows in stub_df will be considered new.")
        print("old_df columns:", list(old_df.columns))
        print("stub_df columns:", list(stub_df.columns))
        return stub_df.copy(), []
    new_df = stub_df.copy()
    drop_idx = []
    for idx, row in tqdm(new_df.iterrows()):
        match_series = (old_df[stub_df.columns] == row).all(axis=1)
        if match_series.any():
            drop_idx.append(np.arange(len(match_series))[match_series])
            new_df = new_df.drop(idx, axis=0)
    return new_df, drop_idx

def clean_stub_df(df):
    df.columns = [k.lower().replace(' ', '_') for k in df.keys()]
    if 'uploaded_on' in df.keys():
        df['uploaded_dt'] = [safe_to_dt(dts) for dts in df['uploaded_on'].values]
    if 'location' in df.keys():
        loc_part_list = [loc.split(', ') for loc in df['location'].values]
        for idx, loc_part_tup in enumerate(loc_part_list):
            city_pred = len(loc_part_tup[1]) == 2
            df.loc[idx, 'city'] = loc_part_tup[0]
            df.loc[idx, 'state'] = loc_part_tup[1] if city_pred else ''
            if len(loc_part_tup) > 2:
                df.loc[idx, 'agency'] = loc_part_tup[2] if city_pred else loc_part_tup[1]
    if 'link' in df.keys():
        df.link = df.link.fillna('')
    if 'vendor' in df.keys():
        df.loc[df.vendor == 'N/A', 'vendor'] = ''
    return df

def save_doge_data(grant_df, stub_grant_df):
    grant_df.to_csv(os.path.join(DATA_DIR, 'doge-grant.csv'), index=False)
    stub_grant_df.to_csv(os.path.join(DATA_DIR, 'doge-grant-stub.csv'), index=False)

def update_doge_data():
    datetime_scrape = datetime.strftime(datetime.now(), '%Y-%m-%d-%H%M')
    print('loading current data...')
    pre_grant_df = load_pre_data()
    print('scraping new data...')
    stub_grant_df = scrape_doge()
    stub_grant_df = clean_stub_df(stub_grant_df)
    print('finding new and changed entries...')
    new_grant_df, grant_drop_idx = df_row_diff_2(pre_grant_df, stub_grant_df)
    print('extending grant table with USASpending data...')
    new_grant_df = extend_grant_data(new_grant_df, datetime_scrape)
    new_grant_df['dt_scrape'] = datetime_scrape
    grant_df = pd.concat([pre_grant_df, new_grant_df], ignore_index=True)
    return grant_df, stub_grant_df

def main():
    grant_df, stub_grant_df = update_doge_data()
    save_doge_data(grant_df, stub_grant_df)

if __name__ == '__main__':
    main()
