import os
from datetime import datetime
from time import sleep

import numpy as np
import pandas as pd
import requests as req
import validators
from tqdm import tqdm

# Set up absolute path to data directory
SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
DATA_DIR = os.path.join(SCRIPT_DIR, 'data')
os.makedirs(DATA_DIR, exist_ok=True)

N_REQ = 10
LIMIT_S = 3    # For ratelimit, if you extend/enrich with another API

def safe_load_csv(filename):
    filepath = os.path.join(DATA_DIR, filename)
    df = pd.read_csv(filepath) if os.path.exists(filepath) else pd.DataFrame([])
    if 'uploaded_dt' in df.keys():
        df['uploaded_dt'] = pd.to_datetime(df['uploaded_dt'])
    return df

def clean_pre_df(df):
    df = df.fillna('')
    return df

def save_grant_data(grant_df, stub_grant_df):
    grant_df.to_csv(os.path.join(DATA_DIR, 'doge-grant.csv'), index=False)
    stub_grant_df.to_csv(os.path.join(DATA_DIR, 'doge-grant-stub.csv'), index=False)

def scrape_grant_endpoint(api_root, params):
    all_grants = []
    page = 1
    per_page = params.get("per_page", 500)
    while True:
        page_params = params.copy()
        page_params["page"] = page
        r = req.get(api_root, params=page_params)
        if r.status_code != 200:
            print(f"Error fetching page {page}: {r.status_code}")
            break
        data = r.json().get('result', {}).get('grants', [])
        if not data:
            break
        all_grants.extend(data)
        print(f"Fetched page {page} with {len(data)} grants")
        meta = r.json().get('meta', {})
        if "pages" in meta and page >= meta["pages"]:
            break
        page += 1
    grant_df = pd.DataFrame(all_grants)
    grant_df = grant_df.rename(columns={'description': 'description_doge'})
    return grant_df

def get_new_grants(stub, enriched):
    if enriched.empty:
        return stub
    if 'link' not in stub.columns or 'link' not in enriched.columns:
        print("ERROR: Unique 'link' column missing.")
        return pd.DataFrame()
    return stub[~stub['link'].isin(enriched['link'])]

def extend_grant_data(grant_df):
    # Optional: extend with USASpending data if needed
    # (This is your original logic, can be left as is or simplified.)
    return grant_df  # If not using USASpending enrichment, just return

def main():
    print("Loading previous grant data...")
    enriched_grant_df = safe_load_csv("doge-grant.csv")
    enriched_grant_df = clean_pre_df(enriched_grant_df)

    print("Scraping latest DOGE grants (all pages)...")
    params = {
        "sort_by": "date",
        "sort_order": "desc",
        "per_page": 500
    }
    stub_grant_df = scrape_grant_endpoint('https://api.doge.gov/savings/grants', params)
    stub_grant_df = clean_pre_df(stub_grant_df)
    save_grant_data(enriched_grant_df, stub_grant_df)  # Save stub immediately

    print("Identifying new grants to process...")
    new_grants = get_new_grants(stub_grant_df, enriched_grant_df)
    print(f"Found {len(new_grants)} new grants to process.")

    if not new_grants.empty:
        extended_new_grants = extend_grant_data(new_grants)
        updated = pd.concat([enriched_grant_df, extended_new_grants], ignore_index=True)
        print(f"Saving updated grant table with {len(updated)} rows...")
        save_grant_data(updated, stub_grant_df)
    else:
        print("No new grants found, nothing to update.")

if __name__ == "__main__":
    main()
