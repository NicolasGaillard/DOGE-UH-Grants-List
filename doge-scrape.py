import os
from datetime import datetime
import pandas as pd
import requests as req
from tqdm import tqdm
import validators

DATA_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), "data")
os.makedirs(DATA_DIR, exist_ok=True)

def safe_load_csv(filename):
    filepath = os.path.join(DATA_DIR, filename)
    if os.path.exists(filepath):
        df = pd.read_csv(filepath)
        return df
    else:
        return pd.DataFrame()

def save_csv(df, filename):
    df.to_csv(os.path.join(DATA_DIR, filename), index=False)

def scrape_doge():
    # ... your usual scraping logic ...
    # Return the stub DataFrame (raw grants from DOGE API)
    api_root = 'https://api.doge.gov/savings/'
    params = {
        "sort_by": "date",
        "sort_order": "desc",
        "per_page": 500
    }
    r = req.get(os.path.join(api_root, 'grants'), params=params)
    stub_grant_df = pd.DataFrame(r.json()['result']['grants'])
    stub_grant_df = stub_grant_df.rename(columns={'description': 'description_doge'})
    return stub_grant_df

def get_new_grants(stub, enriched):
    """Return only grants in stub that are NOT already in enriched."""
    if enriched.empty:
        return stub
    # Use a unique identifier column! Let's assume 'link' is unique for each grant
    if 'link' not in stub.columns or 'link' not in enriched.columns:
        print("ERROR: Unique 'link' column missing.")
        return pd.DataFrame()
    return stub[~stub['link'].isin(enriched['link'])]

def extend_grant_data(grant_df):
    """Enrich new grant rows with USASpending data (API calls)."""
    api_root = 'https://api.usaspending.gov/api/v2/awards/'
    usas_df = pd.DataFrame()
    for link in tqdm(grant_df['link'], desc="Extending new grants"):
        if validators.url(link):
            try:
                grant_id = os.path.basename(link)
                usas_req_url = os.path.join(api_root, grant_id)
                r = req.get(usas_req_url)
                grant_row_df = pd.json_normalize(r.json(), sep='_')
                grant_row_df['link'] = link  # So we can merge back!
                if not grant_row_df.empty and not grant_row_df.isna().all(axis=None):
                    usas_df = pd.concat([usas_df, grant_row_df], ignore_index=True)
            except Exception as e:
                print(f"Error fetching for {link}: {e}")
    if usas_df.empty:
        return grant_df
    # Merge stub and usas info on link
    merged = pd.merge(grant_df, usas_df, on="link", how="left", suffixes=('', '_usas'))
    return merged

def main():
    print("Loading stub and enriched data...")
    stub_grant_df = safe_load_csv("doge-grant-stub.csv")
    enriched_grant_df = safe_load_csv("doge-grant.csv")

    print(f"Loaded {len(stub_grant_df)} stub grants, {len(enriched_grant_df)} enriched grants.")

    print("Identifying new grants to enrich...")
    new_grants = get_new_grants(stub_grant_df, enriched_grant_df)
    print(f"Found {len(new_grants)} new grants to process.")

    if not new_grants.empty:
        extended_new_grants = extend_grant_data(new_grants)
        updated = pd.concat([enriched_grant_df, extended_new_grants], ignore_index=True)
        print(f"Saving updated grants table with {len(updated)} rows...")
        save_csv(updated, "doge-grant.csv")
    else:
        print("No new grants found, nothing to update.")

    # Optionally, update your stub file as well
    save_csv(stub_grant_df, "doge-grant-stub.csv")

if __name__ == "__main__":
    main()
