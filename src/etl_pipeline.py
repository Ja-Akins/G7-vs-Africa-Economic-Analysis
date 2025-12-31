# src/etl_pipeline.py

import os
import requests
import pandas as pd
import numpy as np
from sqlalchemy import create_engine
from concurrent.futures import ThreadPoolExecutor
from dotenv import load_dotenv

# 1. Load Environment Variables
load_dotenv()

# Configuration
DB_USER = os.getenv("DB_USER")
DB_PASS = os.getenv("DB_PASSWORD")
DB_HOST = os.getenv("DB_HOST")
DB_NAME = os.getenv("DB_NAME")

# Construct Connection String securely
DB_CONFIG = f"mysql+mysqlconnector://{DB_USER}:{DB_PASS}@{DB_HOST}/{DB_NAME}"

START_YEAR = 2000
END_YEAR = 2024

COUNTRY_GROUPS = {
    "G7": ["USA", "GBR", "DEU", "FRA", "ITA", "CAN", "JPN"],
    "AFRICA_TOP5": ["NGA", "ZAF", "EGY", "DZA", "MAR"]
}

INDICATORS = {
    "NY.GDP.MKTP.KD.ZG": "GDP Growth (%)",
    "FP.CPI.TOTL.ZG": "Inflation (%)",
    "BX.KLT.DINV.WD.GD.ZS": "FDI (% of GDP)",
    "FS.AST.PRVT.GD.ZS": "Private Credit (% of GDP)",
    "SL.UEM.TOTL.ZS": "Unemployment (%)",
    "GC.DOD.TOTL.GD.ZS": "Central Gov Debt (% of GDP)",
    "EG.ELC.ACCS.ZS": "Access to Electricity (%)",
    "NE.EXP.GNFS.ZS": "Exports (% of GDP)"
}

def fetch_data():
    """Fetches data concurrently from World Bank API."""
    tasks = []
    for group, countries in COUNTRY_GROUPS.items():
        for country in countries:
            tasks.append((country, group))

    all_data = []
    
    print(f"Starting extraction for {len(tasks) * len(INDICATORS)} endpoints...")
    
    with ThreadPoolExecutor(max_workers=5) as executor:
        futures = []
        for country_code, group in tasks:
            for ind_code, ind_name in INDICATORS.items():
                url = (
                    f"https://api.worldbank.org/v2/country/{country_code}/indicator/{ind_code}"
                    f"?date={START_YEAR}:{END_YEAR}&format=json&per_page=100"
                )
                futures.append((executor.submit(requests.get, url, timeout=10),
                                country_code, group, ind_code, ind_name))

        for future, country_code, group, ind_code, ind_name in futures:
            try:
                resp = future.result()
                data = resp.json()
                
                # Check if data exists in response
                if len(data) < 2:
                    continue

                for entry in data[1]:
                    if entry["value"] is not None:
                        all_data.append({
                            "country": entry["country"]["value"],
                            "country_code": entry["country"]["id"],
                            "country_group": group,
                            "indicator_code": ind_code,
                            "indicator_name": ind_name,
                            "year": int(entry["date"]),
                            "value": float(entry["value"])
                        })
            except Exception as e:
                print(f"Error fetching {ind_code} for {country_code}: {e}")
                
    return pd.DataFrame(all_data)

def detect_outliers_iqr(df):
    """Flags outliers using the Interquartile Range method."""
    print("Detecting statistical outliers...")
    df["is_outlier"] = False
    
    for indicator in df["indicator_code"].unique():
        mask = df["indicator_code"] == indicator
        values = df.loc[mask, "value"]
        
        Q1 = values.quantile(0.25)
        Q3 = values.quantile(0.75)
        IQR = Q3 - Q1
        
        lower = Q1 - 1.5 * IQR
        upper = Q3 + 1.5 * IQR
        
        outlier_mask = (df["value"] < lower) | (df["value"] > upper)
        df.loc[mask & outlier_mask, "is_outlier"] = True
        
    return df

def load_to_database(df):
    """Loads processed data into MySQL."""
    print("Loading data to MySQL...")
    engine = create_engine(DB_CONFIG)
    
    df.to_sql(
        "economic_indicators",
        con=engine,
        if_exists="replace",
        index=False,
        chunksize=500,
        method="multi"
    )
    print("Data successfully loaded.")

if __name__ == "__main__":
    # 1. Extract
    df_raw = fetch_data()
    print(f"Extracted {len(df_raw)} rows.")
    
    # 2. Transform
    df_clean = detect_outliers_iqr(df_raw)
    print(f"Flagged {df_clean['is_outlier'].sum()} outliers.")
    
    # 3. Load
    load_to_database(df_clean)
