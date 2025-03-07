"""
Functions to pull and process the He, Kelly, and Manela Factors file.

This script downloads the zip file from:
    https://asafmanela.github.io/papers/hkm/intermediarycapitalrisk/He_Kelly_Manela_Factors.zip
It then unzips the file in memory and saves only the CSV file:
    He_Kelly_Manela_Factors_And_Test_Assets_monthly.csv
to the designated DATA_DIR (as defined in settings.py).


This module defines a function to process the factors DataFrame. It:
  1. Renames the 'yyyymm' column to 'date' and converts it to datetime using format "%Y%m",
     adjusting it to the month end.
  2. Identifies columns starting with "US_bonds_".
  3. Splits these columns into two groups:
       - group_01_10: columns with numeric suffix between 1 and 10,
       - group_11_20: columns with numeric suffix between 11 and 20.
  4. Creates two DataFrames: 
       - us_tr_df: containing 'date' and columns from group_01_10,
       - us_corp_df: containing 'date' and columns from group_11_20.
  5. Drops rows where all entries in the respective group columns are NaN (ignoring the 'date' column).

When this module is run as a script, it will load the raw factors data, process it, and save the US corporate bonds
DataFrame (us_corp_df) as a parquet file in DATA_DIR.

This file contains the factor returns and test asset returns used in He, Kelly, and Manela (2017).
"""

import requests
import zipfile
import io
from pathlib import Path
import pandas as pd
import numpy as np
from pandas.tseries.offsets import MonthEnd
from settings import config

DATA_DIR = Path(config("DATA_DIR"))
URL = "https://asafmanela.github.io/papers/hkm/intermediarycapitalrisk/He_Kelly_Manela_Factors.zip"
TARGET_FILE = "He_Kelly_Manela_Factors_And_Test_Assets_monthly.csv"
TARGET_PATH = DATA_DIR / TARGET_FILE

def pull_he_kelly_manela_factors():
    """
    Downloads the He, Kelly, and Manela factors zip file,
    unzips it in memory, and saves only the CSV file
    'He_Kelly_Manela_Factors_And_Test_Assets_monthly.csv' to DATA_DIR.
    """
    # Download the zip file
    response = requests.get(URL)
    response.raise_for_status()  # Raise an error if the download fails

    # Open the zip file in memory
    with zipfile.ZipFile(io.BytesIO(response.content)) as z:
        # Check if our target file exists in the zip archive
        if TARGET_FILE not in z.namelist():
            raise FileNotFoundError(f"{TARGET_FILE} not found in the downloaded zip file.")
        # Read the target CSV file from the zip archive
        with z.open(TARGET_FILE) as f:
            data = f.read()

    # Ensure the DATA_DIR exists
    DATA_DIR.mkdir(parents=True, exist_ok=True)
    # Save the CSV file to the designated path
    with open(TARGET_PATH, "wb") as outfile:
        outfile.write(data)
    print(f"Saved {TARGET_FILE} to {TARGET_PATH}")

def load_he_kelly_manela_factors(data_dir=DATA_DIR):
    """
    Loads the He, Kelly, and Manela Factors CSV file from DATA_DIR.

    Returns
    -------
    pd.DataFrame
        DataFrame containing the test assets and factor returns.
    """
    path = data_dir / TARGET_FILE
    df = pd.read_csv(path)
    return df

def process_he_kelly_manela_factors(test_df):
    """
    Process the He, Kelly, and Manela factors DataFrame.
    
    This function assumes that test_df has:
      - a column 'yyyymm' representing year and month,
      - several columns starting with "US_bonds_".
      
    It performs the following steps:
      1. Renames 'yyyymm' to 'date' and converts it to datetime using format "%Y%m", 
         then adjusts to the month end.
      2. Identifies all columns starting with "US_bonds_".
      3. Splits these columns into two groups:
           group_01_10: columns where the numeric suffix is between 1 and 10,
           group_11_20: columns where the numeric suffix is between 11 and 20.
      4. Creates two DataFrames:
           us_tr_df: containing 'date' and columns from group_01_10,
           us_corp_df: containing 'date' and columns from group_11_20.
      5. Drops rows where all entries in the respective group columns are NaN (ignoring 'date').
    
    Parameters
    ----------
    test_df : pd.DataFrame
        DataFrame loaded from pull_he_kelly_manela_factors.load_he_kelly_manela_factors.
    
    Returns
    -------
    us_tr_df : pd.DataFrame
         DataFrame containing the treasury-related US bonds columns.
    us_corp_df : pd.DataFrame
         DataFrame containing the corporate-related US bonds columns.
    """
    # Rename 'yyyymm' to 'date' and convert to datetime (month-end)
    test_df = test_df.rename(columns={"yyyymm": "date"})
    test_df["date"] = pd.to_datetime(test_df["date"], format="%Y%m") + MonthEnd(0)
    
    # Identify all columns starting with "US_bonds_"
    us_bonds_cols = [col for col in test_df.columns if col.startswith("US_bonds_")]
    
    # Split into two groups: 1-10 and 11-20 based on the numeric suffix
    group_01_10 = [col for col in us_bonds_cols if 1 <= int(col.split("_")[-1]) <= 10]
    group_11_20 = [col for col in us_bonds_cols if 11 <= int(col.split("_")[-1]) <= 20]
    
    # Create DataFrames including the date column
    us_tr_df = test_df[['date'] + group_01_10].copy()
    us_corp_df = test_df[['date'] + group_11_20].copy()
    
    # Drop rows where all entries in the group columns are NaN (excluding 'date')
    us_tr_df = us_tr_df.dropna(axis=0, how='all', subset=group_01_10)
    us_corp_df = us_corp_df.dropna(axis=0, how='all', subset=group_11_20)
    
    return us_tr_df, us_corp_df



def _demo():
    """
    Demo function: loads the CSV file and prints the first few rows.
    """
    df = load_he_kelly_manela_factors()
    print(df.head())

if __name__ == "__main__":
    pull_he_kelly_manela_factors()

    df = load_he_kelly_manela_factors()
    us_tr_df, us_corp_df = process_he_kelly_manela_factors(df)
    output_path = DATA_DIR / "us_corp_bonds.parquet"
    us_corp_df.to_parquet(output_path)