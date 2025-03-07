"""
Functions to pull and process the He, Kelly, and Manela Factors file.

This script downloads the zip file from:
    https://asafmanela.github.io/papers/hkm/intermediarycapitalrisk/He_Kelly_Manela_Factors.zip
It then unzips the file in memory and saves only the CSV file:
    He_Kelly_Manela_Factors_And_Test_Assets_monthly.csv
to the designated DATA_DIR (as defined in settings.py).

This file contains the factor returns and test asset returns used in He, Kelly, and Manela (2017).
Thank you to Tobias Rodriguez del Pozo for his assistance in writing similar code.
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

def calculate_decile_analysis(decile_returns_df, us_corp_df):
    """
    Calculate analysis metrics comparing decile portfolio returns with benchmark US corporate bond returns.
    
    This function merges the decile returns DataFrame and the benchmark US corporate bonds DataFrame 
    on the 'date' column (using an inner join). It then computes, for each decile (11 to 20):
      - Pearson correlation between the replicated decile return and the benchmark return,
      - R² (square of the correlation),
      - Regression parameters (slope and intercept) from a linear regression of benchmark returns on the replication,
      - Mean Absolute Error (MAE) and Root Mean Squared Error (RMSE) from the regression,
      - Tracking error (standard deviation of the difference between benchmark and replicated returns).
    
    Parameters
    ----------
    decile_returns_df : pd.DataFrame
        DataFrame containing the replicated decile returns with a 'date' column and decile columns
        labeled as integers 11, 12, ..., 20.
    us_corp_df : pd.DataFrame
        DataFrame containing the benchmark US corporate bond returns with a 'date' column and columns
        named "US_bonds_11", "US_bonds_12", ..., "US_bonds_20".
        
    Returns
    -------
    analysis_df : pd.DataFrame
        A DataFrame with one row per decile and columns:
            - 'decile'
            - 'correlation'
            - 'r_squared'
            - 'slope'
            - 'intercept'
            - 'mae'
            - 'rmse'
            - 'tracking_error'
    """

    # Merge the two DataFrames on 'date'
    common_df = pd.merge(decile_returns_df, us_corp_df, on="date", how="inner", suffixes=('_ret', '_corp'))
    
    analysis_list = []
    for decile in range(11, 21):
        # In decile_returns_df, the column is simply the decile number (e.g., 11)
        ret_col = decile  
        # In us_corp_df, the benchmark column is named "US_bonds_" + decile
        corp_col = "US_bonds_" + str(decile)
        
        if ret_col in common_df.columns and corp_col in common_df.columns:
            sub_df = common_df[[ret_col, corp_col]].dropna()
            if len(sub_df) > 0:
                # Compute Pearson correlation and r^2.
                corr = sub_df[ret_col].corr(sub_df[corp_col])
                r2 = corr ** 2
                
                # Run a simple linear regression (using np.polyfit: benchmark ~ replication)
                x = sub_df[ret_col].values
                y = sub_df[corp_col].values
                slope, intercept = np.polyfit(x, y, 1)
                
                # Compute predicted values and residual metrics.
                y_pred = slope * x + intercept
                mae = np.mean(np.abs(y - y_pred))
                rmse = np.sqrt(np.mean((y - y_pred) ** 2))
                
                # Tracking error: standard deviation of the difference between benchmark and replication.
                tracking_error = np.std(y - x)
            else:
                corr = r2 = slope = intercept = mae = rmse = tracking_error = None
        else:
            corr = r2 = slope = intercept = mae = rmse = tracking_error = None

        analysis_list.append({
            "decile": decile,
            "correlation": corr,
            "r_squared": r2,
            "slope": slope,
            "intercept": intercept,
            "mae": mae,
            "rmse": rmse,
            "tracking_error": tracking_error
        })
    
    analysis_df = pd.DataFrame(analysis_list)
    return analysis_df

def _demo():
    """
    Demo function: loads the CSV file and prints the first few rows.
    """
    df = load_he_kelly_manela_factors()
    print(df.head())

if __name__ == "__main__":
    pull_he_kelly_manela_factors()