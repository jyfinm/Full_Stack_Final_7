"""
Functions to process and merge bond data in order to construct portfolio approximations 
following Nozawa (2017) as used in He, Manela, and Kelly (2017).

This module performs the following steps:
  1. Processes the open treasury data (from pull_bondret_treasury.load_bondret_treasury_file):
       - Converts 'DATE' to datetime,
       - Scales 'tr_return' and 'tr_ytm_match' by 1/100,
       - Creates a unique identifier 'cusip_date' (CUSIP + '_' + formatted DATE),
       - Sorts the data by 'DATE'.
       
  2. Processes the CRSP bond returns data (from pull_CRSP_bond_returns.load_bondret):
       - Converts 'date' to datetime,
       - Casts 'year' to int,
       - Scales 't_yld_pt' by 1/100,
       - Creates 'cusip_date' (cusip + '_' + formatted date),
       - Sorts by ['cusip', 'date'],
       - Computes a forward return column 'ret_eom_fwd' (using groupby shift(-1)).
       
  3. Merges the two datasets on 'cusip_date' via an outer join, filling missing 'date' and 'cusip'
     values from the counterpart columns, and then reorders columns using a custom helper from misc_tools.
     
  4. Processes the merged DataFrame:
       - Computes 'yield_spread' = (yield - tr_ytm_match),
       - Computes 'TTM_diff' = (tmt - tau),
       - Drops rows with missing yield_spread,
       - Adjusts negative 'amount_outstanding' by multiplying by -0.001,
       - Sorts by 'date' and resets the index,
       - Computes decile rankings for yield_spread within each date (using qcut), then adds 11 so deciles run from 11 to 20.
       
  5. calculate_decil_returns(merged):
       A vectorized function that computes value–weighted portfolio returns for each decile.
       It uses the pre–computed forward return 'ret_eom_fwd' and the weights based on 'amount_outstanding'.
       It returns a DataFrame with index as 'date' and columns corresponding to decile numbers (11 to 20)
       representing the forward returns.
       
  6. process_all_data(open_df, crsp_df):
       Processes the raw open and CRSP data and returns a dictionary containing:
            'open_df'  : processed open treasury data,
            'crsp_df'  : processed CRSP bond data,
            'merged'   : the merged and fully processed DataFrame.
            
When running this module directly, it computes the decile returns and saves the resulting 
DataFrame (with forward returns) as a parquet file.
"""

import pandas as pd
import numpy as np
from pathlib import Path
from settings import config
from misc_tools import *  # For move_columns_to_front and other custom helpers

# Import pull functions
import pull_bondret_treasury
import pull_CRSP_bond_returns

# Configuration
DATA_DIR = Path(config("DATA_DIR"))
OUTPUT_DIR = Path(config("OUTPUT_DIR"))
START_DATE = config("START_DATE")
END_DATE = config("END_DATE")

#############################
# 1. Process Open Treasury Data
#############################

def process_open_source_bond_data(open_df):
    """
    Process the open treasury data.
    
    Expected columns: 'DATE', 'CUSIP', 'tr_return', 'tr_ytm_match'
    - Converts 'DATE' to datetime.
    - Scales 'tr_return' and 'tr_ytm_match' by 1/100.
    - Creates a unique identifier 'cusip_date' (CUSIP + '_' + formatted DATE).
    - Sorts by 'DATE'.
    """
    open_df["DATE"] = pd.to_datetime(open_df["DATE"], format="%Y%m%d")
    open_df['tr_return'] = open_df['tr_return'] / 100
    open_df['tr_ytm_match'] = open_df['tr_ytm_match'] / 100
    open_df['cusip_date'] = open_df['CUSIP'] + '_' + open_df['DATE'].dt.strftime('%Y%m%d')
    open_df = open_df.sort_values(by="DATE")
    return open_df

#############################
# 2. Process CRSP Bond Data
#############################

def process_crsp_bond_data(crsp_df):
    """
    Process the CRSP bond returns data.
    
    Expected columns: 'date', 'cusip', 'year', 't_yld_pt', 'ret_eom'
    - Converts 'date' to datetime.
    - Casts 'year' to int.
    - Scales 't_yld_pt' by 1/100.
    - Creates 'cusip_date' (cusip + '_' + formatted date).
    - Sorts by ['cusip', 'date'].
    - Computes a forward return column 'ret_eom_fwd' (shift(-1) for each cusip).
    """
    crsp_df['date'] = pd.to_datetime(crsp_df['date'], format="%Y%m%d")
    crsp_df['year'] = crsp_df['year'].astype(int)
    crsp_df['t_yld_pt'] = crsp_df['t_yld_pt'] / 100
    crsp_df['cusip_date'] = crsp_df['cusip'] + '_' + crsp_df['date'].dt.strftime('%Y%m%d')
    crsp_df = crsp_df.sort_values(by=["cusip", "date"])
    crsp_df["ret_eom_fwd"] = crsp_df.groupby("cusip")["ret_eom"].shift(-1)
    return crsp_df

#############################
# 3. Merge the Two Datasets
#############################

def merge_bond_data(proc_open, proc_crsp):
    """
    Merge processed open treasury data and CRSP bond data on 'cusip_date'
    using an outer join. Fills missing 'date' and 'cusip' using values from the counterpart.
    Uses move_columns_to_front to bring 'date', 'cusip', and 'cusip_date' to the front.
    """
    merged = pd.merge(proc_crsp, proc_open, on="cusip_date", how="outer", suffixes=("", "_open"))
    merged["date_combined"] = merged["date"].fillna(merged["DATE"])
    merged["cusip_combined"] = merged["cusip"].fillna(merged["CUSIP"])
    merged.drop(columns=["date", "DATE", "cusip", "CUSIP"], inplace=True)
    merged.rename(columns={"date_combined": "date", "cusip_combined": "cusip"}, inplace=True)
    move_columns_to_front(merged, cols=['date', 'cusip', 'cusip_date'])
    return merged

#############################
# 4. Process the Merged DataFrame
#############################

def process_merged_bond_data(merged):
    """
    Process the merged bond data:
      - Computes 'yield_spread' = (yield - tr_ytm_match)
      - Computes 'TTM_diff' = (tmt - tau)
      - Drops rows with missing 'yield_spread'
      - Adjusts negative 'amount_outstanding' by multiplying by -0.001 (fixes one incorrect data entry)
      - Sorts by 'date' and resets the index
      - Computes decile rankings for 'yield_spread' within each date using qcut,
        then adds 11 so deciles run from 11 to 20.
    """
    merged['yield_spread'] = merged['yield'] - merged['tr_ytm_match']
    merged['TTM_diff'] = merged['tmt'] - merged['tau']
    merged = merged.dropna(subset=["yield_spread"])
    mask = merged['amount_outstanding'] < 0
    merged.loc[mask, 'amount_outstanding'] = merged.loc[mask, 'amount_outstanding'] * -0.001
    merged = merged.sort_values(by="date").reset_index(drop=True)
    merged["decile"] = merged.groupby("date")["yield_spread"].transform(lambda x: pd.qcut(x, 10, labels=False))
    merged['decile'] = merged['decile'] + 11
    return merged

#############################
# 5. Process All Data
#############################

def process_all_data(open_df, crsp_df):
    """
    Process the raw open treasury and CRSP bond data and return a dictionary with:
      - proc_open: Processed open treasury data.
      - proc_crsp: Processed CRSP bond data.
      - merged: The merged and fully processed DataFrame.
    """
    proc_open = process_open_source_bond_data(open_df)
    proc_crsp = process_crsp_bond_data(crsp_df)
    merged = merge_bond_data(proc_open, proc_crsp)
    merged = process_merged_bond_data(merged)
    return proc_open, proc_crsp, merged

#############################
# 6. Calculate Decile Returns (Vectorized)
#############################

def calculate_decile_returns(merged):
    """
    Vectorized calculation of decile portfolio returns.
    
    Assumes that the 'merged' DataFrame contains:
      - 'date': rebalancing date,
      - 'decile': decile assignment (values from 11 to 20),
      - 'amount_outstanding': used as proxy for market cap,
      - 'ret_eom_fwd': the forward return (return realized from t to t+1).
      
    Computes the value–weighted portfolio return for each decile (using amount_outstanding as weights).
    
    Returns
    -------
    portfolio_returns_fwd : pd.DataFrame
        DataFrame with index as 'date' and columns corresponding to decile values (11, 12, …, 20) 
        representing the forward returns.
    portfolio_returns_norm : pd.DataFrame
        Same as portfolio_returns_fwd, but with the date shifted forward by one period.
    """
    df = merged.sort_values(by="date").copy()
    
    # Compute weights within each (date, decile) group.
    df["weight"] = df.groupby(["date", "decile"])["amount_outstanding"].transform(lambda x: x / x.sum())
    # Compute weighted forward return.
    df["weighted_ret"] = df["weight"] * df["ret_eom_fwd"]
    
    # Group by date and decile to sum the weighted returns.
    grouped = df.groupby(["date", "decile"])["weighted_ret"].sum().reset_index()
    portfolio_returns_fwd = grouped.pivot(index="date", columns="decile", values="weighted_ret")
    portfolio_returns_fwd = portfolio_returns_fwd.sort_index(axis=1).reset_index()
    
    # Drop the last row once (assume last row is incomplete or problematic)
    portfolio_returns_fwd = portfolio_returns_fwd.iloc[:-1]
    
    # Create a normalized version by shifting the date forward by one month.
    portfolio_returns_norm = portfolio_returns_fwd.copy()
    portfolio_returns_norm["date"] = portfolio_returns_norm["date"] + pd.DateOffset(months=1)
    
    return portfolio_returns_fwd, portfolio_returns_norm

def load_nozawa(output_dir=OUTPUT_DIR):
    path = Path(output_dir) / "nozawa_decile_returns.parquet"
    nozawa = pd.read_parquet(path)
    return nozawa

#############################
# Main block: Save Decile Returns Parquet
#############################

if __name__ == "__main__":
    # Load raw data using your pull functions.
    open_df = pull_bondret_treasury.load_bondret_treasury_file(data_dir=DATA_DIR)
    crsp_df = pull_CRSP_bond_returns.load_bondret(data_dir=DATA_DIR)
    
    # Process all data: this returns proc_open, proc_crsp, and the merged DataFrame.
    open_df, crsp_df, merged = process_all_data(open_df, crsp_df)
    
    # Calculate decile returns using the vectorized approach.
    portfolio_returns_fwd, decile_returns_df = calculate_decile_returns(merged)
    
    # Save the decile returns DataFrame (with forward returns) to a parquet file.
    decile_returns_df.to_parquet(OUTPUT_DIR / "nozawa_decile_returns.parquet")