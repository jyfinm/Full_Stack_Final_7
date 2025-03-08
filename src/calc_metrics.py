"""
calc_metrics.py

This module provides a function to calculate analysis metrics comparing decile portfolio returns
with benchmark US corporate bond returns.

The function `calculate_decile_analysis` merges the decile returns DataFrame and the benchmark US corporate
bond returns DataFrame on the 'date' column and computes, for each decile (11 to 20):
  - Pearson correlation between the replicated decile return and the benchmark return,
  - R² (the square of the correlation),
  - Regression parameters (slope and intercept) from a simple linear regression of benchmark returns on replication,
  - Mean Absolute Error (MAE) and Root Mean Squared Error (RMSE),
  - Tracking error (standard deviation of the difference between benchmark and replicated returns).

When run as a script, this module loads the necessary parquet files from DATA_DIR, computes the analysis metrics,
and saves the resulting DataFrame as "analysis.parquet".
"""

import numpy as np
import pandas as pd
from pathlib import Path
from settings import config

DATA_DIR = Path(config("DATA_DIR"))
OUTPUT_DIR = Path(config("OUTPUT_DIR"))

def calculate_decile_analysis(decile_returns_df, us_corp_df):
    """
    Calculate analysis metrics comparing decile portfolio returns with benchmark US corporate bond returns.
    
    Parameters
    ----------
    decile_returns_df : pd.DataFrame
        DataFrame containing the replicated decile returns with a 'date' column and decile columns.
        This function expects decile columns (other than 'date') that can be converted to integers.
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
    # Convert decile return column names (excluding 'date') to integers if possible.
    cols = decile_returns_df.columns.drop("date")
    mapping = {}
    for col in cols:
        if isinstance(col, str) and col.isdigit():
            mapping[col] = int(col)
        else:
            mapping[col] = col
    decile_returns_df = decile_returns_df.rename(columns=mapping)
    
    # Merge the two DataFrames on 'date'
    common_df = pd.merge(decile_returns_df, us_corp_df, on="date", how="inner", suffixes=('_ret', '_corp'))
    
    analysis_list = []
    for decile in range(11, 21):
        # The decile returns DataFrame now uses integer column names (e.g., 11, 12, ...)
        ret_col = decile  
        # The us_corp_df has benchmark columns like "US_bonds_11", "US_bonds_12", etc.
        corp_col = "US_bonds_" + str(decile)
        
        if ret_col in common_df.columns and corp_col in common_df.columns:
            sub_df = common_df[[ret_col, corp_col]].dropna()
            if len(sub_df) > 0:
                # Compute Pearson correlation and R².
                corr = sub_df[ret_col].corr(sub_df[corp_col])
                r2 = corr ** 2
                
                # Run a simple linear regression (benchmark ~ replication)
                x = sub_df[ret_col].values
                y = sub_df[corp_col].values
                slope, intercept = np.polyfit(x, y, 1)
                
                # Compute predicted values and error metrics.
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

if __name__ == "__main__":
    # Define file paths for the input parquet files.
    decile_returns_path = OUTPUT_DIR / "nozawa_decile_returns.parquet"
    us_corp_bonds_path = DATA_DIR / "us_corp_bonds.parquet"
    output_path = OUTPUT_DIR / "analysis.parquet"
    
    # Load input DataFrames from parquet files.
    decile_returns_df = pd.read_parquet(decile_returns_path)
    us_corp_df = pd.read_parquet(us_corp_bonds_path)
    
    # Calculate the analysis metrics.
    analysis_df = calculate_decile_analysis(decile_returns_df, us_corp_df)
    
    # Save the analysis DataFrame to a parquet file.
    analysis_df.to_parquet(output_path)