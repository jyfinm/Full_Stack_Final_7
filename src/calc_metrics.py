"""
calc_metrics.py

This module provides functions to calculate analysis metrics comparing decile portfolio returns
with benchmark US corporate bond returns, as well as to split the decile returns DataFrame into two 
separate DataFrames based on a cutoff date. The cutoff date is defined as the last date present in the 
US corporate bonds DataFrame.

Functions:
    - calculate_decile_analysis(decile_returns_df, us_corp_df)
         Calculates correlation, R², regression slope/intercept, MAE, RMSE, and tracking error 
         for decile returns (for deciles 11-20).

    - split_decile_returns(decile_returns_df, us_corp_df)
         Splits the decile returns DataFrame into:
            * replication_df: rows with dates ≤ cutoff date (last date in us_corp_df),
            * updated_reproduction_df: rows with dates > cutoff date.
         
When run as a script, the module loads the necessary parquet files from DATA_DIR and OUTPUT_DIR,
splits the decile returns, and saves the resulting DataFrames as "nozawa_replication.parquet" and 
"nozawa_updated_reproduction.parquet". It does not print any additional output.
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
            - 'portfolio'
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
        ret_col = decile                 # Decile column (as an integer)
        corp_col = "US_bonds_" + str(decile)
        
        if ret_col in common_df.columns and corp_col in common_df.columns:
            sub_df = common_df[[ret_col, corp_col]].dropna()
            if len(sub_df) > 0:
                corr = sub_df[ret_col].corr(sub_df[corp_col])
                r2 = corr ** 2
                x = sub_df[ret_col].values
                y = sub_df[corp_col].values
                slope, intercept = np.polyfit(x, y, 1)
                y_pred = slope * x + intercept
                mae = np.mean(np.abs(y - y_pred))
                rmse = np.sqrt(np.mean((y - y_pred) ** 2))
                tracking_error = np.std(y - x)
            else:
                corr = r2 = slope = intercept = mae = rmse = tracking_error = None
        else:
            corr = r2 = slope = intercept = mae = rmse = tracking_error = None

        analysis_list.append({
            "portfolio": decile,
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

def split_decile_returns(decile_returns_df, us_corp_df):
    """
    Splits the decile returns DataFrame into two parts based on a cutoff date.
    
    The cutoff date is defined as the last date present in the US corporate bonds DataFrame.
    Rows in decile_returns_df with a date on or before the cutoff are placed in the replication sample,
    while rows with a date after the cutoff are placed in the updated reproduction sample.
    
    Parameters
    ----------
    decile_returns_df : pd.DataFrame
        DataFrame containing decile returns with a 'date' column.
    us_corp_df : pd.DataFrame
        DataFrame containing benchmark US corporate bond returns with a 'date' column.
    
    Returns
    -------
    replication_df : pd.DataFrame
         DataFrame with rows from decile_returns_df where date ≤ cutoff_date.
    updated_reproduction_df : pd.DataFrame
         DataFrame with rows from decile_returns_df where date > cutoff_date.
    """
    decile_returns_df = decile_returns_df.sort_values("date").reset_index(drop=True)
    cutoff_date = us_corp_df['date'].max()  # Last date in the benchmark dataset
    replication_df = decile_returns_df[decile_returns_df['date'] <= cutoff_date].copy()
    updated_reproduction_df = decile_returns_df[decile_returns_df['date'] > cutoff_date].copy()
    return replication_df, updated_reproduction_df

def plot_cumulative_returns(reproduction_df, save_path=None, show=True):
    """
    Plot the cumulative returns for each portfolio in the reproduction DataFrame.

    This function assumes that the input DataFrame contains:
      - a 'date' column with datetime values,
      - one or more portfolio return columns representing periodic returns (in decimal form).
    
    It computes cumulative returns as:
        cumulative_return = (1 + r).cumprod() - 1
    for each portfolio, plots them, and optionally saves the plot to a file.

    Parameters
    ----------
    reproduction_df : pd.DataFrame
        DataFrame containing periodic returns for each portfolio. Must have a 'date' column and
        portfolio columns (e.g. columns 11, 12, ... 20).
    save_path : str or Path, optional
        If provided, the plot is saved to this file.
    show : bool, optional
        If True (default), the plot is displayed immediately.

    Returns
    -------
    fig : matplotlib.figure.Figure
        The figure object for the plot.
    ax : matplotlib.axes.Axes
        The axes object for the plot.
    """
    import matplotlib.pyplot as plt

    # Sort by date and set 'date' as index.
    df = reproduction_df.sort_values("date").copy()
    df.set_index("date", inplace=True)
    
    # Compute cumulative returns for each portfolio column.
    cumulative_returns = (1 + df).cumprod() - 1
    
    # Plot cumulative returns.
    fig, ax = plt.subplots(figsize=(10, 6))
    for col in cumulative_returns.columns:
        ax.plot(cumulative_returns.index, cumulative_returns[col], label=f"Portfolio {col}")
    
    ax.set_xlabel("Date")
    ax.set_ylabel("Cumulative Return")
    ax.set_title("Cumulative Return of Each Portfolio")
    ax.legend(title="Decile")
    ax.grid(True)
    
    if save_path:
        plt.savefig(save_path)
    if show:
        plt.show()
    
    return fig, ax

if __name__ == "__main__":
    # Define file paths for input and output.
    decile_returns_path = OUTPUT_DIR / "nozawa_decile_returns.parquet"
    us_corp_bonds_path = DATA_DIR / "us_corp_bonds.parquet"
    analysis_output_path = OUTPUT_DIR / "analysis.parquet"
    replication_output_path = OUTPUT_DIR / "nozawa_replication.parquet"
    updated_reproduction_output_path = OUTPUT_DIR / "nozawa_updated_reproduction.parquet"
    
    # Load input DataFrames from parquet files.
    decile_returns_df = pd.read_parquet(decile_returns_path)
    us_corp_df = pd.read_parquet(us_corp_bonds_path)
    
    # Split decile_returns_df based on the cutoff date (last date in us_corp_df).
    replication_df, updated_reproduction_df = split_decile_returns(decile_returns_df, us_corp_df)
    
    # Save the split DataFrames.
    replication_df.to_parquet(replication_output_path)
    updated_reproduction_df.to_parquet(updated_reproduction_output_path)
    
    # Calculate the analysis metrics.
    analysis_df = calculate_decile_analysis(decile_returns_df, us_corp_df)
    analysis_df.to_parquet(analysis_output_path)