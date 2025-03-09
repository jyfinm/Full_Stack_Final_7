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

import calc_nozawa_portfolio
import pull_he_kelly_manela_factors

import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
import re
from pathlib import Path
from settings import config

DATA_DIR = Path(config("DATA_DIR"))
OUTPUT_DIR = Path(config("OUTPUT_DIR"))

# Helper function to compute summary statistics for a given series.
def calc_summary(series):
    mean_val = series.mean()
    std_val = series.std()
    # Assuming returns are in decimal format; cumulative return = product(1+return) - 1.
    cum_ret = (np.prod(1 + series) - 1) if len(series) > 0 else None
    return mean_val, std_val, cum_ret

# For each summary, also compute the start and end dates based on non-NA values.
def get_date_range(df, col):
    non_na = df[['date', col]].dropna()
    if len(non_na) > 0:
        start_date = non_na['date'].iloc[0]
        end_date = non_na['date'].iloc[-1]
    else:
        start_date = end_date = None
    return start_date, end_date

def calculate_decile_analysis(decile_returns_df, us_corp_df):
    """
    Calculate analysis metrics comparing decile portfolio returns with benchmark US corporate bond returns.
    
    Additionally, compute summary statistics (mean, standard deviation, cumulative return, start date, end date)
    for each column in the merged dataframe. For columns from decile_returns_df and us_corp_df, the portfolio
    column will simply be the decile number.
    
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
            
    benchmark_summary_df : pd.DataFrame
        A DataFrame containing summary statistics for each column from the us_corp_df (benchmark)
        merged into the common dataframe.
        
    replicate_summary_df : pd.DataFrame
        A DataFrame containing summary statistics for each column from the decile_returns_df (replication)
        merged into the common dataframe.
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
    common_df = pd.merge(decile_returns_df, us_corp_df, on="date", how="inner")
    
    # Compute decile analysis metrics for deciles 11 through 20.
    analysis_list = []
    for decile in range(11, 21):
        ret_col = decile                 # Replicate decile column (as an integer)
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
            "MAE": mae,
            "RMSE": rmse,
            "tracking_error": tracking_error
        })
    
    analysis_df = pd.DataFrame(analysis_list)
    
    # Identify replicate columns (from decile_returns_df) and benchmark columns (from us_corp_df)
    replicate_cols = [col for col in common_df.columns if col != "date" and isinstance(col, int)]
    benchmark_cols = [col for col in common_df.columns if isinstance(col, str) and col.startswith("US_bonds_")]
    
    # Calculate summary statistics for replicate columns.
    replicate_summary_list = []
    for col in replicate_cols:
        series = common_df[col].dropna()
        mean_val, std_val, cum_ret = calc_summary(series)
        start_date, end_date = get_date_range(common_df, col)
        replicate_summary_list.append({
            "portfolio": col,  # Use decile number
            "mean": mean_val,
            "std": std_val,
            "cumulative_return": cum_ret,
            "start_date": start_date,
            "end_date": end_date
        })
    replicate_summary_df = pd.DataFrame(replicate_summary_list)
    
    # Calculate summary statistics for benchmark columns.
    benchmark_summary_list = []
    for col in benchmark_cols:  # Fixed: iterate over benchmark_cols instead of replicate_cols.
        series = common_df[col].dropna()
        mean_val, std_val, cum_ret = calc_summary(series)
        start_date, end_date = get_date_range(common_df, col)
        # Extract decile number from column name, e.g., "US_bonds_11" -> "11".
        decile_num = col.replace("US_bonds_", "")
        benchmark_summary_list.append({
            "portfolio": decile_num,
            "mean": mean_val,
            "std": std_val,
            "cumulative_return": cum_ret,
            "start_date": start_date,
            "end_date": end_date
        })
    benchmark_summary_df = pd.DataFrame(benchmark_summary_list)
    
    return analysis_df, benchmark_summary_df, replicate_summary_df

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

def load_analysis(output_dir=OUTPUT_DIR):
    path = Path(output_dir) / "analysis.parquet"
    analysis_df = pd.read_parquet(path)
    return analysis_df

def load_reproduction(output_dir=OUTPUT_DIR):
    path = Path(output_dir) / "nozawa_updated_reproduction.parquet"
    reproduction = pd.read_parquet(path)
    return reproduction

def load_replication(output_dir=OUTPUT_DIR):
    path = Path(output_dir) / "nozawa_replication.parquet"
    replication = pd.read_parquet(path)
    return replication

def load_benchmark_summary(output_dir=OUTPUT_DIR):
    path = Path(output_dir) / "benchmark_summary.parquet"
    benchmark_summary = pd.read_parquet(path)
    return benchmark_summary

def load_replicate_summary(output_dir=OUTPUT_DIR):
    path = Path(output_dir) / "replicate_summary.parquet"
    replicate_summary = pd.read_parquet(path)
    return replicate_summary

if __name__ == "__main__":
    # Define file paths for input and output.
    analysis_output_path = OUTPUT_DIR / "analysis.parquet"
    benchmark_summary_output_path = OUTPUT_DIR / "benchmark_summary.parquet"
    replicate_summary_output_path = OUTPUT_DIR / "replicate_summary.parquet"
    replication_output_path = OUTPUT_DIR / "nozawa_replication.parquet"
    updated_reproduction_output_path = OUTPUT_DIR / "nozawa_updated_reproduction.parquet"
    
    # Load input DataFrames from parquet files.
    decile_returns_df = calc_nozawa_portfolio.load_nozawa(OUTPUT_DIR)
    us_corp_df = pull_he_kelly_manela_factors.load_us_corp_bonds(DATA_DIR)
    
    # Split decile_returns_df based on the cutoff date (last date in us_corp_df).
    replication_df, updated_reproduction_df = split_decile_returns(decile_returns_df, us_corp_df)
    
    # Save the split DataFrames.
    replication_df.to_parquet(replication_output_path)
    updated_reproduction_df.to_parquet(updated_reproduction_output_path)
    
    # Calculate the analysis metrics.
    analysis_df, benchmark_summary_df, replicate_summary_df = calculate_decile_analysis(decile_returns_df, us_corp_df)
    analysis_df.to_parquet(analysis_output_path)
    benchmark_summary_df.to_parquet(benchmark_summary_output_path)
    replicate_summary_df.to_parquet(replicate_summary_output_path)