"""
This script loads the updated reproduction decile returns DataFrame from the designated DATA_DIR
and uses the function `plot_cumulative_returns` from calc_metrics to plot the cumulative returns 
for each decile portfolio. It also loads the merged bond data and uses `plot_avg_yield_tr_ytm` to 
plot the average 'yield' and average 'tr_ytm_match' over time. The resulting plots are saved as PNG 
files in OUTPUT_DIR.

Usage:
    Run this script directly (e.g., via `python plot_cumulative_returns_script.py`)
    to generate and save the cumulative returns and average yield/tr_ytm_match charts.
"""

import calc_metrics
import calc_nozawa_portfolio

import pandas as pd
from settings import config
from pathlib import Path
import numpy as np
from matplotlib import pyplot as plt
import seaborn as sns

DATA_DIR = Path(config("DATA_DIR"))
OUTPUT_DIR = Path(config("OUTPUT_DIR"))

sns.set()

def main():
    
    # Load the reproduction DataFrame (updated reproduction sample)
    reproduction_df = calc_metrics.load_reproduction(OUTPUT_DIR)
    
    # Plot and save the cumulative returns chart.
    fig, ax = calc_metrics.plot_cumulative_returns(
        reproduction_df, 
        save_path=OUTPUT_DIR / "cumulative_returns.png",
        show=False
    )
    
    # Load the merged bond data (assumed to be stored as "merged_bond_data.parquet")
    merged = calc_nozawa_portfolio.load_merged_data(DATA_DIR)
    
    # Plot and save the average yield and average tr_ytm_match chart.
    fig2, ax2 = calc_nozawa_portfolio.plot_avg_yield_tr_ytm(
        merged,
        save_path=OUTPUT_DIR / "avg_yield_tr_ytm.png",
        show=False
    )

if __name__ == "__main__":
    main()