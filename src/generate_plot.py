"""
This script loads the updated reproduction decile returns DataFrame from the designated DATA_DIR
and uses the function `plot_cumulative_returns` from calc_metrics to plot the cumulative returns 
for each decile portfolio. The resulting plot is saved as a PNG file in DATA_DIR.

Usage:
    Run this script directly (e.g., via `python plot_cumulative_returns_script.py`)
    to generate and save the cumulative returns chart.
"""

import calc_metrics
import pandas as pd
from settings import config
from pathlib import Path
import numpy as np
from matplotlib import pyplot as plt
import seaborn as sns

sns.set()

def main():
    DATA_DIR = Path(config("DATA_DIR"))
    
    # Load the reproduction DataFrame (updated reproduction sample)
    reproduction_df = pd.read_parquet(DATA_DIR / "nozawa_updated_reproduction.parquet")
    
    # Plot and save the cumulative returns chart.
    fig, ax = calc_metrics.plot_cumulative_returns(
        reproduction_df, 
        save_path=DATA_DIR / "cumulative_returns.png"
    )

if __name__ == "__main__":
    main()