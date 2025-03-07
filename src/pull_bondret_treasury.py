"""
Functions to pull and save treasury returns data.

Data source:
    https://openbondassetpricing.com/wp-content/uploads/2024/06/bondret_treasury.csv

This module provides functions to download the treasury returns CSV file from the
given URL and save it to a local data directory. You can later load the saved file
for further analysis.

"""

from pathlib import Path
import pandas as pd
from settings import config

DATA_DIR = Path(config("DATA_DIR"))
BOND_TREASURY_URL = "https://openbondassetpricing.com/wp-content/uploads/2024/06/bondret_treasury.csv"


def pull_bondret_treasury_file(url=BOND_TREASURY_URL):
    """
    Pulls the treasury returns CSV file from the specified URL.

    Parameters
    ----------
    url : str, optional
        URL of the CSV file containing treasury returns data,
        by default BOND_TREASURY_URL

    Returns
    -------
    pd.DataFrame
        DataFrame with the treasury returns data.
    """
    df = pd.read_csv(url)
    return df


def load_bondret_treasury_file(data_dir=DATA_DIR):
    """
    Loads the locally saved treasury returns file.

    Parameters
    ----------
    data_dir : Path, optional
        The directory where the treasury returns file is saved,
        by default DATA_DIR

    Returns
    -------
    pd.DataFrame
        DataFrame with the treasury returns data.
    """
    path = Path(data_dir) / "bondret_treasury.csv"
    df = pd.read_csv(path)
    return df


def _demo():
    df = load_bondret_treasury_file(data_dir=DATA_DIR)
    print(df.head())


if __name__ == "__main__":
    # Pull the treasury returns data from the URL
    df = pull_bondret_treasury_file(url=BOND_TREASURY_URL)
    # Define the local file path
    save_path = DATA_DIR / "bondret_treasury.csv"
    # Save the DataFrame to a CSV file (without the index)
    df.to_csv(save_path, index=False)