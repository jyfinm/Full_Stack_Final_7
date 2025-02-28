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

def _demo():
    """
    Demo function: loads the CSV file and prints the first few rows.
    """
    df = load_he_kelly_manela_factors()
    print(df.head())

if __name__ == "__main__":
    pull_he_kelly_manela_factors()