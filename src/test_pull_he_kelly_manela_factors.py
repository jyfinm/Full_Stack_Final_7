import pandas as pd
import pytest
from settings import config
import pull_he_kelly_manela_factors
import os
from pathlib import Path


DATA_DIR = Path(config("DATA_DIR"))
TARGET_FILE = "He_Kelly_Manela_Factors_And_Test_Assets_monthly.csv"
TARGET_PATH = DATA_DIR / TARGET_FILE

def test_pull_he_kelly_manela_factors_functionality():
    pull_he_kelly_manela_factors.pull_he_kelly_manela_factors()
    
    assert os.path.exists(TARGET_PATH), f"Expected {TARGET_PATH} to exist, but it doesn't."


