import pandas as pd
import pytest
from settings import config
import pull_bondret_treasury
DATA_DIR = config("DATA_DIR")


def test_pull_bondret_treasury_functionality():
    df = pull_bondret_treasury.pull_bondret_treasury_file()
    # Test if the function returns a pandas DataFrame
    assert isinstance(df, pd.DataFrame)

    # Test if the DataFrame has the expected columns
    expected_columns = ['CUSIP', 'tr_return', 'tr_ytm_match', 'tau']
    assert all(col in df.columns for col in expected_columns)

    # Test if the function raises an error when given an invalid data directory
    with pytest.raises(FileNotFoundError):
        pull_bondret_treasury.load_bondret_treasury_file(data_dir="invalid_directory")

