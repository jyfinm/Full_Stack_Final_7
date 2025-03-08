import pandas as pd
import pytest
from settings import config
import pull_CRSP_bond_returns
DATA_DIR = config("DATA_DIR")


def test_pull_CRSP_bond_returns_functionality():
    df = pull_CRSP_bond_returns.pull_bond_returns()
    # Test if the function returns a pandas DataFrame
    assert isinstance(df, pd.DataFrame)

    # Test if the DataFrame has the expected columns
    expected_columns = ['cusip', 'tmt', 'yield']
    assert all(col in df.columns for col in expected_columns)

    # Test if the function raises an error when given an invalid data directory
    with pytest.raises(FileNotFoundError):
        pull_CRSP_bond_returns.load_bondret(data_dir="invalid_directory")

