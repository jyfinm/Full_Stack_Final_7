# import pandas as pd
# import pytest
# from settings import config
# import pull_fred
# DATA_DIR = config("DATA_DIR")


# def test_pull_fred_functionality():
#     df = pull_fred.pull_fred()
#     # Test if the function returns a pandas DataFrame
#     assert isinstance(df, pd.DataFrame)

#     # Test if the DataFrame has the expected columns
#     expected_columns = ['CPIAUCNS', 'GDP', 'GDPC1']
#     assert all(col in df.columns for col in expected_columns)

#     # Test if the function raises an error when given an invalid data directory
#     with pytest.raises(FileNotFoundError):
#         pull_fred.load_fred(data_dir="invalid_directory")

# def test_pull_fred_data_validity():
#     df = pull_fred.pull_fred()
    
#     # Test if the default date range has the expected start date and end date
#     assert df.index.min() == pd.Timestamp('1913-01-01')
#     assert df.index.max() >= pd.Timestamp('2023-09-01')

#     # Test if the average annualized growth rate is close to 3.08%
#     ave_annualized_growth = 4 * 100 * df.loc['1913-01-01': '2023-09-01', 'GDPC1'].dropna().pct_change().mean()
#     assert abs(ave_annualized_growth - 3.08) < 0.1
