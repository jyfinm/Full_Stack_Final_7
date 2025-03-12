import os
import pandas as pd
import numpy as np
from datetime import datetime

from pathlib import Path
from settings import config
import calc_nozawa_portfolio
import calc_metrics

DATA_DIR = Path(config("DATA_DIR"))
OUTPUT_DIR = Path(config("OUTPUT_DIR"))

# Test for our calculate_decile_analysis correlations to He-Kelly-Manela's calculated returns
# Test for calculate_decile_analysis correlations using analysis.parquet data
def test_calculate_decile_analysis_correlations():
    analysis_df = calc_metrics.load_analysis(OUTPUT_DIR)
    min_corr = 0.8
    for idx, row in analysis_df.iterrows():
        corr = row["correlation"]
        assert corr is not None, f"Correlation for portfolio {row['portfolio']} is None."
        assert corr >= min_corr, f"Correlation for portfolio {row['portfolio']} is below {min_corr}: {corr}"

# Test for calculate_decile_returns using dummy data
def test_calculate_decile_returns():
    # Create a dummy merged dataframe with 3 unique dates and two decile groups (11 and 12)
    dates = pd.date_range(start="2020-01-31", periods=3, freq="M")
    data = {
        "date": list(dates) * 2,
        "decile": [11] * 3 + [12] * 3,
        "amount_outstanding": [100, 100, 100, 200, 200, 200],
        "ret_eom_fwd": [0.02, 0.03, 0.04, 0.03, 0.04, 0.05],
    }
    merged = pd.DataFrame(data)
    
    portfolio_returns_fwd, portfolio_returns_norm = calc_nozawa_portfolio.calculate_decile_returns(merged)
    
    # Check that 'date' column exists in the pivoted dataframe
    assert "date" in portfolio_returns_fwd.columns, "The output should have a 'date' column."
    
    # Check that decile columns (11 and 12) exist
    assert '11' in portfolio_returns_fwd.columns, "Decile 11 should be a column."
    assert '12' in portfolio_returns_fwd.columns, "Decile 12 should be a column."
    
    # Check that the number of rows equals number of unique dates minus one (last date dropped)
    unique_dates = merged["date"].nunique()
    assert len(portfolio_returns_fwd) == unique_dates - 1, "The number of rows should equal unique dates minus one."

# Test for split_decile_returns using dummy data
def test_split_decile_returns():
    # Create dummy decile returns data
    dates = pd.date_range(start="2020-01-31", periods=10, freq="M")
    decile_data = {"date": dates}
    for dec in range(11, 21):
        decile_data[str(dec)] = 0.01 + np.linspace(0, 0.009, len(dates))
    decile_returns_df = pd.DataFrame(decile_data)
    
    # Create dummy US corp benchmark data with the same dates
    us_corp_data = {"date": dates}
    for dec in range(11, 21):
        us_corp_data["US_bonds_" + str(dec)] = 2 * decile_returns_df[str(dec)]
    us_corp_df = pd.DataFrame(us_corp_data)
    
    replication_df, updated_reproduction_df = calc_metrics.split_decile_returns(decile_returns_df, us_corp_df)
    
    cutoff_date = us_corp_df["date"].max()
    if not replication_df.empty:
        assert replication_df["date"].max() <= cutoff_date, "Replication data dates should be on or before the cutoff date."
    if not updated_reproduction_df.empty:
        assert updated_reproduction_df["date"].min() > cutoff_date, "Updated reproduction data dates should be after the cutoff date."