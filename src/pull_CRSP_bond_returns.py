"""
This module pulls and processes corporate bond return data from WRDS. It 
queries the WRDS BONDRET database for selected fields (including CUSIP, date, 
price, yield, time-to-maturity, amount outstanding, and monthly return) for bonds 
within a specified date range. The retrieved data is then augmented with a 
'year' column (extracted from the date) and saved as a parquet file for later use.

Usage:
    When run as a script, this module connects to WRDS using the provided 
    credentials and configuration from settings.py, executes the SQL query 
    defined in `pull_bond_returns()`, and saves the resulting DataFrame to 
    DATA_DIR / "Bondreturns.parquet".

Configuration:
    The module retrieves key settings from the project's configuration via 
    settings.py:
      - DATA_DIR: Directory where data files are stored.
      - WRDS_USERNAME: WRDS username for the database connection.
      - START_DATE: Start date for the query (format YYYY-MM-DD).
      - END_DATE: End date for the query (format YYYY-MM-DD).

References:
    - WRDS BONDRET data documentation: https://wrds-www.wharton.upenn.edu/pages/get-data/wrds-bond-returns/
    - Example code from: https://github.com/zhangruoxikathywork/corporate_bond_liquidity_research/blob/main/src/load_wrds_bondret.py

Functions:
    - pull_bond_returns(wrds_username, start_date, end_date)
        Connects to WRDS, queries the BONDRET table for selected fields, and 
        returns the DataFrame with an additional 'year' column.
    
    - load_bondret(data_dir)
        Loads the saved Bondreturns.parquet file from DATA_DIR.
    
    - _demo()
        A demonstration function to test the load_bondret() function.
        
When run as a script, the module executes the pull_bond_returns() function 
and saves the resulting DataFrame as a parquet file.
"""

from pathlib import Path

import pandas as pd
from pandas.tseries.offsets import MonthEnd, YearEnd
import numpy as np
import wrds
from settings import config

DATA_DIR = Path(config("DATA_DIR"))
WRDS_USERNAME = config("WRDS_USERNAME")
START_DATE = config("START_DATE")
END_DATE = config("END_DATE")

# data from: https://wrds-www.wharton.upenn.edu/pages/get-data/wrds-bond-returns/
description_bondret = {
    "date": "Date",
    "issue_id": "Mergent FISD Issue Id",
    "cusip": "CUSIP ID",
    "bond_sym_id": "TRACE Bond Symbol",
    "bsym": "Bloomberg Identifier",
    "isin": "ISIN",
    "company_symbol": "Company Symbol (issuer stock ticker)",
    "bond_type": "Corporate Bond Types: Convertible, Debenture, Medium Term Note, MTN Zero",
    "security_level": "Indicates if the security is a secured, senior or subordinated issue of the issuer",
    "conv": "Flag Convertible",
    "offering_date": "Offering Date",
    "offering_amt": "Offering Amount",
    "offering_price": "Offering Price",
    "principal_amt": "The face or par value of a single bond (sum to be paid at maturity)",
    "maturity": "Maturity Date",
    "treasury_maturity": "Treasury Maturity",
    "coupon": "The current applicable annual interest rate that the bond's issuer is obligated to pay bondholders",
    "day_count_basis": "Basis used for determining the interest paid during each interest period (eg 30/360, ACT/360)",
    "dated_date": "Date from which interest accrues or from which original issue discount is amortized",
    "first_interest_date": "Date on which first interest payment will be made to bondholder",
    "last_interest_date": "Date on which last interest payment will be made",
    "ncoups": "Number of coupons per year",
    "amount_outstanding": "Amount oustanding",
    "n_mr": "Numeric Moody Rating (1=AAA)",
    "tmt": "Time to Maturity (Years)",
    "yield": "Yield",
    "t_yld_pt": "Average	trade‐weighted	yield	point",
    "ret_ldm" : "Monthly	return	calculated	based	on	PRICE_LDM	and	accrued	coupon	interest",
    "ret_l5m" : "Monthly	return	calculated	based	on	PRICE_L5M	and	accrued	coupon	interest",
    "ret_eom" : "Monthly	return	calculated	based	on	PRICE_EOM (Last	price	at	which	bond	was	traded	in	a	given month) and	accrued	coupon	interest"
}

#Referenced 
#https://github.com/zhangruoxikathywork/corporate_bond_liquidity_research/blob/main/src/load_wrds_bondret.py
def pull_bond_returns(wrds_username=WRDS_USERNAME, start_date=START_DATE, end_date=END_DATE):

    # All query fields
    # cusip, date, price_eom, price_ldm, price_l5m,
    # bsym, isin, company_symbol, bond_type, rating_cat, tmt,
    # rating_class, t_date, t_volume, t_dvolume, t_spread,
    # security_level, conv, offering_date, offering_amt, offering_price,
    # principal_amt, maturity, treasury_maturity, coupon, day_count_basis,
    # dated_date, first_interest_date, last_interest_date, ncoups,
    # r_sp, r_mr, r_fr, n_sp, n_mr, n_fr, rating_num

    query = f"""
        SELECT 
            cusip, date, price_eom, tmt, amount_outstanding,
            yield, t_yld_pt, ret_eom
        FROM 
            WRDSAPPS.BONDRET
        WHERE 
            date BETWEEN '{start_date}' AND '{end_date}'
    """
    db = wrds.Connection(wrds_username=WRDS_USERNAME)
    bond = db.raw_sql(query, date_cols=['date'])
    db.close()

    bond["year"] = bond["date"].dt.year
    return bond


def load_bondret(data_dir=DATA_DIR):
    path = Path(data_dir) / "CRSP_bond_returns.parquet"
    bond = pd.read_parquet(path)
    return bond


def _demo():
    comp = load_bondret(data_dir=DATA_DIR)


if __name__ == "__main__":
   comp = pull_bond_returns(wrds_username=WRDS_USERNAME)
   comp.to_parquet(DATA_DIR / "CRSP_bond_returns.parquet")
