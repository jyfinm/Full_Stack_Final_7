from pathlib import Path

import pandas as pd
from pandas.tseries.offsets import MonthEnd, YearEnd
import numpy as np
import wrds
from settings import config

OUTPUT_DIR = Path(config.OUTPUT_DIR)
DATA_DIR = Path(config.DATA_DIR)
WRDS_USERNAME = config.WRDS_USERNAME
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
    "tmt": "Time to Maturity (Years)"
}

#Referenced 
#https://github.com/zhangruoxikathywork/corporate_bond_liquidity_research/blob/main/src/load_wrds_bondret.py
def pull_bond_returns(wrds_username=WRDS_USERNAME, start_date='1960-01-31', end_date='2024-12-31'):
    db = wrds.Connection(wrds_username=WRDS_USERNAME)

    # All query fields
    # cusip, date, price_eom, price_ldm, price_l5m,
    # bsym, isin, company_symbol, bond_type, rating_cat, tmt,
    # rating_class, t_date, t_volume, t_dvolume, t_spread,
    # security_level, conv, offering_date, offering_amt, offering_price,
    # principal_amt, maturity, treasury_maturity, coupon, day_count_basis,
    # dated_date, first_interest_date, last_interest_date, ncoups,
    # amount_outstanding, r_sp, r_mr, r_fr, n_sp, n_mr, n_fr, rating_num

    query = f"""
        SELECT 
            cusip, date, price_eom, tmt,
            t_volume, t_dvolume, t_spread,
            offering_amt, offering_price,
            principal_amt, maturity, coupon, ncoups,
            amount_outstanding, r_mr, n_mr, offering_date
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
    path = Path(data_dir) / "Bondreturns.parquet"
    bond = pd.read_parquet(path)
    return bond


def _demo():
    comp = load_bondret(data_dir=DATA_DIR)


if __name__ == "__main__":
   comp = pull_bond_returns(wrds_username=WRDS_USERNAME)
   comp.to_parquet(DATA_DIR / "Bondreturns.parquet")
