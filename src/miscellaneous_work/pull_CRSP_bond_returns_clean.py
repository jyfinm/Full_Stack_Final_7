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

# data from: https://wrds-www.wharton.upenn.edu/pages/get-data/wrds-bond-returns/wrds-clean-trace-enhanced-file/
description_bondret_clean = {
    "cusip_id":"CUSIP ID",
    "bond_sym_id" : "TRACE Bond symbol",
    "company_symbol": "Company Symbol (issuer stock ticker)",
    "trd_exctn_dt" : "Execution Date",
    "trd_exctn_tm" : "Execution Time",
    "yld_pt" : "Yield",
    "yld_sign_cd":"Yield Direction",
    "rptd_pr":"Price"

}

def pull_bond_returns_clean(wrds_username=WRDS_USERNAME, start_date=START_DATE, end_date=END_DATE):

    query = f"""
        SELECT 
            cusip_id, trd_exctn_dt, yld_pt
        FROM 
            wrdsapps_bondret.TRACE_ENHANCED_CLEAN
        WHERE 
            trd_exctn_dt BETWEEN '{start_date}' AND '{end_date}'
    """
    db = wrds.Connection(wrds_username=WRDS_USERNAME)
    bond = db.raw_sql(query, date_cols=['trd_exctn_dt'])
    db.close()

    return bond


def load_bondret(data_dir=DATA_DIR):
    path = Path(data_dir) / "BondreturnsClean.parquet"
    bond = pd.read_parquet(path)
    return bond


def _demo():
    comp = load_bondret(data_dir=DATA_DIR)


if __name__ == "__main__":
   comp = pull_bond_returns_clean(wrds_username=WRDS_USERNAME)
   comp.to_parquet(DATA_DIR / "BondreturnsClean.parquet")
