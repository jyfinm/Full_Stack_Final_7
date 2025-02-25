from pathlib import Path

import wrds
import pandas as pd

OUTPUT_DIR = Path(config("OUTPUT_DIR"))
DATA_DIR = Path(config("DATA_DIR"))
WRDS_USERNAME = config("WRDS_USERNAME")

#See here: https://www.crsp.org/wp-content/uploads/guides/CRSP_US_Treasury_Database_Guide_for_SAS_ASCII_EXCEL_R.pdf
description_crsp_fama = {
    "mcaldt: Last Quotation Date in the Month",
    "kytreasnox: Treasury Record Identifier", 
    "tmewretd: Monthly Equal Weighted Portfolio Return"
}
def pull_fama_bond_portfolios(start_date='1960-01-31', end_date='2024-12-31'):
    db = wrds.Connection()

    # SQL query to pull Fama bond portfolio returns
    query = f"""
    SELECT kytreasnox, mcaldt, tmewretd
    FROM crsp_a_treasuries.tfz_mth_bp
    WHERE mcaldt BETWEEN '{start_date}' AND '{end_date}'
    ORDER BY mcaldt;
    """

    fama_bond_portfolios = db.raw_sql(query, date_cols=['mcaldt'])

    db.close()

    return fama_bond_portfolios

def load_fama_port_returns(data_dir=DATA_DIR):
    path = Path(data_dir) / "FamaBond.parquet"
    famabond = pd.read_parquet(path)
    return famabond

def _demo():
    comp = load_fama_port_returns(data_dir=DATA_DIR)

if __name__ == "__main__":
    comp = pull_fama_bond_portfolios(wrds_username=WRDS_USERNAME)
    comp.to_parquet(DATA_DIR / "FamaBond.parquet")