import wrds
import pandas as pd

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

df = pull_fama_bond_portfolios()
print(df.head())  
