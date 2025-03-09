r"""
Code to generate tables for latex.

You can test out the latex code in the following minimal working
example document:

\documentclass{article}
\usepackage{booktabs}
\begin{document}
First document. This is a simple example, with no 
extra parameters or packages included.

\begin{table}
\centering
YOUR LATEX TABLE CODE HERE
%\input{example_table.tex}
\end{table}
\end{document}
"""
import calc_metrics
import pull_he_kelly_manela_factors

import pandas as pd
import numpy as np
from pathlib import Path
from settings import config

DATA_DIR = Path(config("DATA_DIR"))
OUTPUT_DIR = Path(config("OUTPUT_DIR"))

def generate_latex_tables():
    """
    Loads the replication, benchmark, and analysis DataFrames from their respective parquet files,
    adjusts the benchmark US corporate bonds data so that its start date matches that of the replication data,
    and generates LaTeX tables from each DataFrame. The generated tables are saved to the OUTPUT_DIR as:
    
      - 'replication_table.tex' for nozawa_replication.parquet (a sample of the first 10 rows),
      - 'us_corp_table.tex' for us_corp_bonds.parquet (first 10 rows after date adjustment),
      - 'analysis_table.tex' for analysis.parquet (the full analysis table).
    """
    # Load the replication DataFrame from OUTPUT_DIR.
    replication_df = calc_metrics.load_replication(OUTPUT_DIR)
    
    # Load the benchmark US corporate bonds DataFrame from DATA_DIR.
    us_corp_df = pull_he_kelly_manela_factors.load_us_corp_bonds(DATA_DIR)
    
    # Adjust us_corp_df so that it starts at the same date as replication_df.
    start_date = replication_df['date'].min()
    us_corp_df = us_corp_df[us_corp_df['date'] >= start_date].copy()
    
    # Load the analysis DataFrame from OUTPUT_DIR.
    analysis_df = calc_metrics.load_analysis(OUTPUT_DIR)
    
    # Suppress scientific notation and limit floats to 2 decimal places.
    pd.set_option('display.float_format', lambda x: '%.2f' % x)
    float_format_func = lambda x: '{:.2f}'.format(x)
    
    # Generate LaTeX table strings.
    # For replication and us_corp, we take a sample of the first 10 rows.
    latex_rep = replication_df.head(10).to_latex(float_format=float_format_func, index=False)
    latex_us_corp = us_corp_df.head(10).to_latex(float_format=float_format_func, index=False)
    # For analysis, we output the full table.
    latex_analysis = analysis_df.to_latex(float_format=float_format_func, index=False)
    
    # Save each LaTeX table to a file in OUTPUT_DIR.
    with open(OUTPUT_DIR / 'replication_table.tex', "w") as f:
        f.write(latex_rep)
    with open(OUTPUT_DIR / 'us_corp_table.tex', "w") as f:
        f.write(latex_us_corp)
    with open(OUTPUT_DIR / 'analysis_table.tex', "w") as f:
        f.write(latex_analysis)

if __name__ == "__main__":
    generate_latex_tables()