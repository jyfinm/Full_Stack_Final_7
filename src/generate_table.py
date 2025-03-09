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
import re
from pathlib import Path
from settings import config

DATA_DIR = Path(config("DATA_DIR"))
OUTPUT_DIR = Path(config("OUTPUT_DIR"))

# Create a summary with a row of dots between the head and tail.
def create_summary(df):
    head_df = df.head(10)
    tail_df = df.tail(10)
    dots_row = pd.DataFrame({col: ["..."] for col in df.columns})
    return pd.concat([head_df, dots_row, tail_df], ignore_index=True)

def generate_latex_tables():
    """
    Loads the replication, benchmark, and analysis DataFrames from their respective parquet files,
    adjusts the benchmark US corporate bonds data so that its start date matches that of the replication data,
    and generates LaTeX tables from each DataFrame. The generated tables are saved to OUTPUT_DIR as:
    
      - 'replication_table.tex' for nozawa_replication.parquet (a sample with head, dots row, and tail),
      - 'us_corp_table.tex' for us_corp_bonds.parquet (same sample approach),
      - 'analysis_table.tex' for analysis.parquet (the full analysis table),
      - 'benchmark_summary.tex' for the benchmark summary DataFrame, and
      - 'replicate_summary.tex' for the replicate summary DataFrame.
    """
    # Load the replication DataFrame from OUTPUT_DIR.
    replication_df = calc_metrics.load_replication(OUTPUT_DIR)
    
    # Load the benchmark US corporate bonds DataFrame from DATA_DIR.
    us_corp_df = pull_he_kelly_manela_factors.load_us_corp_bonds(DATA_DIR)
    
    # Adjust us_corp_df so that it starts at the same date as replication_df.
    start_date = replication_df['date'].min()
    us_corp_df = us_corp_df[us_corp_df['date'] >= start_date].copy()
    
    # Convert the date columns to strings (YYYY-MM-DD).
    if 'date' in replication_df.columns:
        replication_df['date'] = replication_df['date'].dt.strftime("%Y-%m-%d")
    if 'date' in us_corp_df.columns:
        us_corp_df['date'] = us_corp_df['date'].dt.strftime("%Y-%m-%d")
    
    replication_summary = create_summary(replication_df)
    us_corp_summary = create_summary(us_corp_df)
    
    # Load the analysis DataFrame from OUTPUT_DIR.
    analysis_df = calc_metrics.load_analysis(OUTPUT_DIR)
    if 'date' in analysis_df.columns:
        analysis_df['date'] = analysis_df['date'].dt.strftime("%Y-%m-%d")
    
    # Load benchmark summary and replicate summary DataFrames.
    benchmark_summary_df = calc_metrics.load_benchmark_summary(OUTPUT_DIR)
    replicate_summary_df = calc_metrics.load_replicate_summary(OUTPUT_DIR)
    
    # Optionally, if these summary DataFrames contain date columns, convert them to strings.
    for df in [benchmark_summary_df, replicate_summary_df]:
        for col in ['start_date', 'end_date']:
            if col in df.columns:
                df[col] = df[col].astype(str)

    # Adjust column headers: remove all non numerical characters for some, add spaces and capitalize title for others
    remove_non_numeric = lambda s: re.sub(r'\D', '', s)
    replication_summary.columns = [remove_non_numeric(col) for col in replication_summary.columns]
    us_corp_summary.columns = [remove_non_numeric(col) for col in us_corp_summary.columns]
    analysis_df.columns = [col.replace('_', ' ').title() for col in analysis_df.columns]
    benchmark_summary_df.columns = [col.replace('_', ' ').title() for col in benchmark_summary_df.columns]
    replicate_summary_df.columns = [col.replace('_', ' ').title() for col in replicate_summary_df.columns]
    
    # Suppress scientific notation and limit floats to 4 decimal places.
    pd.set_option('display.float_format', lambda x: '%.4f' % x)
    float_format_func = lambda x: '{:.4f}'.format(x)
    
    # Generate LaTeX table strings.
    latex_rep = replication_summary.to_latex(float_format=float_format_func, index=False)
    latex_us_corp = us_corp_summary.to_latex(float_format=float_format_func, index=False)
    latex_analysis = analysis_df.to_latex(float_format=float_format_func, index=False)
    latex_benchmark_summary = benchmark_summary_df.to_latex(float_format=float_format_func, index=False)
    latex_replicate_summary = replicate_summary_df.to_latex(float_format=float_format_func, index=False)
    
    # Save each LaTeX table to a file in OUTPUT_DIR.
    with open(OUTPUT_DIR / 'replication_table.tex', "w") as f:
        f.write(latex_rep)
    with open(OUTPUT_DIR / 'us_corp_table.tex', "w") as f:
        f.write(latex_us_corp)
    with open(OUTPUT_DIR / 'analysis_table.tex', "w") as f:
        f.write(latex_analysis)
    with open(OUTPUT_DIR / 'benchmark_summary.tex', "w") as f:
        f.write(latex_benchmark_summary)
    with open(OUTPUT_DIR / 'replicate_summary.tex', "w") as f:
        f.write(latex_replicate_summary)

if __name__ == "__main__":
    generate_latex_tables()