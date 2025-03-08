import pull_he_kelly_manela_factors
import calc_nozawa_portfolio
from settings import config
from pathlib import Path
DATA_DIR = Path(config("DATA_DIR"))
OUTPUT_DIR = Path(config("OUTPUT_DIR"))

import numpy as np
from matplotlib import pyplot as plt
import seaborn as sns

sns.set()

test_df = pull_he_kelly_manela_factors.load_he_kelly_manela_factors(data_dir=DATA_DIR)
nozawa_df = calc_nozawa_portfolio.load_nozawa(output_dir=OUTPUT_DIR)

(
    100 * 
    df[['CPIAUCNS', 'GDPC1']]
    .rename(columns={'CPIAUCNS':'Inflation', 'GDPC1':'Real GDP'})
    .dropna()
    .pct_change(4)
    ).plot()
plt.title("Inflation and Real GDP, Seasonally Adjusted")
plt.ylabel('Percent change from 12-months prior')
filename = OUTPUT_DIR / 'updated_reproduction.png'
plt.savefig(filename);
