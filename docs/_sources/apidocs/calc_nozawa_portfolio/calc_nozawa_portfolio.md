# {py:mod}`calc_nozawa_portfolio`

```{py:module} calc_nozawa_portfolio
```

```{autodoc2-docstring} calc_nozawa_portfolio
:allowtitles:
```

## Module Contents

### Functions

````{list-table}
:class: autosummary longtable
:align: left

* - {py:obj}`process_open_source_bond_data <calc_nozawa_portfolio.process_open_source_bond_data>`
  - ```{autodoc2-docstring} calc_nozawa_portfolio.process_open_source_bond_data
    :summary:
    ```
* - {py:obj}`process_crsp_bond_data <calc_nozawa_portfolio.process_crsp_bond_data>`
  - ```{autodoc2-docstring} calc_nozawa_portfolio.process_crsp_bond_data
    :summary:
    ```
* - {py:obj}`merge_bond_data <calc_nozawa_portfolio.merge_bond_data>`
  - ```{autodoc2-docstring} calc_nozawa_portfolio.merge_bond_data
    :summary:
    ```
* - {py:obj}`process_merged_bond_data <calc_nozawa_portfolio.process_merged_bond_data>`
  - ```{autodoc2-docstring} calc_nozawa_portfolio.process_merged_bond_data
    :summary:
    ```
* - {py:obj}`process_all_data <calc_nozawa_portfolio.process_all_data>`
  - ```{autodoc2-docstring} calc_nozawa_portfolio.process_all_data
    :summary:
    ```
* - {py:obj}`calculate_decile_returns <calc_nozawa_portfolio.calculate_decile_returns>`
  - ```{autodoc2-docstring} calc_nozawa_portfolio.calculate_decile_returns
    :summary:
    ```
* - {py:obj}`plot_avg_yield_tr_ytm <calc_nozawa_portfolio.plot_avg_yield_tr_ytm>`
  - ```{autodoc2-docstring} calc_nozawa_portfolio.plot_avg_yield_tr_ytm
    :summary:
    ```
* - {py:obj}`load_nozawa <calc_nozawa_portfolio.load_nozawa>`
  - ```{autodoc2-docstring} calc_nozawa_portfolio.load_nozawa
    :summary:
    ```
* - {py:obj}`load_merged_data <calc_nozawa_portfolio.load_merged_data>`
  - ```{autodoc2-docstring} calc_nozawa_portfolio.load_merged_data
    :summary:
    ```
````

### Data

````{list-table}
:class: autosummary longtable
:align: left

* - {py:obj}`DATA_DIR <calc_nozawa_portfolio.DATA_DIR>`
  - ```{autodoc2-docstring} calc_nozawa_portfolio.DATA_DIR
    :summary:
    ```
* - {py:obj}`OUTPUT_DIR <calc_nozawa_portfolio.OUTPUT_DIR>`
  - ```{autodoc2-docstring} calc_nozawa_portfolio.OUTPUT_DIR
    :summary:
    ```
* - {py:obj}`START_DATE <calc_nozawa_portfolio.START_DATE>`
  - ```{autodoc2-docstring} calc_nozawa_portfolio.START_DATE
    :summary:
    ```
* - {py:obj}`END_DATE <calc_nozawa_portfolio.END_DATE>`
  - ```{autodoc2-docstring} calc_nozawa_portfolio.END_DATE
    :summary:
    ```
````

### API

````{py:data} DATA_DIR
:canonical: calc_nozawa_portfolio.DATA_DIR
:value: >
   'Path(...)'

```{autodoc2-docstring} calc_nozawa_portfolio.DATA_DIR
```

````

````{py:data} OUTPUT_DIR
:canonical: calc_nozawa_portfolio.OUTPUT_DIR
:value: >
   'Path(...)'

```{autodoc2-docstring} calc_nozawa_portfolio.OUTPUT_DIR
```

````

````{py:data} START_DATE
:canonical: calc_nozawa_portfolio.START_DATE
:value: >
   'config(...)'

```{autodoc2-docstring} calc_nozawa_portfolio.START_DATE
```

````

````{py:data} END_DATE
:canonical: calc_nozawa_portfolio.END_DATE
:value: >
   'config(...)'

```{autodoc2-docstring} calc_nozawa_portfolio.END_DATE
```

````

````{py:function} process_open_source_bond_data(open_df)
:canonical: calc_nozawa_portfolio.process_open_source_bond_data

```{autodoc2-docstring} calc_nozawa_portfolio.process_open_source_bond_data
```
````

````{py:function} process_crsp_bond_data(crsp_df)
:canonical: calc_nozawa_portfolio.process_crsp_bond_data

```{autodoc2-docstring} calc_nozawa_portfolio.process_crsp_bond_data
```
````

````{py:function} merge_bond_data(proc_open, proc_crsp)
:canonical: calc_nozawa_portfolio.merge_bond_data

```{autodoc2-docstring} calc_nozawa_portfolio.merge_bond_data
```
````

````{py:function} process_merged_bond_data(merged)
:canonical: calc_nozawa_portfolio.process_merged_bond_data

```{autodoc2-docstring} calc_nozawa_portfolio.process_merged_bond_data
```
````

````{py:function} process_all_data(open_df, crsp_df)
:canonical: calc_nozawa_portfolio.process_all_data

```{autodoc2-docstring} calc_nozawa_portfolio.process_all_data
```
````

````{py:function} calculate_decile_returns(merged)
:canonical: calc_nozawa_portfolio.calculate_decile_returns

```{autodoc2-docstring} calc_nozawa_portfolio.calculate_decile_returns
```
````

````{py:function} plot_avg_yield_tr_ytm(merged, save_path=None, show=True)
:canonical: calc_nozawa_portfolio.plot_avg_yield_tr_ytm

```{autodoc2-docstring} calc_nozawa_portfolio.plot_avg_yield_tr_ytm
```
````

````{py:function} load_nozawa(output_dir=OUTPUT_DIR)
:canonical: calc_nozawa_portfolio.load_nozawa

```{autodoc2-docstring} calc_nozawa_portfolio.load_nozawa
```
````

````{py:function} load_merged_data(output_dir=DATA_DIR)
:canonical: calc_nozawa_portfolio.load_merged_data

```{autodoc2-docstring} calc_nozawa_portfolio.load_merged_data
```
````
