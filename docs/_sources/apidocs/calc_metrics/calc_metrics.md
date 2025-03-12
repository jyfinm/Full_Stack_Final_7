# {py:mod}`calc_metrics`

```{py:module} calc_metrics
```

```{autodoc2-docstring} calc_metrics
:allowtitles:
```

## Module Contents

### Functions

````{list-table}
:class: autosummary longtable
:align: left

* - {py:obj}`calc_summary <calc_metrics.calc_summary>`
  - ```{autodoc2-docstring} calc_metrics.calc_summary
    :summary:
    ```
* - {py:obj}`get_date_range <calc_metrics.get_date_range>`
  - ```{autodoc2-docstring} calc_metrics.get_date_range
    :summary:
    ```
* - {py:obj}`calculate_decile_analysis <calc_metrics.calculate_decile_analysis>`
  - ```{autodoc2-docstring} calc_metrics.calculate_decile_analysis
    :summary:
    ```
* - {py:obj}`split_decile_returns <calc_metrics.split_decile_returns>`
  - ```{autodoc2-docstring} calc_metrics.split_decile_returns
    :summary:
    ```
* - {py:obj}`plot_cumulative_returns <calc_metrics.plot_cumulative_returns>`
  - ```{autodoc2-docstring} calc_metrics.plot_cumulative_returns
    :summary:
    ```
* - {py:obj}`load_analysis <calc_metrics.load_analysis>`
  - ```{autodoc2-docstring} calc_metrics.load_analysis
    :summary:
    ```
* - {py:obj}`load_reproduction <calc_metrics.load_reproduction>`
  - ```{autodoc2-docstring} calc_metrics.load_reproduction
    :summary:
    ```
* - {py:obj}`load_replication <calc_metrics.load_replication>`
  - ```{autodoc2-docstring} calc_metrics.load_replication
    :summary:
    ```
* - {py:obj}`load_benchmark_summary <calc_metrics.load_benchmark_summary>`
  - ```{autodoc2-docstring} calc_metrics.load_benchmark_summary
    :summary:
    ```
* - {py:obj}`load_replicate_summary <calc_metrics.load_replicate_summary>`
  - ```{autodoc2-docstring} calc_metrics.load_replicate_summary
    :summary:
    ```
````

### Data

````{list-table}
:class: autosummary longtable
:align: left

* - {py:obj}`DATA_DIR <calc_metrics.DATA_DIR>`
  - ```{autodoc2-docstring} calc_metrics.DATA_DIR
    :summary:
    ```
* - {py:obj}`OUTPUT_DIR <calc_metrics.OUTPUT_DIR>`
  - ```{autodoc2-docstring} calc_metrics.OUTPUT_DIR
    :summary:
    ```
````

### API

````{py:data} DATA_DIR
:canonical: calc_metrics.DATA_DIR
:value: >
   'Path(...)'

```{autodoc2-docstring} calc_metrics.DATA_DIR
```

````

````{py:data} OUTPUT_DIR
:canonical: calc_metrics.OUTPUT_DIR
:value: >
   'Path(...)'

```{autodoc2-docstring} calc_metrics.OUTPUT_DIR
```

````

````{py:function} calc_summary(series)
:canonical: calc_metrics.calc_summary

```{autodoc2-docstring} calc_metrics.calc_summary
```
````

````{py:function} get_date_range(df, col)
:canonical: calc_metrics.get_date_range

```{autodoc2-docstring} calc_metrics.get_date_range
```
````

````{py:function} calculate_decile_analysis(decile_returns_df, us_corp_df)
:canonical: calc_metrics.calculate_decile_analysis

```{autodoc2-docstring} calc_metrics.calculate_decile_analysis
```
````

````{py:function} split_decile_returns(decile_returns_df, us_corp_df)
:canonical: calc_metrics.split_decile_returns

```{autodoc2-docstring} calc_metrics.split_decile_returns
```
````

````{py:function} plot_cumulative_returns(reproduction_df, save_path=None, show=True)
:canonical: calc_metrics.plot_cumulative_returns

```{autodoc2-docstring} calc_metrics.plot_cumulative_returns
```
````

````{py:function} load_analysis(output_dir=OUTPUT_DIR)
:canonical: calc_metrics.load_analysis

```{autodoc2-docstring} calc_metrics.load_analysis
```
````

````{py:function} load_reproduction(output_dir=OUTPUT_DIR)
:canonical: calc_metrics.load_reproduction

```{autodoc2-docstring} calc_metrics.load_reproduction
```
````

````{py:function} load_replication(output_dir=OUTPUT_DIR)
:canonical: calc_metrics.load_replication

```{autodoc2-docstring} calc_metrics.load_replication
```
````

````{py:function} load_benchmark_summary(output_dir=OUTPUT_DIR)
:canonical: calc_metrics.load_benchmark_summary

```{autodoc2-docstring} calc_metrics.load_benchmark_summary
```
````

````{py:function} load_replicate_summary(output_dir=OUTPUT_DIR)
:canonical: calc_metrics.load_replicate_summary

```{autodoc2-docstring} calc_metrics.load_replicate_summary
```
````
