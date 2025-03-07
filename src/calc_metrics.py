def calculate_decile_analysis(decile_returns_df, us_corp_df):
    """
    Calculate analysis metrics comparing decile portfolio returns with benchmark US corporate bond returns.
    
    This function merges the decile returns DataFrame and the benchmark US corporate bonds DataFrame 
    on the 'date' column (using an inner join). It then computes, for each decile (11 to 20):
      - Pearson correlation between the replicated decile return and the benchmark return,
      - R² (square of the correlation),
      - Regression parameters (slope and intercept) from a linear regression of benchmark returns on the replication,
      - Mean Absolute Error (MAE) and Root Mean Squared Error (RMSE) from the regression,
      - Tracking error (standard deviation of the difference between benchmark and replicated returns).
    
    Parameters
    ----------
    decile_returns_df : pd.DataFrame
        DataFrame containing the replicated decile returns with a 'date' column and decile columns
        labeled as integers 11, 12, ..., 20.
    us_corp_df : pd.DataFrame
        DataFrame containing the benchmark US corporate bond returns with a 'date' column and columns
        named "US_bonds_11", "US_bonds_12", ..., "US_bonds_20".
        
    Returns
    -------
    analysis_df : pd.DataFrame
        A DataFrame with one row per decile and columns:
            - 'decile'
            - 'correlation'
            - 'r_squared'
            - 'slope'
            - 'intercept'
            - 'mae'
            - 'rmse'
            - 'tracking_error'
    """

    # Merge the two DataFrames on 'date'
    common_df = pd.merge(decile_returns_df, us_corp_df, on="date", how="inner", suffixes=('_ret', '_corp'))
    
    analysis_list = []
    for decile in range(11, 21):
        # In decile_returns_df, the column is simply the decile number (e.g., 11)
        ret_col = decile  
        # In us_corp_df, the benchmark column is named "US_bonds_" + decile
        corp_col = "US_bonds_" + str(decile)
        
        if ret_col in common_df.columns and corp_col in common_df.columns:
            sub_df = common_df[[ret_col, corp_col]].dropna()
            if len(sub_df) > 0:
                # Compute Pearson correlation and r^2.
                corr = sub_df[ret_col].corr(sub_df[corp_col])
                r2 = corr ** 2
                
                # Run a simple linear regression (using np.polyfit: benchmark ~ replication)
                x = sub_df[ret_col].values
                y = sub_df[corp_col].values
                slope, intercept = np.polyfit(x, y, 1)
                
                # Compute predicted values and residual metrics.
                y_pred = slope * x + intercept
                mae = np.mean(np.abs(y - y_pred))
                rmse = np.sqrt(np.mean((y - y_pred) ** 2))
                
                # Tracking error: standard deviation of the difference between benchmark and replication.
                tracking_error = np.std(y - x)
            else:
                corr = r2 = slope = intercept = mae = rmse = tracking_error = None
        else:
            corr = r2 = slope = intercept = mae = rmse = tracking_error = None

        analysis_list.append({
            "decile": decile,
            "correlation": corr,
            "r_squared": r2,
            "slope": slope,
            "intercept": intercept,
            "mae": mae,
            "rmse": rmse,
            "tracking_error": tracking_error
        })
    
    analysis_df = pd.DataFrame(analysis_list)
    return analysis_df