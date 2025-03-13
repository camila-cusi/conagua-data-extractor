import pandas as pd


def filtrar_datos(
    df: pd.DataFrame,
    start_date: str = None,
    end_date: str = None
) -> pd.DataFrame:
    """
    df: pd.dataframe of temperature or rain.
    start_date / end_date: format "YYYY-mm-dd"
    """
    clima_df = df.copy()
    # Ensure the index is a DatetimeIndex.
    if not isinstance(clima_df.index, pd.DatetimeIndex):
        clima_df.index = pd.to_datetime(clima_df.index)

    # Convert start and end dates to datetime objects, if provided.
    if start_date:
        start_date = pd.to_datetime(start_date)
    if end_date:
        end_date = pd.to_datetime(end_date)

    # Apply the filtering based on provided dates.
    if start_date and end_date:
        return clima_df.loc[
            (clima_df.index >= start_date) & (clima_df.index <= end_date)
        ]
    elif start_date:
        return clima_df.loc[clima_df.index >= start_date]
    elif end_date:
        return clima_df.loc[clima_df.index <= end_date]
    else:
        # No filtering if no dates are provided.
        return clima_df
