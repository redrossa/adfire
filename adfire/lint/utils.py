import pandas as pd
import pandera as pa


def filter_df_by_schema(df: pd.DataFrame, schema: pa.DataFrameSchema) -> pd.DataFrame:
    return df[schema.columns.keys()]