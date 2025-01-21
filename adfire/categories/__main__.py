import pandas as pd

from adfire.io import write_record

global portfolio


def categorize_by_year_month(df: pd.DataFrame) -> pd.DataFrame:
    df['month'] = pd.to_datetime(df['date']).dt.month
    df['year'] = pd.to_datetime(df['date']).dt.year
    monthly_series = df.groupby(['month', 'year', 'category'])['amount'].sum()
    df = monthly_series.reset_index()
    df = df.pivot(index=['year', 'month'], columns='category', values='amount')
    return df


def write_table(df: pd.DataFrame) -> None:
    yearly_df = df.groupby(level='year').sum().reset_index()
    yearly_df['month'] = None
    yearly_df = yearly_df.set_index(['year', 'month'])

    all_df = df.sum().to_frame().T
    all_df['year'] = None
    all_df['month'] = None
    all_df = all_df.set_index(['year', 'month'])

    df = pd.concat([df, yearly_df, all_df])
    df = df.fillna(0)
    df = df.sort_index()
    df = abs(df)
    df = df.round(2)
    write_record(df, 'categories.csv', index=True)


def main():
    df = portfolio.linted
    df = categorize_by_year_month(df)

    write_table(df)


if __name__ == "__main__":
    main()
