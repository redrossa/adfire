import matplotlib.pyplot as plt
import pandas as pd

from adfire.io import write_record

global portfolio


def categorize_by_year_month(df: pd.DataFrame) -> pd.DataFrame:
    df['month'] = pd.to_datetime(df['date']).dt.month
    df['year'] = pd.to_datetime(df['date']).dt.year
    monthly_series = df.groupby(['month', 'year', 'category'])['amount'].sum()
    df = monthly_series.reset_index()
    df = df.pivot(index=['year', 'month'], columns='category', values='amount')

    df = df.fillna(0)
    df = df.sort_index()
    df = abs(df)

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
    df = df.sort_index()
    df = df.round(2)
    write_record(df, 'categories.csv', index=True)


def plot_linear(df: pd.DataFrame, title: str, output_filemae: str) -> None:
    df = df.reset_index()
    df['year_month'] = pd.to_datetime(df['year'].astype(str) + '-' + df['month'].astype(str))

    plt.figure(figsize=(10, 6))

    for category in df.columns:
        if category not in ['year_month', 'year', 'month']:
            plt.plot(df['year_month'], df[category], marker='o', label=category)

    plt.title(title, fontsize=16)
    plt.xlabel('Year-Month', fontsize=14)
    plt.ylabel('Amount', fontsize=14)
    plt.xticks(rotation=45)
    plt.grid(visible=True)
    plt.legend(title='Categories', fontsize=8)
    plt.tight_layout()

    plt.savefig(output_filemae)
    plt.close()


def main():
    df = portfolio.linted
    df = categorize_by_year_month(df)

    write_table(df)
    plot_linear(df, 'Trends of Categories Over Time', 'trends.png')
    plot_linear(df.cumsum(), 'Cumulative Amount of Categories Over Time', 'cumsum.png')


if __name__ == '__main__':
    main()
