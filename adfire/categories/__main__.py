import sys

import matplotlib.pyplot as plt
import numpy as np
import pandas as pd

from adfire.io import write_record
from adfire.utils import get_worths

global portfolio


def sign(s: pd.Series) -> pd.Series:
    supercat = s.name.split('.')[0]
    return (-s if supercat in ['expenses', 'savings'] else s).replace(-0, 0)


def generate_descendants(category):
    if category is np.nan:
        return None
    parts = category.split('.')
    return ['.'.join(parts[:i + 1]) for i in range(len(parts))]


def categorize_by_year_month(df: pd.DataFrame, categories: list[str] = None) -> pd.DataFrame:
    df['month'] = pd.to_datetime(df['date']).dt.month
    df['year'] = pd.to_datetime(df['date']).dt.year
    defined_categories = df['category'].dropna().unique()
    df['category'] = df['category'].apply(generate_descendants)
    df = df.explode('category')

    df['worth'] = get_worths(df)
    monthly_series = df.groupby(['month', 'year', 'category'])['worth'].sum()
    df = monthly_series.reset_index()
    df = df.pivot(index=['year', 'month'], columns='category', values='worth')
    df = df[categories if categories else defined_categories]

    df = df.fillna(0)
    df = df.sort_index()
    df = df.apply(sign)

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


def plot_linear(df: pd.DataFrame, title: str, output_filename: str) -> None:
    df = df.reset_index()
    df['year_month'] = pd.to_datetime(df['year'].astype(str) + '-' + df['month'].astype(str))

    plt.figure(figsize=(10, 6))

    lines = []

    for category in df.columns:
        if category not in ['year_month', 'year', 'month']:
            line, = plt.plot(df['year_month'], df[category], marker='o', label=category)
            lines.append((df[category].iloc[-1], line))  # Store final y-value and line object

    # Sort lines by final y-values in descending order
    lines.sort(key=lambda item: item[0], reverse=True)

    # Extract sorted lines and labels
    sorted_lines = [line for _, line in lines]
    sorted_labels = [line.get_label() for line in sorted_lines]

    plt.title(title, fontsize=16)
    plt.xlabel('Year-Month', fontsize=14)
    plt.ylabel('Amount', fontsize=14)
    plt.xticks(rotation=45)
    plt.grid(visible=True)

    # Updated legend order
    plt.legend(sorted_lines, sorted_labels, title='Categories', fontsize=8)

    plt.tight_layout()
    plt.savefig(output_filename)
    plt.close()


def main():
    df = portfolio.linted
    df = categorize_by_year_month(df, categories=sys.argv[1:])

    write_table(df)
    plot_linear(df, 'Trends of Categories Over Time', 'trends.png')
    plot_linear(df.cumsum(), 'Cumulative Amount of Categories Over Time', 'cumsum.png')


if __name__ == '__main__':
    main()
