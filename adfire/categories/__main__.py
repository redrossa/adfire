from adfire.io import write_record

global portfolio


def main():
    df = portfolio.linted
    worth_sum = df.groupby('category')['worth'].sum()
    amount = abs(worth_sum)
    amount.name = 'amount'
    amount_df = amount.reset_index().set_index('category')
    write_record(amount_df, 'index.csv', index=True)


if __name__ == "__main__":
    main()
