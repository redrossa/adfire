from adfire.io import write_record
from adfire.lint.accounts import parse_config

global portfolio


def main():
    df = portfolio.linted
    accounts_mapping = parse_config(portfolio.config)
    mapped_df = df.merge(accounts_mapping, left_on=accounts_mapping.index.names, right_index=True, how='left')
    balances_df = mapped_df.groupby(['account_id', 'symbol'])['amount'].sum()
    balances_df = balances_df.reset_index().pivot(index='account_id', columns='symbol', values='amount')
    balances_df['worth'] = mapped_df.groupby('account_id')['worth'].sum()
    reverse_mapping = accounts_mapping.reset_index().groupby('account_id').last()
    balances_df.index = reverse_mapping.loc[balances_df.index.astype(int), 'account_name']
    write_record(balances_df, 'index.csv', index=True)


if __name__ == "__main__":
    main()
