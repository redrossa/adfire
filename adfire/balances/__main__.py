from adfire.io import write_record
from adfire.schema import AccountBalancesSchema

global portfolio


def main():
    df = portfolio.linted
    last_df = df.groupby('account_name').last()
    last_df['balance'] = last_df['balance_total']

    mask_is_credit = last_df['account_type'] == 'credit'
    net_worth = last_df[~mask_is_credit]['balance_total'].sum() - last_df[mask_is_credit]['balance_total'].sum()
    last_df.loc['Net Worth'] = net_worth.round(2)

    last_df = AccountBalancesSchema.validate(last_df)
    write_record(last_df, 'balances.csv', index=True)


if __name__ == "__main__":
    main()
