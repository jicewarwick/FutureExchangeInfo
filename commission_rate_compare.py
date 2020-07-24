import datetime as dt
import json
import os

import pandas as pd

from future_settle_info import FutureExchangeSettleInfo


def gather_account_commission_rate(data_path: str) -> pd.DataFrame:
    file_names = os.listdir(data_path)
    file_names.remove('login_info_full.json')
    file_names = [file for file in file_names if '.json' in file]
    storage = []
    for file in file_names:
        path = os.path.join(data_path, file)
        with open(path, 'r', encoding='utf-8') as f:
            data = json.load(f)
        for account in data['account_info']:
            account_name = account['account_name']
            broker = account['broker_name']
            commission_rate = pd.DataFrame(account['commission_rate'])
            commission_rate['account_name'] = account_name
            commission_rate['broker_name'] = broker
            storage.append(commission_rate)

    data = pd.concat(storage)
    data.drop('exchange_id', axis=1, inplace=True)
    data.close_ratio_by_money = data.close_ratio_by_money * 10000
    data.close_today_ratio_by_money = data.close_today_ratio_by_money * 10000
    data.open_ratio_by_money = data.open_ratio_by_money * 10000
    data.rename({'instrument_id': '合约代码', 'account_name': '产品', 'broker_name': '经纪商', 'close_ratio_by_money': '平仓/额',
                 'close_ratio_by_volume': '平仓/手',
                 'close_today_ratio_by_money': '平今/额', 'close_today_ratio_by_volume': '平今/手',
                 'open_ratio_by_money': '开仓/额', 'open_ratio_by_volume': '开仓/手'}, axis=1, inplace=True)
    data.set_index(['合约代码', '产品', '经纪商'], inplace=True)
    data = data.where(data.values > 0.01, 0)
    data = data.loc[:, ['开仓/额', '平仓/额', '平今/额', '开仓/手', '平仓/手', '平今/手']]
    return data


def compare_commission_rates(exchange_commission: pd.DataFrame, account_commission: pd.DataFrame) -> pd.DataFrame:
    account_commission = account_commission.reset_index(['经纪商', '产品'])
    exchange_commission.columns = ['交易所' + it for it in exchange_commission.columns]
    specific_settings = exchange_commission.join(account_commission, how='inner')
    specific_settings.set_index(['产品', '经纪商'], append=True, inplace=True)

    product_id = exchange_commission.index.str.extractall('([a-zA-Z]*)').loc[(slice(None), 0), :].values.flatten()
    general_settings = pd.merge(exchange_commission, account_commission, left_on=product_id, right_index=True)
    general_settings.drop('key_0', axis=1, inplace=True)
    general_settings.set_index(['产品', '经纪商'], append=True, inplace=True)
    general_settings.update(specific_settings, overwrite=True)

    target_cols = ['平仓/额', '平今/额', '开仓/额', '平仓/手', '平今/手', '开仓/手']
    exchange_cols = ['交易所' + it for it in target_cols]
    account_rate = general_settings.loc[:, target_cols]
    exchange_rate = general_settings.loc[:, exchange_cols]
    ratio = account_rate.values / exchange_rate.values
    ratio_df = pd.DataFrame(ratio, columns=target_cols, index=general_settings.index)
    ratio_df['平仓/额'].update(ratio_df['平仓/手'])
    ratio_df['平今/额'].update(ratio_df['平今/手'])
    ratio_df['开仓/额'].update(ratio_df['开仓/手'])
    ratio_df = ratio_df.loc[:, ['开仓/额', '平仓/额', '平今/额']]
    ratio_df.columns = ['开仓提价', '平仓提价', '平今提价']
    ratio_df = (ratio_df - 1) * 100

    res = pd.concat([general_settings, ratio_df], axis=1)
    res = res.reorder_levels(['经纪商', '产品', '合约代码']).sort_index()
    return res


if __name__ == '__main__':
    root = 'D:/我的表/期货手续费/'
    account_commission = gather_account_commission_rate(root)

    date = dt.date.today() - dt.timedelta(days=1)
    settle_info_obj = FutureExchangeSettleInfo()
    exchange_commission = settle_info_obj.get_commission_rate(date)

    res = compare_commission_rates(exchange_commission, account_commission)
    res.to_excel('comparision.xlsx', freeze_panes=(1, 3))
