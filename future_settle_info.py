import datetime as dt
from io import StringIO, BytesIO

import pandas as pd
import requests


class FutureExchangeSettleInfoBase(object):
    @staticmethod
    def get_commission_rate(date: dt.date) -> pd.DataFrame:
        raise NotImplementedError


class DCESettleInfo(FutureExchangeSettleInfoBase):
    @staticmethod
    def get_commission_rate(date: dt.date) -> pd.DataFrame:
        dce_url = 'http://www.dce.com.cn/publicweb/businessguidelines/exportFutAndOptSettle.html'
        param = {"variety": "all", "trade_type": "0", "year": str(date.year), "month": str(date.month-1),
                 "day": str(date.day), "exportFlag": "excel"}
        rsp = requests.post(dce_url, data=param)
        commission_raw = pd.read_excel(BytesIO(rsp.content))
        ind = commission_raw['手续费收取方式'] == '绝对值'
        commission_raw['平今手续费'] = commission_raw['短线开仓手续费'] + commission_raw['短线平仓手续费'] - commission_raw['开仓手续费']
        commission_raw = commission_raw.loc[:, ['合约代码', '开仓手续费', '平仓手续费', '平今手续费']]
        by_amount = commission_raw.loc[ind, :].copy()
        by_amount.rename({'开仓手续费': '开仓/手', '平仓手续费': '平仓/手', '平今手续费': '平今/手'}, axis=1, inplace=True)
        by_amount.set_index('合约代码', inplace=True)
        by_ratio = commission_raw.loc[~ind, :].copy()
        by_ratio.rename({'开仓手续费': '开仓/额', '平仓手续费': '平仓/额', '平今手续费': '平今/额'}, axis=1, inplace=True)
        by_ratio.set_index('合约代码', inplace=True)
        commission = pd.concat([by_ratio, by_amount], axis=1, sort=True).fillna(0)
        commission['交易所'] = 'DCE'
        commission = commission.loc[:, ['交易所', '开仓/额', '平仓/额', '平今/额', '开仓/手', '平仓/手', '平今/手']]
        commission.index.name = '合约代码'
        return commission


class ZCESettleInfo(FutureExchangeSettleInfoBase):
    @staticmethod
    def get_commission_rate(date: dt.date) -> pd.DataFrame:
        zss_url = f'http://www.czce.com.cn/cn/DFSStaticFiles/Future/{date.year}/{date.strftime("%Y%m%d")}/FutureDataClearParams.htm'
        rsp = requests.get(zss_url)
        rsp.encoding = 'utf-8'
        commission_raw = pd.read_html(StringIO(rsp.text))[0]
        commission_raw = commission_raw.iloc[:-1, :]
        commission = commission_raw.loc[:, ['合约代码']]
        commission['开仓/额'] = 0
        commission['平仓/额'] = 0
        commission['平今/额'] = 0
        commission['开仓/手'] = commission_raw['交易手续费'].astype(float)
        commission['平仓/手'] = commission_raw['交易手续费'].astype(float)
        commission['平今/手'] = commission_raw['平今仓手续费'].astype(float)
        commission['交易所'] = 'ZCE'
        commission.set_index('合约代码', inplace=True)
        commission = commission.loc[:, ['交易所', '开仓/额', '平仓/额', '平今/额', '开仓/手', '平仓/手', '平今/手']]
        return commission


class ShanghaiFutureExchange(FutureExchangeSettleInfoBase):
    @staticmethod
    def get_commission_rate(date: dt.date):
        raise NotImplementedError

    @staticmethod
    def process_data(url: str) -> pd.DataFrame:
        rsp = requests.get(url)
        commission_raw = pd.DataFrame(rsp.json()['Settlement'])
        commission_raw.loc[:, ['TRADEFEERATION', 'TRADEFEEUNIT', 'DISCOUNTRATE']] = commission_raw.loc[:,
                                                                                    ['TRADEFEERATION', 'TRADEFEEUNIT',
                                                                                     'DISCOUNTRATE']].astype(float)
        commission = commission_raw.loc[:, ['INSTRUMENTID', 'TRADEFEERATION', 'TRADEFEEUNIT']]
        commission.rename({'INSTRUMENTID': '合约代码', 'TRADEFEERATION': '开仓/额', 'TRADEFEEUNIT': '开仓/手'}, axis=1,
                          inplace=True)
        commission['开仓/额'] = commission['开仓/额'] * 10000
        commission['平仓/额'] = commission['开仓/额']
        commission['平仓/手'] = commission['开仓/手']
        commission['平今/额'] = commission['开仓/额'] * commission_raw['DISCOUNTRATE']
        commission['平今/手'] = commission['开仓/手'] * commission_raw['DISCOUNTRATE']
        commission['交易所'] = ''
        commission.set_index('合约代码', inplace=True)
        commission = commission.loc[:, ['交易所', '开仓/额', '平仓/额', '平今/额', '开仓/手', '平仓/手', '平今/手']]
        return commission


class SHFESettleInfo(ShanghaiFutureExchange):
    @staticmethod
    def get_commission_rate(date: dt.date) -> pd.DataFrame:
        shfe_url = f'http://www.shfe.com.cn/data/instrument/Settlement{date.strftime("%Y%m%d")}.dat'
        commission = ShanghaiFutureExchange.process_data(shfe_url)
        commission['交易所'] = 'SHFE'
        return commission


class INESettleInfo(ShanghaiFutureExchange):
    @staticmethod
    def get_commission_rate(date: dt.date) -> pd.DataFrame:
        ine_url = f'http://www.ine.cn/data/instrument/Settlement{date.strftime("%Y%m%d")}.dat?rnd=0.69952368535167'
        commission = ShanghaiFutureExchange.process_data(ine_url)
        commission['交易所'] = 'INE'
        return commission


class FutureExchangeSettleInfo(FutureExchangeSettleInfoBase):
    @staticmethod
    def get_commission_rate(date: dt.date) -> pd.DataFrame:
        exchanges_settle_info = [SHFESettleInfo(), INESettleInfo(), DCESettleInfo(), ZCESettleInfo()]
        commission_info = [it.get_commission_rate(date) for it in exchanges_settle_info]
        commission = pd.concat(commission_info, sort=False).sort_index()
        return commission


if __name__ == '__main__':
    date = dt.date.today() - dt.timedelta(days=1)
    settle_info_obj = FutureExchangeSettleInfo()
    commission = settle_info_obj.get_commission_rate(date)

