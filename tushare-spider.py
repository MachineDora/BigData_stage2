import tushare as ts
import pandas as pd
import time


def getDaily(trade_date=''):
    success = False
    while not success:
        try:
            daily_data = pro.daily(trade_date=trade_date)
            success = True
        except:
            time.sleep(0.5)
    return daily_data


pro = ts.pro_api(
    'b335a04bce16aa1b4de6100e313105d2e0c7bfa2a25b4be3098a8114')

START_DATE = '20190628'  # 爬取日行情开始日期
END_DATE = '20200628'  # 爬取日行情结束日期

trade_cal = pro.trade_cal(exchange='',  # 交易所 SSE上交所 SZSE深交所, 默认为SSE\
                          is_open='1',
                          start_date=START_DATE,
                          end_date=END_DATE,
                          fields='cal_date')

columns_map = {
    'ts_code': '股票代码',
    'trade_date': '交易日期',
    'open': '开盘价',
    'high': '最高价',
    'low': '最低价',
    'close': '收盘价',
    'pre_close': '昨日收盘价',
    'change': '涨跌额',
    'pct_chg': '涨跌幅（未复权）',
    'vol': '成交量（手）',
    'amount': '成交额（千元）'
}

daily_data = getDaily('20190628')
daily_data.rename(columns=columns_map, inplace=True)

for date in trade_cal['cal_date'].values:
    daily_data = getDaily(date)
    daily_data.rename(columns=columns_map, inplace=True)
    daily_data.to_csv(f'./daily_data/{date}.csv')
