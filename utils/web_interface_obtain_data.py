import os
import sys

import pandas as pd
import baostock as bs

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from utils.log_utils import setup_logger_simple_msg
import akshare as ak

LOGGER = setup_logger_simple_msg(name='web_interface_obtain_data')

def akshare_em_obtain_daily(code, start_date=None, end_date=None, adjust=None):
    """
    akshare，东财接口
    """
    df = ak.stock_zh_a_hist(symbol=code, period="daily", adjust="")
    if df.shape[0] == 0:
        LOGGER.info(f'{code}未获取到数据')
        return
    df = df.rename(columns={
        '日期': 'date', '开盘': 'open', '最高': 'high', '最低': 'low',
        '收盘': 'close', '成交量': 'volume', '成交额': 'turnover'
    })
    df['code'] = code
    df = df[['code', 'date', 'open', 'high', 'low', 'close', 'volume', 'turnover']]
    df['date'] = pd.to_datetime(df['date'])
    df['open'] = pd.to_numeric(df['open'])
    df['high'] = pd.to_numeric(df['high'])
    df['low'] = pd.to_numeric(df['low'])
    df['close'] = pd.to_numeric(df['close'])
    df['volume'] = pd.to_numeric(df['volume'])
    # 从这个接口获取的数据，成交量的单位是百，要乘100
    df['volume'] = df['volume'] * 100
    df['turnover'] = pd.to_numeric(df['turnover'])
    return df

def get_baostock_code(code):
    if code.startswith('6'):
        baostock_code = f"sh.{code}"
    elif code.startswith('0') or code.startswith('3'):
        baostock_code = f"sz.{code}"
    else:
        baostock_code = code
    return baostock_code

def baostock_obtain_daily(code, start_date, end_date, adjust=None):
    baostock_adjust = '3'
    if adjust is not None:
        LOGGER.info('请检查复权参数是否正确')
        return None
    baostock_code = get_baostock_code(code)
    #### 登陆系统 ####
    lg = bs.login()
    # 显示登陆返回信息
    if lg.error_code != '0':
        LOGGER.info('login respond error_code:' + lg.error_code)
        LOGGER.info('login respond  error_msg:' + lg.error_msg)
    #### 获取沪深A股历史K线数据 ####
    # 详细指标参数，参见“历史行情指标参数”章节；“分钟线”参数与“日线”参数不同。“分钟线”不包含指数。
    rs = bs.query_history_k_data_plus(baostock_code,
                                      "date,code,open,high,low,close,volume,amount,adjustflag,turn,tradestatus,pctChg,isST",
                                      start_date=start_date, end_date=end_date,
                                      frequency="d", adjustflag=baostock_adjust)
    if rs.error_code != 0:
        LOGGER.info('query_history_k_data_plus respond error_code:' + rs.error_code)
        LOGGER.info('query_history_k_data_plus respond  error_msg:' + rs.error_msg)
    data_list = []
    while (rs.error_code == '0') & rs.next():
        # 获取一条记录，将记录合并在一起
        data_list.append(rs.get_row_data())
    df = pd.DataFrame(data_list, columns=rs.fields)
    df.rename(columns={
        'amount': 'turnover'
    }, inplace=True)
    # 去掉成交量为空的，这些数据可能是停牌导致的
    df = df[df['volume'] != '']
    df['code'] = code
    df = df[['code', 'date', 'open', 'high', 'low', 'close', 'volume', 'turnover']]
    df['date'] = pd.to_datetime(df['date'])
    df['open'] = pd.to_numeric(df['open'])
    df['high'] = pd.to_numeric(df['high'])
    df['low'] = pd.to_numeric(df['low'])
    df['close'] = pd.to_numeric(df['close'])
    df['volume'] = pd.to_numeric(df['volume'])
    #### 登出系统 ####
    bs.logout()
    return df