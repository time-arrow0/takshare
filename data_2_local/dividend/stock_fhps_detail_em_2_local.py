import os
import sys
import time

import pandas as pd

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
import akshare as ak
from data_2_local.common_data_2_local import df_append_2_local, obtain_code_max_date_timestamps, \
    obtain_middle_small_delisted_stock_code_set
from utils.log_utils import setup_logger_simple_msg

TABLE_NAME = 'stock_fhps_detail_em'
CODE_COLUMN = 'code'
DATE_COLUMN = 'ex_dividend_date'
LOGGER = setup_logger_simple_msg(name='stock_fhps_detail_em_2_local')

def get_baostock_code(code):
    if code.startswith('6'):
        baostock_code = f"sh.{code}"
    elif code.startswith('0'):
        baostock_code = f"sz.{code}"
    else:
        baostock_code = code
    return baostock_code

def remove_empty(str_list):
    result = []
    for s in str_list:
        if s.strip():
            result.append(s)
    return result



def initial_stock_fhps_detail_em_2_local():
    """
    初始运行一次
    """
    with open(file='data/stock_fhps_detail_em_2_local/399101_codes.txt', mode='r', encoding='utf-8') as f:
        content = f.read()
        t_stock_codes = remove_empty(content.split('\n'))
        print(len(t_stock_codes))
    with open(file='data/stock_fhps_detail_em_2_local/complete_codes.txt', mode='r', encoding='utf-8') as f:
        content = f.read()
        t_complete_codes = remove_empty(content.split('\n'))
        print(len(t_complete_codes))
    t_complete_code_set = set(t_complete_codes)
    # 已退市中小板股票
    delisted_stock_code_set = obtain_middle_small_delisted_stock_code_set()
    # 去掉已经同步过的，退市的
    codes = []
    for code in t_stock_codes:
        if code in t_complete_code_set:
            continue
        if code in delisted_stock_code_set:
            continue
        codes.append(code)
    # codes = ['002003']
    print(f'len_t_codes: {len(codes)}')

    # 获取到的数据，列为code, date
    # df = obtain_code_max_date_timestamps(table_name=TABLE_NAME, code_column=CODE_COLUMN, date_column=DATE_COLUMN)
    # dict: 代码----上个除权除息日期(表中该代码数据最大日期)
    # code_max_date_ts_dict = df.set_index('code')['date'].to_dict()

    column_mapping = {
        '报告期': 'report_period',
        '业绩披露日期': 'performance_disclosure_date',
        '送转股份-送转总比例': 'total_bonus_ratio',
        '送转股份-送股比例': 'bonus_ratio',
        '送转股份-转股比例': 'conversion_ratio',
        '现金分红-现金分红比例': 'cash_dividend_ratio',
        '现金分红-现金分红比例描述': 'cash_dividend_description',
        '现金分红-股息率': 'dividend_yield',
        '每股收益': 'eps',
        '每股净资产': 'navps',
        '每股公积金': 'surplus_reserve_per_share',
        '每股未分配利润': 'retained_earnings_per_share',
        '净利润同比增长': 'net_profit_yoy_growth',
        '总股本': 'total_shares',
        '预案公告日': 'plan_announcement_date',
        '股权登记日': 'equity_record_date',
        '除权除息日': 'ex_dividend_date',
        '方案进度': 'plan_progress',
        '最新公告日期': 'latest_announcement_date'
    }
    dtype_dict = {
        'total_bonus_ratio': 'float64',
        'bonus_ratio': 'float64',
        'conversion_ratio': 'float64',
        'cash_dividend_ratio': 'float64',
        'dividend_yield': 'float64',
        'eps': 'float64',
        'navps': 'float64',
        'surplus_reserve_per_share': 'float64',
        'retained_earnings_per_share': 'float64',
        'net_profit_yoy_growth': 'float64',
        'total_shares': 'int64'
    }
    # codes = ['002001']
    for code in codes:
        try:
            df = ak.stock_fhps_detail_em(symbol=code)
            df = df.rename(columns=column_mapping)
            df['code'] = code
            df['total_shares'] = df['total_shares'].fillna(0)
            df['report_period'] = pd.to_datetime(df['report_period'])
            df['performance_disclosure_date'] = pd.to_datetime(df['performance_disclosure_date'])
            df['plan_announcement_date'] = pd.to_datetime(df['plan_announcement_date'])
            df['equity_record_date'] = pd.to_datetime(df['equity_record_date'])
            df['ex_dividend_date'] = pd.to_datetime(df['ex_dividend_date'])
            df['latest_announcement_date'] = pd.to_datetime(df['latest_announcement_date'])
            df = df.astype(dtype_dict)
            df_append_2_local(table_name=TABLE_NAME, df=df)
            with open(file='data/stock_fhps_detail_em_2_local/complete_codes.txt', mode='a', encoding='utf-8') as f:
                f.write(f'{code}\n')
        except Exception as e:
            with open(file='data/stock_fhps_detail_em_2_local/failed_codes.txt', mode='a', encoding='utf-8') as f:
                f.write(f'{code}\n')
            print(e)
        time.sleep(0.5)
    
def daily_adjust_factor_2_local():
    """
    每日运行一次
    """
    pass


if __name__ == '__main__':
    pd.set_option('display.max_rows', None)  # 设置行数为无限制
    pd.set_option('display.max_columns', None)  # 设置列数为无限制
    pd.set_option('display.width', 1000)  # 设置列宽
    pd.set_option('display.colheader_justify', 'left')  # 设置列标题靠左

    initial_stock_fhps_detail_em_2_local()
