import os
import sys
import time

import pandas as pd

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
import akshare as ak
from data_2_local.common_data_2_local import df_append_2_local, obtain_code_max_date_timestamps, \
    obtain_middle_small_delisted_stock_code_set, obtain_middle_small_delisted_stock_code_list
from utils.log_utils import setup_logger_simple_msg

TABLE_NAME = 'stock_fhps_detail_ths'
CODE_COLUMN = 'code'
DATE_COLUMN = 'ex_dividend_date'
LOGGER = setup_logger_simple_msg(name='stock_fhps_detail_ths_2_local')

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



def initial_stock_fhps_detail_ths_2_local():
    """
    初始运行一次
    """
    # with open(file='data/stock_fhps_detail_ths_2_local/399101_codes.txt', mode='r', encoding='utf-8') as f:
    #     content = f.read()
    #     stock_codes = remove_empty(content.split('\n'))
    #     print(len(stock_codes))
    # 已退市中小板股票
    stock_codes = obtain_middle_small_delisted_stock_code_list()
    with open(file='data/stock_fhps_detail_ths_2_local/complete_codes.txt', mode='r', encoding='utf-8') as f:
        content = f.read()
        complete_codes = remove_empty(content.split('\n'))
        print(len(complete_codes))
    complete_code_set = set(complete_codes)
    # 去掉已经同步过的，退市的
    codes = []
    for code in stock_codes:
        if code in complete_code_set:
            continue
        codes.append(code)
    print(f'len_codes: {len(codes)}')

    # 获取到的数据，列为code, date
    # df = obtain_code_max_date_timestamps(table_name=TABLE_NAME, code_column=CODE_COLUMN, date_column=DATE_COLUMN)
    # dict: 代码----上个除权除息日期(表中该代码数据最大日期)
    # code_max_date_ts_dict = df.set_index('code')['date'].to_dict()

    column_mapping = {
        '报告期': 'report_period',
        '董事会日期': 'board_meeting_date',
        '股东大会预案公告日期': 'shareholders_meeting_announcement_date',
        '实施公告日': 'implementation_announcement_date',
        '分红方案说明': 'dividend_plan_description',
        'A股股权登记日': 'equity_record_date',
        'A股除权除息日': 'ex_dividend_date',
        '分红总额': 'total_dividend',
        '方案进度': 'plan_progress',
        '股利支付率': 'dividend_payout_ratio',
        '税前分红率': 'pretax_dividend_yield'
    }
    # codes = ['002002']
    for code in codes:
        try:
            df = ak.stock_fhps_detail_ths(symbol=code)
            df = df.rename(columns=column_mapping)
            df['code'] = code
            df['board_meeting_date'] = pd.to_datetime(df['board_meeting_date'])
            df['shareholders_meeting_announcement_date'] = pd.to_datetime(df['shareholders_meeting_announcement_date'])
            df['implementation_announcement_date'] = pd.to_datetime(df['implementation_announcement_date'])
            df['equity_record_date'] = pd.to_datetime(df['equity_record_date'])
            df['ex_dividend_date'] = pd.to_datetime(df['ex_dividend_date'])
            df_append_2_local(table_name=TABLE_NAME, df=df)
            with open(file='data/stock_fhps_detail_ths_2_local/complete_codes.txt', mode='a', encoding='utf-8') as f:
                f.write(f'{code}\n')
        except Exception as e:
            with open(file='data/stock_fhps_detail_ths_2_local/failed_codes.txt', mode='a', encoding='utf-8') as f:
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

    initial_stock_fhps_detail_ths_2_local()
