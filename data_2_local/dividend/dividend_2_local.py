import os
import sys
import time
from typing import re

import pandas as pd

from dao.dao import obtain_df_by_sql

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
import akshare as ak
from data_2_local.common_data_2_local import df_append_2_local, obtain_code_max_date_timestamps, \
    obtain_middle_small_delisted_stock_code_set
from utils.log_utils import setup_logger_simple_msg

TABLE_NAME = 'dividend'
CODE_COLUMN = 'code'
DATE_COLUMN = 'ex_dividend_date'
# TABLE_NAME_EM = 'stock_fhps_detail_em'
# TABLE_NAME_THS = 'stock_fhps_detail_ths'
LOGGER = setup_logger_simple_msg(name='dividend_2_local')


def remove_empty(str_list):
    result = []
    for s in str_list:
        if s.strip():
            result.append(s)
    return result


def extract_dividend_info(description):
    """
    从分红方案描述中提取送股数、转股数和现金分红
    参数:
    description: 字符串，分红方案描述
    返回:
    tuple: (送股数, 转股数, 现金分红)
    """
    # 如果不分红不转增
    if pd.isna(description) or description == "不分红不转增":
        return 0.0, 0.0, 0.0

    # 初始化变量
    share_giving = 0.0
    share_conversion = 0.0
    cash_dividend = 0.0

    # 统一处理"股"字前面的空格
    desc = description.replace("股 ", "股").replace(" 股", "股")

    # 提取现金分红
    cash_pattern = r'派([\d.]+)元'
    cash_match = re.search(cash_pattern, desc)
    if cash_match:
        cash_dividend = float(cash_match.group(1))

    # 提取送股数
    giving_pattern = r'送([\d.]+)股'
    giving_match = re.search(giving_pattern, desc)
    if giving_match:
        share_giving = float(giving_match.group(1))

    # 提取转股数 (包括"转"和"转增")
    # "(?: )"代表非捕获分组，匹配但不捕获到分组中
    conversion_pattern = r'转(?:增)?([\d.]+)股'
    conversion_match = re.search(conversion_pattern, desc)
    if conversion_match:
        share_conversion = float(conversion_match.group(1))

    return share_giving, share_conversion, cash_dividend

def initial_dividend_2_local():
    """
    初始运行一次
    """
    with open(file='data/dividend_2_local/399101_codes.txt', mode='r', encoding='utf-8') as f:
        content = f.read()
        stock_codes = remove_empty(content.split('\n'))
        print(len(stock_codes))
    with open(file='data/dividend_2_local/complete_codes.txt', mode='r', encoding='utf-8') as f:
        content = f.read()
        complete_codes = remove_empty(content.split('\n'))
        print(len(complete_codes))
    complete_code_set = set(complete_codes)
    # 已退市中小板股票
    delisted_stock_code_set = obtain_middle_small_delisted_stock_code_set()
    # 去掉已经同步过的，退市的
    codes = []
    for code in stock_codes:
        if code in complete_code_set:
            continue
        if code in delisted_stock_code_set:
            sql = f"""
                SELECT code,
	                equity_record_date,
	                ex_dividend_date,
	                bonus_ratio AS share_giving_count,
	                conversion_ratio AS share_conversion_count,
	                cash_dividend_ratio AS cash_dividend
                FROM stock_fhps_detail_em
                WHERE equity_record_date IS NOT NULL
            """
            df = obtain_df_by_sql(sql)
            if df.shape[0] == 0:
                continue
        else:
            sql = f"""
                SELECT code,
                    equity_record_date,
                    ex_dividend_date,
                    dividend_plan_description
                FROM stock_fhps_detail_ths
                WHERE equity_record_date IS NOT NULL
            """
            df = obtain_df_by_sql(sql)
            # 应用提取函数
            results = df['dividend_plan_description'].apply(extract_dividend_info)
            share_giving_counts = []
            share_conversion_counts = []
            cash_dividends = []
            for r in results:
                share_giving_counts.append(r[0])
                share_conversion_counts.append(r[1])
                cash_dividends.append(r[2])
            # 创建新列
            df['share_giving_count'] = share_giving_counts
            df['share_conversion_count'] = share_conversion_counts
            df['cash_dividend'] = cash_dividends
            if df.shape[0] == 0:
                continue
        df_append_2_local(table_name=TABLE_NAME, df=df)
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
            with open(file='data/dividend_2_local/complete_codes.txt', mode='a', encoding='utf-8') as f:
                f.write(f'{code}\n')
        except Exception as e:
            with open(file='data/dividend_2_local/failed_codes.txt', mode='a', encoding='utf-8') as f:
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

    initial_dividend_2_local()