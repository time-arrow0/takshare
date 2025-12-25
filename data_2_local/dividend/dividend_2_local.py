import os
import re
import sys
import time
from datetime import datetime, date

import pandas as pd

from dao.dao import obtain_df_by_sql, obtain_list_by_sql

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


def sync_dividend_all():
    """
    同步所有数据
    """
    # with open(file='data/dividend_2_local/399101_codes.txt', mode='r', encoding='utf-8') as f:
    #     content = f.read()
    #     stock_codes = remove_empty(content.split('\n'))
    #     print(len(stock_codes))
    # with open(file='data/dividend_2_local/complete_codes.txt', mode='r', encoding='utf-8') as f:
    #     content = f.read()
    #     complete_codes = remove_empty(content.split('\n'))
    #     print(len(complete_codes))
    # complete_code_set = set(complete_codes)
    # 已退市中小板股票
    # delisted_stock_code_set = obtain_middle_small_delisted_stock_code_set()
    # sql = f"""
    #     SELECT code, dividend.report_period FROM dividend
    # """
    # existed_list = obtain_list_by_sql(sql)
    # existed_code_date_set = set()
    # for row in existed_list:
    #     code = row[0]
    #     report_date_str = row[1].strftime("%Y%m%d")
    #     existed_code_date_set.add(f'{code}-{report_date_str}')

    sql = f"""
        SELECT code,
            report_period,
    	    equity_record_date,
    	    ex_dividend_date,
    	    bonus_ratio AS share_giving_count,
    	    conversion_ratio AS share_conversion_count,
    	    cash_dividend_ratio AS cash_dividend
        FROM stock_fhps_detail_em
        WHERE ex_dividend_date IS NOT NULL
            AND (code, ex_dividend_date) NOT IN (SELECT code, ex_dividend_date FROM dividend)
        """
    df0 = obtain_df_by_sql(sql)

    sql = f"""
        SELECT code,
            report_period,
            equity_record_date,
            ex_dividend_date,
            dividend_plan_description
        FROM stock_fhps_detail_ths
        WHERE ex_dividend_date IS NOT NULL AND (code, CASE
                         WHEN SUBSTR(report_period, 5, 1) = '年' THEN DATE(CONCAT(LEFT(report_period, 4), '1231'))
                         WHEN SUBSTR(report_period, 5, 1) = '中' THEN DATE(CONCAT(LEFT(report_period, 4), '0630'))
                         ELSE NULL
            END) NOT IN (SELECT code, report_period FROM stock_fhps_detail_em WHERE ex_dividend_date IS NOT NULL)
        """
    df1 = obtain_df_by_sql(sql)
    # 应用提取函数
    results = df1['dividend_plan_description'].apply(extract_dividend_info)
    share_giving_counts = []
    share_conversion_counts = []
    cash_dividends = []
    for r in results:
        share_giving_counts.append(r[0])
        share_conversion_counts.append(r[1])
        cash_dividends.append(r[2])
    # 创建新列
    df1['share_giving_count'] = share_giving_counts
    df1['share_conversion_count'] = share_conversion_counts
    df1['cash_dividend'] = cash_dividends
    df1.drop('dividend_plan_description', axis=1, inplace=True)
    # 修改df1['report_period']，df1['report_period']不是日期，是"xxxx年报", "xxxx中报"这种形式
    mask_annual = df1['report_period'].str[4] == '年'
    df1.loc[mask_annual, 'date_str'] = df1.loc[mask_annual, 'report_period'].str[:4] + '-12-31'
    # 处理中报 (xxxx中报 -> xxxx-06-30)
    # 通过检查第5个字符是否为"中"来判断
    mask_interim = df1['report_period'].str[4] == '中'
    df1.loc[mask_interim, 'date_str'] = df1.loc[mask_interim, 'report_period'].str[:4] + '-06-30'
    # 转换为datetime类型
    df1['report_period'] = pd.to_datetime(df1['date_str'], errors='coerce')
    df1.drop(labels=['date_str'], axis=1, inplace=True)
    df = pd.concat(objs=[df0, df1]).drop_duplicates(subset=['code', 'report_period'], keep='first')
    if df.shape[0] == 0:
        LOGGER.info('未获取到em, ths表中数据')
        return
    # 不要了，直接在上面sql查询时就过滤掉了
    # # 删掉已经在数据库中的
    # df['union_key'] = df['code'] + '-' + pd.to_datetime(df['report_period']).dt.strftime('%Y%m%d')
    # df = df[~df['union_key'].isin(existed_code_date_set)]
    if df.shape[0] == 0:
        LOGGER.info('没有新数据')
        return
    df_append_2_local(table_name=TABLE_NAME, df=df)


def sync_dividend():
    """
    只同步报告期为去年12月31日和今年6月30日的数据
    在东财数据同步任务运行完成后运行，目前和东财数据同步任务运行频率一样，每周运行一次
    """
    # 要考虑去年12月31日报告和今年6月30日报告
    current_time = datetime.now()
    year = current_time.year
    last_year = date(year - 1, 12, 31)
    mid_year = date(year, 6, 30)
    last_year_str = last_year.strftime('%Y%m%d')
    mid_year_str = mid_year.strftime('%Y%m%d')
    # 查询表中每个code的去年12月31日报告和今年6月30日报告数据
    sql = f"""
            SELECT code,
                report_period,
                equity_record_date,
                ex_dividend_date,
                bonus_ratio AS share_giving_count,
                conversion_ratio AS share_conversion_count,
                cash_dividend_ratio AS cash_dividend
            FROM stock_fhps_detail_em
            WHERE report_period IN (date('{last_year_str}'), date('{mid_year_str}'))
                AND ex_dividend_date IS NOT NULL
                AND (code, report_period) NOT IN
                (
                    SELECT code, report_period
                    FROM dividend
                    WHERE report_period IN (date('{last_year_str}'), date('{mid_year_str}'))
                )
        """
    df = obtain_df_by_sql(sql)
    if df.shape[0] == 0:
        LOGGER.info(f'没有要新增的数据')
        return
    # 插入表中
    table_column_set = {'code', 'report_period', 'performance_disclosure_date', 'total_bonus_ratio', 'bonus_ratio',
                        'conversion_ratio', 'cash_dividend_ratio', 'cash_dividend_description', 'dividend_yield',
                        'eps', 'navps', 'surplus_reserve_per_share', 'retained_earnings_per_share',
                        'net_profit_yoy_growth', 'total_shares', 'plan_announcement_date', 'equity_record_date',
                        'ex_dividend_date', 'plan_progress', 'latest_announcement_date'}
    df_append_2_local(TABLE_NAME, df, table_column_set)
    LOGGER.info(f'新增数量: {df.shape[0]}')



if __name__ == '__main__':
    pd.set_option('display.max_rows', None)  # 设置行数为无限制
    pd.set_option('display.max_columns', None)  # 设置列数为无限制
    pd.set_option('display.width', 1000)  # 设置列宽
    pd.set_option('display.colheader_justify', 'left')  # 设置列标题靠左

    # sync_dividend_all()
    sync_dividend()
