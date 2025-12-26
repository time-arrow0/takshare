import os
import sys
import time
from datetime import datetime, date

import pandas as pd

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from dao.dao import obtain_df_by_sql, obtain_list_by_sql
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


def obtain_sh_sz_stock_codes():
    sql = """
        SELECT CODE FROM stocks_sh_main
        UNION
        SELECT CODE FROM stocks_sh_kc
        UNION
        SELECT CODE FROM stocks_sz_main
        UNION
        SELECT CODE FROM stocks_sz_cy
        """
    seq = obtain_list_by_sql(sql)
    codes = []
    for row in seq:
        codes.append(row[0])
    return codes

def obtain_may_delisted_stock_code_set():
    with open(file='data/stock_fhps_detail_em_2_local/em_not_contains_codes.txt', mode='r', encoding='utf-8') as f:
        content = f.read()
        codes = remove_empty(content.split('\n'))
        return set(codes)

def initial_stock_fhps_detail_em_2_local():
    """
    初始运行一次
    """
    stock_codes = obtain_sh_sz_stock_codes()
    with open(file='data/stock_fhps_detail_em_2_local/complete_codes.txt', mode='r', encoding='utf-8') as f:
        content = f.read()
        complete_codes = remove_empty(content.split('\n'))
        print(len(complete_codes))
    complete_code_set = set(complete_codes)
    # 已退市中小板股票
    delisted_stock_code_set = obtain_middle_small_delisted_stock_code_set()
    # 其他可能退市股票
    delisted_stock_code_set2 = obtain_may_delisted_stock_code_set()
    # 去掉已经同步过的，退市的
    codes = []
    for code in stock_codes:
        if code in complete_code_set:
            continue
        if code in delisted_stock_code_set:
            continue
        if code in delisted_stock_code_set2:
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
            # 查出已存在的
            sql = f"""
            SELECT code, ex_dividend_date
            FROM stock_fhps_detail_em
            WHERE code = '{code}'
            """
            existed_code_dates = obtain_list_by_sql(sql)
            existed_code_date_set = set()
            for row in existed_code_dates:
                existed_code_date_set.add(row[1])
            df = ak.stock_fhps_detail_em(symbol=code)
            df = df.rename(columns=column_mapping)
            df['code'] = code
            df['total_shares'] = df['total_shares'].fillna(0)
            df['report_period'] = pd.to_datetime(df['report_period'])
            df['performance_disclosure_date'] = pd.to_datetime(df['performance_disclosure_date'])
            df['plan_announcement_date'] = pd.to_datetime(df['plan_announcement_date'])
            df['equity_record_date'] = pd.to_datetime(df['equity_record_date'])
            df['ex_dividend_date'] = pd.to_datetime(df['ex_dividend_date'])
            df = df[~df['ex_dividend_date'].isin(existed_code_date_set)]
            if df.shape[0] == 0:
                with open(file='data/stock_fhps_detail_em_2_local/complete_codes.txt', mode='a', encoding='utf-8') as f:
                    f.write(f'{code}\n')
                LOGGER.info(f'{code}没有新数据, 跳过')
                continue
            df['latest_announcement_date'] = pd.to_datetime(df['latest_announcement_date'])
            df = df.astype(dtype_dict)
            df_append_2_local(table_name=TABLE_NAME, df=df)
            with open(file='data/stock_fhps_detail_em_2_local/complete_codes.txt', mode='a', encoding='utf-8') as f:
                f.write(f'{code}\n')
        except Exception as e:
            with open(file='data/stock_fhps_detail_em_2_local/failed_codes.txt', mode='a', encoding='utf-8') as f:
                f.write(f'{code}\n')
            # raise
            print(f'{code}, {e}')
        time.sleep(0.5)
    
def weekly_adjust_factor_2_local():
    """
    每周运行一次更新，分红配送-东财
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
        SELECT code, report_period
        FROM stock_fhps_detail_em
        WHERE report_period in (date('{last_year_str}'), date('{mid_year_str}'))
    """
    existed_df = obtain_df_by_sql(sql)
    existed_last_year_codes = set(existed_df.loc[existed_df['report_period'] == last_year, 'code'])
    existed_mid_year_codes = set(existed_df.loc[existed_df['report_period'] == mid_year, 'code'])
    column_mapping = {
        '代码': 'code',
        '名称': 'name',
        '送转股份-送转总比例': 'total_bonus_ratio',
        '送转股份-送转比例': 'bonus_ratio',
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

    df_list = []
    # 年报数据
    try:
        df_year = ak.stock_fhps_em(date=last_year_str)
        if df_year.shape[0] != 0:
            df_year.rename(columns=column_mapping, inplace=True)
            # 筛选不在表中的
            df_year = df_year[~df_year['code'].isin(existed_last_year_codes)]
            if df_year.shape[0] != 0:
                df_year['report_period'] = last_year
                df_year['report_period'] = pd.to_datetime(df_year['report_period'])
                df_year['plan_announcement_date'] = pd.to_datetime(df_year['plan_announcement_date'])
                df_year['equity_record_date'] = pd.to_datetime(df_year['equity_record_date'])
                df_year['ex_dividend_date'] = pd.to_datetime(df_year['ex_dividend_date'])
                df_year['latest_announcement_date'] = pd.to_datetime(df_year['latest_announcement_date'])
                df_year['total_shares'] = df_year['total_shares'].fillna(0)
                df_year = df_year.astype(dtype_dict)
                df_list.append(df_year)
                LOGGER.info(f'{last_year_str}数量: {df_year.shape[0]}')
    except Exception as e:
        LOGGER.error(f"获取年报数据失败: {e}")
        raise
    # 2. 如果是下半年（7月后），还要查今年的中报
    # 中报数据（7月1日之后才需要）
    if current_time.month >= 7:
        try:
            df_mid = ak.stock_fhps_em(date=mid_year_str)
            if df_mid.shape[0] != 0:
                df_mid.rename(columns=column_mapping, inplace=True)
                # 筛选不在表中的
                df_mid = df_mid[~df_mid['code'].isin(existed_mid_year_codes)]
                if df_mid.shape[0] != 0:
                    df_mid['report_period'] = mid_year
                    df_mid['report_period'] = pd.to_datetime(df_mid['report_period'])
                    df_mid['plan_announcement_date'] = pd.to_datetime(df_mid['plan_announcement_date'])
                    df_mid['equity_record_date'] = pd.to_datetime(df_mid['equity_record_date'])
                    df_mid['ex_dividend_date'] = pd.to_datetime(df_mid['ex_dividend_date'])
                    df_mid['latest_announcement_date'] = pd.to_datetime(df_mid['latest_announcement_date'])
                    df_mid['total_shares'] = df_mid['total_shares'].fillna(0)
                    df_mid = df_mid.astype(dtype_dict)
                    df_list.append(df_mid)
                    LOGGER.info(f'{mid_year_str}数量: {df_mid.shape[0]}')
        except Exception as e:
            LOGGER.error(f"获取中报数据失败: {e}")
            raise
    # 合并数据
    if df_list:
        df = pd.concat(df_list, ignore_index=True)
        # 去重处理
        df = df.drop_duplicates(subset=['code', 'ex_dividend_date'], keep='last')
        # 插入表中
        table_column_set = {'code', 'report_period', 'performance_disclosure_date', 'total_bonus_ratio', 'bonus_ratio', 'conversion_ratio', 'cash_dividend_ratio', 'cash_dividend_description', 'dividend_yield', 'eps', 'navps', 'surplus_reserve_per_share', 'retained_earnings_per_share', 'net_profit_yoy_growth', 'total_shares', 'plan_announcement_date', 'equity_record_date', 'ex_dividend_date', 'plan_progress', 'latest_announcement_date'}
        df_append_2_local(TABLE_NAME, df, table_column_set)
        LOGGER.info(f'新增数量: {df.shape[0]}')
    else:
        LOGGER.info(f'没有要新增的数据')



if __name__ == '__main__':
    pd.set_option('display.max_rows', None)  # 设置行数为无限制
    pd.set_option('display.max_columns', None)  # 设置列数为无限制
    pd.set_option('display.width', 1000)  # 设置列宽
    pd.set_option('display.colheader_justify', 'left')  # 设置列标题靠左

    # initial_stock_fhps_detail_em_2_local()
    weekly_adjust_factor_2_local()