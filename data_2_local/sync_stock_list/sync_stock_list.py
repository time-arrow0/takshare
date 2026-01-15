import os
import sys
from datetime import datetime

sys.path.append(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))
import pandas as pd

from dao.dao import execute_by_sql, insert_batch_single_column
from data_2_local.common_data_obtain import obtain_stock_codes_a
from utils.log_utils import setup_logger_simple_msg
import akshare as ak

LOGGER = setup_logger_simple_msg(name='stock_list_2_local')


def data_2_local():
    date_str = datetime.now().strftime("%Y%m%d")
    # 定义列名。turnover: 成交额
    column_mapping = {
        '序号': 'serial_no',
        '代码': 'code',
        '名称': 'name',
        '最新价': 'latest_price',
        '涨跌幅': 'change_pct',
        '涨跌额': 'change_amount',
        '成交量': 'volume',
        '成交额': 'turnover',
        '振幅': 'amplitude',
        '最高': 'high',
        '最低': 'low',
        '今开': 'open',
        '昨收': 'pre_close',
        '量比': 'volume_ratio',
        '换手率': 'turnover_rate',
        '市盈率-动态': 'pe_ratio',
        '市净率': 'pb_ratio',
        '总市值': 'total_market_cap',
        '流通市值': 'circulating_market_cap',
        '涨速': 'change_speed',
        '5分钟涨跌': 'change_5min',
        '60日涨跌幅': 'change_60d',
        '年初至今涨跌幅': 'change_ytd'
    }
    # column_names = ['serial_no', 'code', 'name', 'latest_price', 'change_pct', 'change_amount', 'volume', 'turnover',
    #                 'amplitude', 'high', 'low', 'open', 'pre_close', 'volume_ratio', 'turnover_rate', 'pe_ratio',
    #                 'pb_ratio', 'total_market_cap', 'circulating_market_cap', 'change_speed', 'change_5min',
    #                 'change_60d', 'change_ytd']
    # dtype_dict = {
    #     'code': str
    # }
    exist_stock_codes_a_set = set(obtain_stock_codes_a())
    df = ak.stock_zh_a_spot_em()
    df.rename(columns=column_mapping, inplace=True)
    print(df.shape[0])
    series = df['code']
    # stocks_sh_main_df = df[(df['code'].str.startswith('6'))]
    # print(type(series))
    sh_main_codes = []
    sh_kc_codes = []
    sz_main_codes = []
    sz_cy_codes = []
    prefix_set = set()
    for code in series:
        # 如果已经存在，跳过
        if code in exist_stock_codes_a_set:
            continue
        prefix = code[:2]
        prefix_set.add(prefix)
        if prefix in {'60'}:
            sh_main_codes.append(code)
        elif prefix in {'68'}:
            sh_kc_codes.append(code)
        elif prefix in {'00'}:
            sz_main_codes.append(code)
        elif prefix in {'30'}:
            sz_cy_codes.append(code)

    prefixes = list(prefix_set)
    prefixes.sort(key=lambda x: x)
    print(prefixes)

    l0 = len(sh_main_codes)
    if l0 != 0:
        sh_main_sql = get_insert_sql('stocks_sh_main', sh_main_codes)
        execute_by_sql(sh_main_sql)
        LOGGER.info(f"{date_str}, sh_main, 新增{l0}条")
    l1 = len(sh_kc_codes)
    if l1 != 0:
        sh_kc_sql = get_insert_sql('stocks_sh_kc', sh_kc_codes)
        execute_by_sql(sh_kc_sql)
        LOGGER.info(f"{date_str}, sh_kc, 新增{l1}条")
    l2 = len(sz_main_codes)
    if l2 != 0:
        sz_main_sql = get_insert_sql('stocks_sz_main', sz_main_codes)
        execute_by_sql(sz_main_sql)
        LOGGER.info(f"{date_str}, sz_main, 新增{l2}条")
    l3 = len(sz_cy_codes)
    if l3 != 0:
        sz_cy_sql = get_insert_sql('stocks_sz_cy', sz_cy_codes)
        execute_by_sql(sz_cy_sql)
        LOGGER.info(f"{date_str}, sz_cy, 新增{l3}条")
    if l0 == 0 and l1 == 0 and l2 == 0 and l3 == 0:
        LOGGER.info(f"{date_str}, 没有新增数据")

def deleted_2_local():
    """
    已退市的，不包括京市
    """
    directory = 'D:/new_tdx/T0002/export/bfq_delisted'
    items = os.listdir(directory)
    codes = []
    for item in items:
        code = item[9:15]
        if code[0] == '9' or code[0] == '2':
            continue
        codes.append(code)
    # print(len(codes))
    # print(codes)
    table_name = 'stocks_delisted'
    insert_batch_single_column(table_name=table_name, column='code', data=codes)


def get_insert_sql(table_name, codes):
    if len(codes) == 0:
        return None
    sql = f"""
        INSERT INTO {table_name} (code) VALUES 
    """
    values_str = "('" + "'),('".join(codes) + "');"
    sql = sql + values_str
    return sql


def obtain_big_quant_codes():
    with open(file='data/stock_list_2_local/big_quant_codes.txt', mode='r', encoding='utf-8') as f:
        content = f.read()
        codes0 = content.split('\n')
    codes = []
    for code in codes0:
        codes.append(code[:6])
    return codes


def compare_codes():
    big_quant_codes = obtain_big_quant_codes()
    big_quant_code_set = set(big_quant_codes)
    db_codes = obtain_stock_codes_a()
    db_code_set = set(db_codes)
    # db有但big_quant没有的
    print(db_code_set - big_quant_code_set)
    # big_quant有但db没有的
    print(big_quant_code_set - db_code_set)


if __name__ == '__main__':
    # data_2_local()
    # t_table_name = 'tt'
    # tcodes = ['001', '002']
    # tsql = get_insert_sql(t_table_name, tcodes)
    # print(tsql)

    # compare_codes()
    # deleted_2_local()
    data_2_local()
