import os
import sys
sys.path.append(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))

from utils.web_interface_obtain_data import baostock_obtain_daily
from data_2_local.sync_stock_name_change.sync_stock_name_change import sync_name_change
import time
from datetime import datetime

import pandas as pd

from dao.dao import obtain_list_by_sql, df_append_2_local
import akshare as ak
from utils.holiday_utils import ChinaHolidayChecker
from utils.log_utils import setup_logger_simple_msg

LOGGER = setup_logger_simple_msg(name='sync_bfq_daily_stock_price')

TABLE_SH_MAIN = 'bfq_daily_stock_price_sh_main'
TABLE_SH_KC = 'bfq_daily_stock_price_sh_kc'
TABLE_SZ_MAIN = 'bfq_daily_stock_price_sz_main'
TABLE_SZ_CY = 'bfq_daily_stock_price_sz_cy'

def obtain_complete_code_set(file_path='data/complete_codes.txt'):
    with open(file=file_path, mode='r', encoding='utf-8') as f:
        content = f.read()
    codes = content.split('\n')
    return set(codes)

def append_2_complete_codes_file(code, file_path='data/complete_codes.txt'):
    with open(file=file_path, mode='a', encoding='utf-8') as f:
        f.write(f'{code}\n')

def obtain_existed_code_date_set():
    sql = """
        SELECT code, DATE_FORMAT(date, '%Y%m%d') AS date FROM bfq_daily_stock_price_sh_main
        UNION
        SELECT code, DATE_FORMAT(date, '%Y%m%d') AS date FROM bfq_daily_stock_price_sh_kc
        UNION
        SELECT code, DATE_FORMAT(date, '%Y%m%d') AS date FROM bfq_daily_stock_price_sz_main
        UNION
        SELECT code, DATE_FORMAT(date, '%Y%m%d') AS date FROM bfq_daily_stock_price_sz_cy
    """
    existed_list = obtain_list_by_sql(sql)
    existed_code_date_set = set()
    for row in existed_list:
        code = row[0]
        date_str = row[1]
        existed_code_date_set.add(f'{code}-{date_str}')
    return existed_code_date_set

def obtain_existed_delisted_code_date_set():
    sql = """
        SELECT code, DATE_FORMAT(date, '%Y%m%d') AS date FROM bfq_daily_stock_price_sh_main WHERE code in (SELECT code FROM stocks_delisted)
        UNION
        SELECT code, DATE_FORMAT(date, '%Y%m%d') AS date FROM bfq_daily_stock_price_sh_kc WHERE code in (SELECT code FROM stocks_delisted)
        UNION
        SELECT code, DATE_FORMAT(date, '%Y%m%d') AS date FROM bfq_daily_stock_price_sz_main WHERE code in (SELECT code FROM stocks_delisted)
        UNION
        SELECT code, DATE_FORMAT(date, '%Y%m%d') AS date FROM bfq_daily_stock_price_sz_cy WHERE code in (SELECT code FROM stocks_delisted)
    """
    existed_list = obtain_list_by_sql(sql)
    existed_code_date_set = set()
    for row in existed_list:
        code = row[0]
        date_str = row[1]
        existed_code_date_set.add(f'{code}-{date_str}')
    return existed_code_date_set

def tdx_file_data_2_local(dir0):
    """
    如果要批量增量更新，需要排除掉已有数据，要修改代码。obtain_existed_code_date_set()数据量过大，不再使用
    """
    # existed_code_date_set = obtain_existed_code_date_set()
    # 逐个读取目标目录文件，查表中该代码最大日期，把大于最大日期的数据写入数据库
    items = os.listdir(dir0)
    dtype_dict = {
        'open': 'float64',
        'close': 'float64',
        'high': 'float64',
        'low': 'float64',
        # 'volume': 'int64',
    }
    skiprows = 2
    # turnover: 成交额
    column_names = ['date', 'open', 'high', 'low', 'close', 'volume', 'turnover']
    # 代码在文件名中的截取范围
    code_start = 7
    code_end = 13  # 不包含
    # 已完成的代码
    complete_codes = obtain_complete_code_set('data/tdx_complete_codes.txt')
    complete_code_set = set(complete_codes)
    sh_main_dfs = []
    sh_kc_dfs = []
    sz_main_dfs = []
    sz_cy_dfs = []
    for item in items:
        code = item[code_start: code_end]
        if code in complete_code_set:
            continue
        try:
            file_path = os.path.join(dir0, item)
            df = pd.read_csv(file_path,
                         sep='\s+',
                         skiprows=skiprows,
                         comment='#',  # 跳过以#开头的行
                         skip_blank_lines=True,  # 跳过空行
                         names=column_names,
                         dtype=dtype_dict,
                         encoding='GBK')
            df = df[['date', 'open', 'high', 'low', 'close', 'volume', 'turnover']]
            df['code'] = code
            df['date'] = pd.to_datetime(df['date'])
            # df['union_key'] = df['code'] + '-' + pd.to_datetime(df['date']).dt.strftime('%Y%m%d')
            # df = df[~df['union_key'].isin(existed_code_date_set)]
            # df.drop(labels=['union_key'], axis=1, inplace=True)
            if df.shape[0] == 0:
                continue
            prefix = code[:2]
            if prefix == '60':
                table_name = 'bfq_daily_stock_price_sh_main'
                sh_main_dfs.append(df)
            elif prefix == '68':
                table_name = 'bfq_daily_stock_price_sh_kc'
                sh_kc_dfs.append(df)
            elif prefix == '00':
                table_name = 'bfq_daily_stock_price_sz_main'
                sz_main_dfs.append(df)
            elif prefix == '30':
                table_name = 'bfq_daily_stock_price_sz_cy'
                sz_cy_dfs.append(df)
            else:
                LOGGER.info(f'{code}, 未找到符合前缀{prefix}的表')
                continue
            if df.shape[0] > 0:
                df_append_2_local(table_name=table_name, df=df)
            append_2_complete_codes_file(code=code, file_path='data/tdx_complete_codes.txt')
        except Exception as e:
            s = f'{code}保存出错'
            LOGGER.error(s)
            raise e

def tdx_file_single_date_data_2_local(dir0, date_str):
    """
    同步指定日期的数据
    """
    # existed_code_date_set = obtain_existed_code_date_set()

    date = datetime.strptime(date_str, '%Y%m%d')
    # 逐个读取目标目录文件，查表中该代码最大日期，把大于最大日期的数据写入数据库
    items = os.listdir(dir0)
    dtype_dict = {
        'open': 'float64',
        'close': 'float64',
        'high': 'float64',
        'low': 'float64',
        # 'volume': 'int64',
    }
    skiprows = 2
    # turnover: 成交额
    column_names = ['date', 'open', 'high', 'low', 'close', 'volume', 'turnover']
    # 代码在文件名中的截取范围
    code_start = 7
    code_end = 13  # 不包含
    # 已完成的代码
    complete_codes = obtain_complete_code_set('data/tdx_single_date_complete_codes.txt')
    complete_code_set = set(complete_codes)
    sh_main_dfs = []
    sh_kc_dfs = []
    sz_main_dfs = []
    sz_cy_dfs = []
    ts = pd.Timestamp(year=date.year, month=date.month, day=date.day)
    for item in items:
        code = item[code_start: code_end]
        if code in complete_code_set:
            continue
        try:
            file_path = os.path.join(dir0, item)
            df = pd.read_csv(file_path,
                         sep='\s+',
                         skiprows=skiprows,
                         comment='#',  # 跳过以#开头的行
                         skip_blank_lines=True,  # 跳过空行
                         names=column_names,
                         dtype=dtype_dict,
                         encoding='GBK')
            df = df[['date', 'open', 'high', 'low', 'close', 'volume', 'turnover']]
            df['code'] = code
            df['date'] = pd.to_datetime(df['date'])
            df = df[df['date'] == ts]
            # df['union_key'] = df['code'] + '-' + pd.to_datetime(df['date']).dt.strftime('%Y%m%d')
            # df = df[~df['union_key'].isin(existed_code_date_set)]
            # df.drop(labels=['union_key'], axis=1, inplace=True)
            if df.shape[0] == 0:
                continue
            prefix = code[:2]
            if prefix == '60':
                sh_main_dfs.append(df)
            elif prefix == '68':
                sh_kc_dfs.append(df)
            elif prefix == '00':
                sz_main_dfs.append(df)
            elif prefix == '30':
                sz_cy_dfs.append(df)
            else:
                LOGGER.info(f'{code}, 未找到符合前缀{prefix}的表')
                continue
            append_2_complete_codes_file(code=code, file_path='data/tdx_single_date_complete_codes.txt')
        except Exception as e:
            s = f'{code}保存出错'
            LOGGER.error(s)
            raise e
    if len(sh_main_dfs) > 0:
        table_name = 'bfq_daily_stock_price_sh_main'
        sh_main_df = pd.concat(sh_main_dfs, ignore_index=True)
        df_append_2_local(table_name=table_name, df=sh_main_df)
        LOGGER.info('sh_main同步完成')
    if len(sh_kc_dfs) > 0:
        table_name = 'bfq_daily_stock_price_sh_kc'
        sh_kc_df = pd.concat(sh_kc_dfs, ignore_index=True)
        df_append_2_local(table_name=table_name, df=sh_kc_df)
        LOGGER.info('sh_kc同步完成')
    if len(sz_main_dfs) > 0:
        table_name = 'bfq_daily_stock_price_sz_main'
        sz_main_df = pd.concat(sz_main_dfs, ignore_index=True)
        df_append_2_local(table_name=table_name, df=sz_main_df)
        LOGGER.info('sz_main同步完成')
    if len(sz_cy_dfs) > 0:
        table_name = 'bfq_daily_stock_price_sz_cy'
        sz_cy_df = pd.concat(sz_cy_dfs, ignore_index=True)
        df_append_2_local(table_name=table_name, df=sz_cy_df)
        LOGGER.info('sz_cy同步完成')

def sync_delisted():
    """
        如果要批量增量更新，需要排除掉已有数据，要修改代码。obtain_existed_code_date_set()数据量过大，不再使用
    """
    # t0 = time.time()
    # existed_code_date_set = obtain_existed_code_date_set()
    # t1 = time.time()
    # LOGGER.info(f'obtain_existed_code_date_set耗时{t1 - t0:.2f}秒')
    sql = """
        SELECT code FROM stocks_delisted
    """
    # 本次同步已完成的
    complete_code_set = obtain_complete_code_set()
    delisted_code_list = obtain_list_by_sql(sql)
    delisted_codes = []
    for row in delisted_code_list:
        code = row[0]
        if code in complete_code_set:
            continue
        delisted_codes.append(row[0])
    delisted_codes = ['000508', '600200', '603402', '600849', '688809', '001396', '001369', '301687']
    LOGGER.info(f'len delisted_codes: {len(delisted_codes)}')
    current_date_str = datetime.now().strftime('%Y-%m-%d')
    for code in delisted_codes:
        # df = akshare_em_obtain_daily(code=code)
        df = baostock_obtain_daily(code=code, start_date='1990-12-26', end_date=current_date_str)
        if df.shape[0] == 0:
            LOGGER.info(f'{code}未获取到数据')
            continue
        # df['union_key'] = df['code'] + '-' + pd.to_datetime(df['date']).dt.strftime('%Y%m%d')
        # df = df[~df['union_key'].isin(existed_code_date_set)]
        # df.drop(labels=['union_key'], axis=1, inplace=True)
        prefix = code[:2]
        if prefix == '60':
            table_name = 'bfq_daily_stock_price_sh_main'
        elif prefix == '68':
            table_name = 'bfq_daily_stock_price_sh_kc'
        elif prefix == '00':
            table_name = 'bfq_daily_stock_price_sz_main'
        elif prefix == '30':
            table_name = 'bfq_daily_stock_price_sz_cy'
        else:
            LOGGER.info(f'{code}, 未找到符合前缀{prefix}的表')
            continue
        success = df_append_2_local(table_name, df)
        append_2_complete_codes_file(code)
        time.sleep(0.5)



def web_interface_data_2_local():
    """
    每日收盘后，通过网络接口获取数据，写入数据库
    """
    current_date = datetime.now().date()
    date_str = current_date.strftime('%Y%m%d')
    # 如果当天不是工作日，不操作
    if not ChinaHolidayChecker.is_workday(current_date):
        LOGGER.info(f'{current_date}不是工作日, 不操作')
        return
    # 进一步检查，判断指数是否有数据，指数有数据才运行
    index_df = ak.stock_zh_index_daily_em(symbol="sh000001", start_date=date_str, end_date=date_str)
    if index_df.shape[0] == 0:
        LOGGER.info(f'{current_date}指数无数据, 不操作')
        return
    # 如果所有市场今日已有数据，不再运行
    # 查询4个市场数据最大日期
    sql = """
        SELECT 'sh_main' AS market, DATE_FORMAT(MAX(date), '%Y%m%d') AS max_date From bfq_daily_stock_price_sh_main
        UNION
        SELECT 'sh_kc' AS market, DATE_FORMAT(MAX(date), '%Y%m%d') AS max_date From bfq_daily_stock_price_sh_kc
        UNION
        SELECT 'sz_main' AS market, DATE_FORMAT(MAX(date), '%Y%m%d') AS max_date From bfq_daily_stock_price_sz_main
        UNION
        SELECT 'sz_cy' AS market, DATE_FORMAT(MAX(date), '%Y%m%d') AS max_date From bfq_daily_stock_price_sz_cy
    """
    max_date_rows = obtain_list_by_sql(sql)
    market_prefixes_dict = {
        'sh_main': {'60'},
        'sh_kc': {'68'},
        'sz_main': {'00'},
        'sz_cy': {'30'}
    }
    market_table_dict = {
        'sh_main': 'bfq_daily_stock_price_sh_main',
        'sh_kc': 'bfq_daily_stock_price_sh_kc',
        'sz_main': 'bfq_daily_stock_price_sz_main',
        'sz_cy': 'bfq_daily_stock_price_sz_cy'
    }
    # 需要同步的前缀
    sync_prefix_set = set()
    for row in max_date_rows:
        market = row[0]
        max_date_str = row[1]
        if max_date_str != date_str:
            prefixes = market_prefixes_dict[market]
            sync_prefix_set = sync_prefix_set.union(prefixes)
    # 如果需要同步的前缀为空，跳过。（后面如果有需求，可以先查出表中当日数据，再把接口获取的数据去除掉已存在的，再插入表中）
    if len(sync_prefix_set) == 0:
        LOGGER.info(f'{current_date}已有数据, 不操作')
        return
    # 定义中文到英文的列名映射
    column_mapping = {
        '序号': 'id',
        '代码': 'code',
        '名称': 'name',
        # '最新价': 'latest_price',
        '最新价': 'close',  # 最新价作为close
        '涨跌幅': 'change_rate',
        '涨跌额': 'change_amount',
        '成交量': 'volume',
        '成交额': 'turnover',
        '振幅': 'amplitude',
        '最高': 'high',
        '最低': 'low',
        '今开': 'open',
        '昨收': 'previous_close',
        '量比': 'volume_ratio',
        '换手率': 'turnover_rate',
        '市盈率-动态': 'pe_ratio',
        '市净率': 'pb_ratio',
        '总市值': 'total_market_cap',
        '流通市值': 'float_market_cap',
        '涨速': 'change_speed',
        '5分钟涨跌': 'change_5min',
        '60日涨跌幅': 'change_60d',
        '年初至今涨跌幅': 'change_ytd'
    }
    # 定义数据类型映射
    dtype_dict = {
        'id': 'int64',
        'code': 'str',
        'name': 'str',
        'close': 'float64',
        'change_rate': 'float64',
        'change_amount': 'float64',
        'volume': 'float64',
        'turnover': 'float64',
        'amplitude': 'float64',
        'high': 'float64',
        'low': 'float64',
        'open': 'float64',
        'previous_close': 'float64',
        'volume_ratio': 'float64',
        'turnover_rate': 'float64',
        'pe_ratio': 'float64',
        'pb_ratio': 'float64',
        'total_market_cap': 'float64',
        'float_market_cap': 'float64',
        'change_speed': 'float64',
        'change_5min': 'float64',
        'change_60d': 'float64',
        'change_ytd': 'float64'
    }

    # 查询当日数据
    df = ak.stock_sz_a_spot_em()
    df.rename(columns=column_mapping, inplace=True)
    # 去掉值为空的
    df = df[pd.notna(df["open"])]
    if df.shape[0] == 0:
        LOGGER.info(f'{current_date}未获取到数据, 不操作')
        return
    # 转换类型
    df = df.astype(dtype_dict)
    df['code'] = df['code'].astype('str')
    df['name'] = df['name'].astype('str')
    # 只取在需同步前缀列表中的数据
    df['prefix'] = df['code'].str[:2]
    df = df[df['prefix'].isin(sync_prefix_set)]
    df = df[['code', 'open', 'high', 'low', 'close', 'volume', 'turnover']]
    df['date'] = date_str
    df['date'] = pd.to_datetime(df['date'])
    # 这个接口返回的数据成交量单位是"100"，需要乘以100
    df['volume'] = df['volume'] * 100
    # 按前缀拆分为4个市场的df分别处理
    for market, prefixes in market_prefixes_dict.items():
        df0 = df[df['prefix'].isin(prefixes)]
        df0 = df0[['code', 'date', 'open', 'high', 'low', 'close', 'volume', 'turnover']]
        table_name = market_table_dict[market]
        df_append_2_local(table_name=table_name, df=df0)
        LOGGER.info(f'{current_date}, {market}同步数据完成, 数据条数: {df0.shape[0]}')
    # 更新名称变化数据
    sync_name_change(date_str, df[['code', 'name']])

if __name__ == '__main__':
    # sync_delisted()
    # t_dir = 'D:/new_tdx/T0002/export/bfq-sh-20251226'
    # t_dir = 'D:/new_tdx/T0002/export/bfq-sz-20251226'
    # tdx_file_data_2_local(t_dir)
    t_dir = 'D:/new_tdx/T0002/export/bfq-sh-sz-20251229'
    # t_dir = 'D:/new_tdx/T0002/export/bfq-sz-20251229'
    tdx_file_single_date_data_2_local(t_dir, '20251229')
    # web_interface_data_2_local()