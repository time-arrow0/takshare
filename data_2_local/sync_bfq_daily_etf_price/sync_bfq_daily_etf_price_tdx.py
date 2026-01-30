# 根据通达信导出数据同步
import os
import sys
sys.path.append(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))

from datetime import datetime

import pandas as pd
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

from data_2_local.common_data_2_local import df_append_2_local, get_price_last_date, get_price_max_date
from utils.db_utils import get_db_url
from utils.log_utils import setup_logger_simple_msg

# 数据库连接配置
DATABASE_URL = get_db_url()
# 创建数据库引擎
engine = create_engine(
    DATABASE_URL,
    echo=False,  # 生产环境关闭SQL日志
    pool_size=10,  # 连接池大小
    max_overflow=20,  # 最大溢出连接数
    pool_timeout=30,  # 获取连接超时时间
    pool_recycle=1800,  # 连接回收时间（避免MySQL 8小时问题）
    pool_pre_ping=True  # 执行前检测连接有效性
)
# 创建Session工厂
SessionLocal = sessionmaker(
    autocommit=False,  # 不自动提交
    autoflush=False,  # 不自动flush
    bind=engine  # 绑定到引擎
)

LOGGER = setup_logger_simple_msg(name='sync_bfq_daily_etf_price_tdx')

table_name = 'bfq_daily_etf_price'

def get_file_name(code):
    # 已退市股票。用当前页面数据导出的。已退市股票用高级导出是空的，没数据。
    if code.startswith(('50', '51', '56', '58')):
        file_name = f"bfq-SH#{code}.txt"
    elif code.startswith('15'):
        file_name = f"bfq-SZ#{code}.txt"
    else:
        file_name = ''
    return file_name


def get_initial_file_data(code, greater_date_ts=None):
    file_name = get_file_name(code)
    file_path = f'D:/new_tdx/T0002/export/bfq-etf-20260130/{file_name}'

    dtype_dict = {
        'open': 'float64',
        'close': 'float64',
        'high': 'float64',
        'low': 'float64',
        'turnover': 'float64',
    }
    skiprows = 2
    # turnover: 成交额
    column_names = ['date', 'open', 'high', 'low', 'close', 'volume', 'turnover']
    df = pd.read_csv(file_path,
                     sep='\s+',
                     skiprows=skiprows,
                     comment='#',  # 跳过以#开头的行
                     skip_blank_lines=True,  # 跳过空行
                     names=column_names,
                     dtype=dtype_dict,
                     encoding='GBK')
    # print(df)
    df = df[['date', 'open', 'high', 'low', 'close', 'volume', 'turnover']]
    df['code'] = code
    df['date'] = pd.to_datetime(df['date'])
    if greater_date_ts is not None:
        df = df[df['date'] > greater_date_ts]
    return df

def initial_tdx_file_data_2_local():
    with open(file='data/sync_bfq_daily_etf_price_tdx/codes.txt', mode='r', encoding='utf-8') as f:
        content = f.read()
        etf_codes0 = content.split('\n')
        etf_codes = []
        for code in etf_codes0:
            if code.strip():
                etf_codes.append(code)
    with open(file='data/sync_bfq_daily_etf_price_tdx/complete_codes.txt', mode='r', encoding='utf-8') as f:
        content = f.read()
        complete_codes0 = content.split('\n')
        complete_codes = []
        for code in complete_codes0:
            if code.strip():
                complete_codes.append(code)
    complete_code_set = set(complete_codes)
    codes = []
    for code in etf_codes:
        if code in complete_code_set:
            continue
        codes.append(code)
    print(f'len_codes: {len(codes)}')

    for code in codes:
        df = get_initial_file_data(code, None)
        df_append_2_local(table_name=table_name, df=df)
        with open(file='data/sync_bfq_daily_etf_price_tdx/complete_codes.txt', mode='a', encoding='utf-8') as f:
            f.write(f'{code}\n')

def tdx_file_data_2_local(tdx_file_dir, start_date_str=None, end_date_str=None):
    # 逐个读取目标目录文件，查表中该代码最大日期，把大于最大日期的数据写入数据库
    items = os.listdir(tdx_file_dir)
    dtype_dict = {
        'open': 'float64',
        'close': 'float64',
        'high': 'float64',
        'low': 'float64',
        'turnover': 'float64',
    }
    skiprows = 2
    # turnover: 成交额
    column_names = ['date', 'open', 'high', 'low', 'close', 'volume', 'turnover']
    # 代码在文件名中的截取范围
    code_start = 7
    code_end = 13  # 不包含
    # 已完成的代码
    with open(file='data/sync_bfq_daily_etf_price_tdx/complete_codes.txt', mode='r', encoding='utf-8') as f:
        content = f.read()
        complete_codes = content.split('\n')
    complete_code_set = set()
    for code in complete_codes:
        if code.strip():
            complete_code_set.add(code)

    for item in items:
        code = item[code_start: code_end]
        if code in complete_code_set:
            continue
        try:
            file_path = os.path.join(tdx_file_dir, item)
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
            last_date = get_price_last_date(table_name, code)
            if last_date is not None:
                last_date_ts = pd.Timestamp(last_date)
                df = df[df['date'] > last_date_ts].copy()
                if start_date_str:
                    start_date_ts = pd.Timestamp(start_date_str)
                    df = df[df['date'] >= start_date_ts].copy()
                if end_date_str:
                    end_date_ts = pd.Timestamp(end_date_str)
                    df = df[df['date'] <= end_date_ts].copy()

            if df.shape[0] > 0:
                df_append_2_local(table_name=table_name, df=df)
            with open(file='data/sync_bfq_daily_etf_price_tdx/complete_codes.txt', mode='a',
                      encoding='utf-8') as f:
                f.write(f'{code}\n')
            complete_code_set.add(code)
        except Exception as e:
            s = f'{code}保存出错'
            LOGGER.error(s)
            raise e




if __name__ == '__main__':
    pd.set_option('display.max_rows', None)  # 设置行数为无限制
    pd.set_option('display.max_columns', None)  # 设置列数为无限制
    pd.set_option('display.width', 1000)  # 设置列宽
    pd.set_option('display.colheader_justify', 'left')  # 设置列标题靠左

    # initial_tdx_file_data_2_local()
    # tdx_file_data_2_local('D:/new_tdx/T0002/export/bfq-etf-20260113', start_date_str=None, end_date_str='20260112')
    try:
        initial_tdx_file_data_2_local()
    except Exception as e:
        LOGGER.error(e)

