import os
import sys
sys.path.append(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))

import pandas as pd

from utils.log_utils import setup_logger_simple_msg
from dao.dao import obtain_list_by_sql, df_append_2_local

LOGGER = setup_logger_simple_msg(name='sync_stock_name_change')
TABLE_NAME = 'stock_name_change'

def obtain_initial_name_change_data():
    """
    获取初始股票名称变化数据，目前是从bigquant，在20251226获取的
    注意先把"Ａ"替换为"A"，"Ａ"并不是英文字母"A"
    """
    file_path = 'data/namechange20251226.txt'
    skiprows = 2
    column_names = ['code', 'start_date', 'end_date', 'name']
    df = pd.read_csv(file_path,
                     sep=',',
                     skiprows=skiprows,
                     comment='#',  # 跳过以#开头的行
                     skip_blank_lines=True,  # 跳过空行
                     names=column_names,
                     encoding='UTF-8')
    df.drop('end_date', axis=1, inplace=True)
    # name去掉空格
    df['name'] = df['name'].str.replace(r'\s+', '', regex=True)
    # code只取代码
    df['code'] = df['code'].str[:6]
    dfs = []
    # name等于上一条的数据删掉，只保留name不等于上一条的。结果就是，相邻name相等的，只保留最早的那一条
    for code, group in df.groupby('code'):
        group['name1'] = group['name'].shift(1)
        # df1 = group[group['name'] == group['name1']]
        # if df1.shape[0] > 0:
        #     print(df1.values)
        df1 = group[group['name'] != group['name1']]
        dfs.append(df1)
    final_df = pd.concat(dfs, ignore_index=True)
    final_df.drop(labels=['name1'], axis=1, inplace=True)
    # print(final_df)
    # print(final_df[final_df['code'] == '000002'])
    # print(final_df.shape[0])
    final_df['start_date'] = pd.to_datetime(final_df['start_date'])
    return final_df

def sync_all():
    """
    股票名称变化，暂时通过bigquant的cn_stock_name_change表(数据平台: 股票数据->股票信息)获取，
    因为akshare只有1个stock_info_sz_change_name获取深圳市场股票名称变更，没有上海市场的
    """
    sql = """
        SELECT CONCAT(code, '-', DATE_FORMAT(MAX(start_date), '%Y%m%d'))
        FROM stock_name_change
        GROUP BY code
    """
    code_start_date_strs = obtain_list_by_sql(sql)
    code_start_date_str_set = set()
    for row in code_start_date_strs:
        code_start_date_str_set.add(row[0])
    df = obtain_initial_name_change_data()
    df['unique_key'] = df['code'] + '-' + df['start_date'].dt.strftime('%Y%m%d')
    df = df[~df['unique_key'].isin(code_start_date_str_set)]
    df.drop(labels=['unique_key'], axis=1, inplace=True)
    LOGGER.info(f'数据量: {df.shape[0]}')
    df_append_2_local(TABLE_NAME, df)

def obtain_max_date_code_name_dict():
    sql = """
        select s.code, s.name
        from stock_name_change s
        inner join
        (
            select code, max(start_date) as start_date
            from stock_name_change
            group by code
        ) a on s.code = a.code and s.start_date = a.start_date
    """
    code_names = obtain_list_by_sql(sql)
    max_date_code_name_dict = {}
    for row in code_names:
        code, name = row
        max_date_code_name_dict[code] = name
    return max_date_code_name_dict

def sync_name_change(date_str, df):
    """
    这个在每日更新股票价格数据时顺便更新，把df['code', 'name']传进来
    """
    max_date_code_name_dict = obtain_max_date_code_name_dict()
    df['name1'] = df['code'].map(max_date_code_name_dict)
    # 筛选出name1为空，或者不等于name的列
    df = df[pd.isna(df['name1']) | (df['name'] != df['name1'])]
    if df.shape[0] == 0:
        LOGGER.info(f'{date_str}没有新名称')
        return
    df.loc[:, 'start_date'] = pd.to_datetime(date_str)
    df = df[['code', 'start_date', 'name']]
    df_append_2_local(TABLE_NAME, df)
    LOGGER.info(f'{date_str}同步名称完成')


if __name__ == '__main__':
    # obtain_initial_name_change_data()
    # sync_all()
    # t_max_date_code_name_dict = obtain_max_date_code_name_dict()
    # print(t_max_date_code_name_dict)
    # sync_name_change('20251229', pd.DataFrame({'code': ['000001'], 'name': ['平安银行']}))
    print()