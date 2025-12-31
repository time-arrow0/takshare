import os
import sys

# 项目根目录添加到系统路径
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
import logging
from contextlib import contextmanager
from typing import List, Union

import pandas as pd
from sqlalchemy import create_engine, MetaData, text, insert, Table
from sqlalchemy.orm import sessionmaker

from utils.db_utils import get_db_url
from utils.log_utils import setup_logger_simple_msg

logging.basicConfig(level=logging.INFO)
LOGGER = setup_logger_simple_msg(name='dao')

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
metadata = MetaData()
# 创建Session工厂
SessionLocal = sessionmaker(
    autocommit=False,  # 不自动提交
    autoflush=False,  # 不自动flush
    bind=engine  # 绑定到引擎
)

@contextmanager
def get_db_session():
    """
    数据库会话的上下文管理器
    使用方式：
    with get_db_session() as session:
        # 在这里执行数据库操作
    """
    session = SessionLocal()  # 从工厂创建新的Session
    try:
        yield session  # 将session提供给代码块使用
        session.commit()  # 如果代码块没有异常，提交事务
    except Exception as e:
        session.rollback()  # 如果有异常，回滚事务
        # LOGGER.error(f"数据库操作失败: {e}")
        raise  # 重新抛出异常
    finally:
        session.close()  # 无论如何都关闭Session


def obtain_df_by_sql(sql: str) -> pd.DataFrame:
    """
    从数据库获取数据，返回dataframe
    """
    df = pd.read_sql(sql, engine)
    return df

def obtain_value_by_sql(sql: str) -> int:
    """
    从数据库获取单个值
    """
    with get_db_session() as session:
        # 使用 text() 执行原生 SQL 查询
        result = session.execute(text(sql))
        v = result.scalar()
        return v

def obtain_list_by_sql(sql: str):
    """
    从数据库获取单个值
    """
    with get_db_session() as session:
        # 使用 text() 执行原生 SQL 查询
        result = session.execute(text(sql))
        seq = result.fetchall()
        return seq

def execute_by_sql(sql: str):
    """
    执行insert, update, delete等更新sql
    """
    with get_db_session() as session:
        result = session.execute(text(sql))
        # LOGGER.info(f'sql执行完成, 返回结果: {result}')
        return result

def insert_batch_single_column(table_name, column, data, batch_size=10000):
    with get_db_session() as session:
        total_inserted = 0
        # 分批插入
        # range(start, stop, step): 生成从 start 到 stop-1，步长为 step 的整数序列
        for i in range(0, len(data), batch_size):
            batch_data = data[i:i + batch_size]
            # 构建插入语句
            insert_stmt = text(f"INSERT INTO {table_name} ({column}) VALUES (:value)")
            # 准备参数
            params = [{"value": value} for value in batch_data]
            # 执行批量插入
            session.execute(insert_stmt, params)
            batch_inserted = len(batch_data)
            total_inserted += batch_inserted
            LOGGER.info(f"已插入 {batch_inserted} 条记录，累计 {total_inserted} 条")
        LOGGER.info(f"批量插入完成，总计插入 {total_inserted} 条记录")
        return total_inserted


def insert_batch(table_name: str, columns: List[str], rows: List[Union[tuple, list, dict]], batch_size: int = 10000) -> int:
    """
    使用SQLAlchemy Core的insert表达式进行批量插入
    """
    # 反射表结构
    try:
        table = Table(table_name, metadata, autoload_with=engine)
    except Exception as e:
        logging.error(f"无法反射表结构: {e}")
        raise
    # 转换数据为字典列表
    dict_list = []
    for i, item in enumerate(rows):
        if isinstance(item, dict):
            row_dict = {col: item.get(col) for col in columns}
        elif isinstance(item, (tuple, list)):
            row_dict = dict(zip(columns, item))
        else:
            raise ValueError(f"不支持的数据类型: {type(item)}")
        dict_list.append(row_dict)
    total_inserted = 0
    # 分批插入
    for i in range(0, len(dict_list), batch_size):
        batch = dict_list[i:i + batch_size]
        # 使用insert表达式
        stmt = insert(table).values(batch)
        with get_db_session() as session:
            result = session.execute(stmt)
        batch_count = len(batch)
        total_inserted += batch_count
        LOGGER.info(f"批次 {i // batch_size + 1}: 插入 {batch_count} 条记录")
    LOGGER.info(f"批量插入完成，总计插入 {total_inserted} 条记录")
    return total_inserted

def df_append_2_local(table_name, df, table_column_set=None):
    """
    如果传了table_column_set，不在table_column_set中的列会被删掉，避免插入报错
    """
    if table_column_set:
        columns = df.columns
        for column in columns:
            if column not in table_column_set:
                df.drop(column, axis=1, inplace=True)
        if len(df.columns) == 0:
            LOGGER.info('df没有和表中相同的列')
            return None
    with get_db_session() as session:
        # 使用session的connection
        df.to_sql(
            name=table_name,
            con=session.connection(),
            if_exists='append',
            index=False,
            method='multi',
            chunksize=10000
        )
        # 不需要显式commit，上下文管理器会自动处理
        print(f"{table_name} 成功插入 {len(df)} 条数据")
        return True


if __name__ == '__main__':
    tsql = """
    SELECT CODE FROM stocks_sh_main
UNION
SELECT CODE FROM stocks_sh_kc
UNION
SELECT CODE FROM stocks_sz_main
UNION
SELECT CODE FROM stocks_sz_cy
    """
    tseq = obtain_list_by_sql(tsql)
    for trow in tseq:
        print(trow[0])
