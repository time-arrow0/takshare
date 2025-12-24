# 数据库连接配置
import logging
import traceback
from contextlib import contextmanager

import pandas as pd
from sqlalchemy import create_engine, MetaData, text
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.orm import sessionmaker

from utils.db_utils import get_db_url
from utils.log_utils import setup_logger_simple_msg

LOGGER = setup_logger_simple_msg(name='common_data_2_local')

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

def obtain_middle_small_delisted_stock_code_list():
    table_name = 'delisted_middle_small'
    try:
        with get_db_session() as session:
            result = session.execute(text(f"SELECT code FROM {table_name}"))
            codes = [row[0] for row in result]
            return codes
    except Exception as e:
        print(f"获取退市中小板代码时出错 {table_name}: {e}")
        raise

def obtain_middle_small_delisted_stock_code_set():
    delisted_stock_code_list = obtain_middle_small_delisted_stock_code_list()
    delisted_stock_code_set = set(delisted_stock_code_list)
    return delisted_stock_code_set

def get_price_last_date(table_name, code):
    """
    获取price相关表记录最新日期
    :param table_name:
    :param code:
    :return:
    """
    try:
        with get_db_session() as session:
            # 使用 text() 执行原生 SQL 查询
            result = session.execute(text(f"SELECT MAX(date) FROM {table_name} WHERE code = '{code}'"))
            max_date = result.scalar()
            if max_date:
                return max_date
            else:
                return None
    except Exception as e:
        print(f"获取最后日期时出错 {table_name}: {e}")
        raise

def get_price_max_date(table_name):
    """
    获取price相关表记录最新日期
    :param table_name:
    :return:
    """
    try:
        with get_db_session() as session:
            # 使用 text() 执行原生 SQL 查询
            result = session.execute(text(f"SELECT MAX(date) FROM {table_name}"))
            max_date = result.scalar()
            if max_date:
                return max_date
            else:
                return None
    except Exception as e:
        print(f"获取最大日期时出错 {table_name}: {e}")
        raise

def obtain_code_max_date_timestamps(table_name, code_column, date_column):
    """
    返回dataframe，code列，date列
    """
    # 价格数据查询。前复权数据，早期数据涨跌幅列无意义
    query_sql = f"""
    SELECT {code_column} AS code, MAX({date_column}) AS date
    FROM {table_name}
    GROUP BY {code_column}
    """
    df = pd.read_sql(query_sql, engine)
    return df

def df_append_2_local(table_name, df, table_column_set=None):
    """
    如果传了table_column_set，不在table_column_set中的列会被删掉，避免插入报错
    """
    try:
        if table_column_set:
            columns = df.columns
            for column in columns:
                if column not in table_column_set:
                    df.drop(column, axis=1, inplace=True)
            if len(df.columns) == 0:
                LOGGER.info('df没有和表中相同的列')
                return None
        with get_db_session() as session:
            try:
                # 使用session的connection
                df.to_sql(
                    name=table_name,
                    con=session.connection(),
                    if_exists='append',
                    index=False,
                    method='multi',
                    chunksize=1000
                )
                # 不需要显式commit，上下文管理器会自动处理
                print(f"{table_name} 成功插入 {len(df)} 条数据")
                return True

            except Exception as e:
                # 不需要显式rollback，上下文管理器会自动处理
                print(f"插入数据时出错: {e}")
                raise e  # 重新抛出异常，让外层捕获
    except SQLAlchemyError as e:
        print(f"数据库错误: {e}")
        traceback.print_exc()
        raise
    except Exception as e:
        print(f"发生未知错误: {e}")
        traceback.print_exc()
        raise