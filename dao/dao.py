# 数据库连接配置
import logging
import traceback
from contextlib import contextmanager
from typing import List

import pandas as pd
from sqlalchemy import create_engine, MetaData, text
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.orm import sessionmaker

from utils.db_utils import get_db_url


logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

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
        logger.error(f"数据库操作失败: {e}")
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
