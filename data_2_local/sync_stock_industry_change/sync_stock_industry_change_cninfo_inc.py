import os
import sys
import time
from datetime import datetime

sys.path.append(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))

from dao.dao import df_append_2_local, obtain_value_by_sql
from utils.log_utils import setup_logger_simple_msg
from custom_extend.stock_industry_change_cninfo_inc import stock_industry_change_cninfo_inc

LOGGER = setup_logger_simple_msg(name='stock_industry_change_cninfo_inc')

TABLE_NAME = 'stock_industry_change_cninfo_inc'

def sync_stock_industry_change_cninfo_inc_all():
    cols_map = {
        'OBJECTID': 'objectid',
        'ORGNAME': 'orgname',
        'SECCODE': 'seccode',
        'SECNAME': 'secname',
        'VARYDATE': 'varydate',
        'F001V': 'f001v',
        'F002V': 'f002v',
        'F003V': 'f003v',
        'F004V': 'f004v',
        'F005V': 'f005v',
        'F006V': 'f006v',
        'F007V': 'f007v',
        'F008C': 'f008c',
        'CHANGE_CODE': 'change_code',
    }
    objectid = 96877
    rowcount = 2000
    while True:
        LOGGER.info(f'objectid: {objectid}')
        df = stock_industry_change_cninfo_inc(objectid=objectid, rowcount=rowcount)
        if df.shape[0] == 0:
            LOGGER.info('数据已获取完, 结束')
            break
        df.rename(columns=cols_map, inplace=True)
        df = df[['objectid','orgname','seccode','secname','varydate','f001v','f002v','f003v','f004v','f005v','f006v','f007v','f008c','change_code']]
        df_append_2_local(TABLE_NAME, df)
        objectid = df.iloc[-1, 0] + 1 # 取最后一条的objectid再加1，因为已按照objectid排序，最后一条objectid最大
        time.sleep(0.5)

def sync_stock_industry_change_cninfo_inc():
    cols_map = {
        'OBJECTID': 'objectid',
        'ORGNAME': 'orgname',
        'SECCODE': 'seccode',
        'SECNAME': 'secname',
        'VARYDATE': 'varydate',
        'F001V': 'f001v',
        'F002V': 'f002v',
        'F003V': 'f003v',
        'F004V': 'f004v',
        'F005V': 'f005v',
        'F006V': 'f006v',
        'F007V': 'f007v',
        'F008C': 'f008c',
        'CHANGE_CODE': 'change_code',
    }
    sql = """
        select max(objectid) from stock_industry_change_cninfo_inc
    """
    max_objectid = obtain_value_by_sql(sql)
    if not max_objectid:
        LOGGER.error('未获取到max_objectid, 无法进行同步')
        return
    objectid = max_objectid + 1
    # print(max_objectid)
    LOGGER.info(f'objectid: {objectid}')
    rowcount = 2000
    count = 0
    while True:
        df = stock_industry_change_cninfo_inc(objectid=objectid, rowcount=rowcount)
        if df.shape[0] == 0:
            LOGGER.info('数据已获取完, 结束')
            break
        df.rename(columns=cols_map, inplace=True)
        df = df[['objectid','orgname','seccode','secname','varydate','f001v','f002v','f003v','f004v','f005v','f006v','f007v','f008c','change_code']]
        df_append_2_local(TABLE_NAME, df)
        objectid = df.iloc[-1, 0] + 1 # 取最后一条的objectid再加1，因为已按照objectid排序，最后一条objectid最大
        count += df.shape[0]
        time.sleep(0.5)
    if count == 0:
        LOGGER.info(f'未获取到新数据')

if __name__ == '__main__':
    # sync_stock_industry_change_cninfo_inc_all()
    sync_stock_industry_change_cninfo_inc()