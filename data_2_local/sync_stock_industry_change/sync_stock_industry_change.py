import os
import sys
import time
from datetime import datetime

sys.path.append(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))

from dao.dao import df_append_2_local, obtain_value_by_sql, obtain_list_by_sql, insert_batch
from utils.log_utils import setup_logger_simple_msg
from custom_extend.stock_industry_change_cninfo_inc import stock_industry_change_cninfo_inc

LOGGER = setup_logger_simple_msg(name='sync_stock_industry_change')

TABLE_NAME = 'stock_industry_change'


class IndustryStartDate:
    def __init__(self, industry, start_date):
        self.industry = industry
        self.start_date = start_date


class StockInfo:
    def __init__(self, code, security_name, organization_name):
        self.code = code
        self.security_name = security_name
        self.organization_name = organization_name


class IndustryInfo:
    def __init__(self, industry_code, industry_category, industry_sub_category, industry_major_category,
                 industry_medium_category):
        self.industry_code = industry_code
        self.industry_category = industry_category
        self.industry_sub_category = industry_sub_category
        self.industry_major_category = industry_major_category
        self.industry_medium_category = industry_medium_category


def sync_stock_industry_change_all():
    # 1. 数据只要"证监会行业分类标准"的3个代码(008001-证监会行业分类标准, 008009-证监会行业分类标准（2001）, 008021-证监会行业分类标准（2012）)，
    #   注意：如果某数据只有“更新状态”为“删除”的数据，那么它是已退市股，对于历史数据来说，“更新状态”为“删除”的数据还是要的
    #   只取“证监会行业分类标准”的数据，数据也是全的，这验证过了
    # 2. 定义dict：股票----List[行业+开始日期]，遍历第一步获取的数据，
    #   如果之前没有该股票的行业数据，赋值到股票----List[行业+开始日期]，列表中只含有当前数据
    #   如果之前已有该股票行业数据，
    #     取最后一条（由于按日期排序，最后一条是日期最大的）list[-1]，
    #       如果当前数据日期等于list[-1]的日期，跳过
    #       当前数据日期不等于list[-1]的日期，list新增当前数据
    sql = """
        select seccode, secname, orgname, varydate, f001v, f002v, f003v,  f004v, f005v, f006v, f007v, change_code
        from stock_industry_change_cninfo_inc
        where substr(seccode, 1, 2) in ('60', '68', '00', '30') and f001v in ('008001', '008009', '008021')
        order by seccode, varydate
    """
    list0 = obtain_list_by_sql(sql)
    # 代码----行业+开始日期
    code_2_industry_dates_dict = {}
    # 证券代码----证券其他信息
    stock_code_info_dict = {}
    # 行业代码----行业其他信息
    industry_code_info_dict = {}
    for row in list0:
        seccode, secname, orgname, varydate, f001v, f002v, f003v, f004v, f005v, f006v, f007v, change_code = row
        if not seccode or not varydate:
            continue
        # 如果有重复的，用新数据覆盖老数据
        stock_code_info_dict[seccode] = StockInfo(code=seccode, security_name=secname, organization_name=orgname)
        industry_code_info_dict[f003v] = IndustryInfo(
            industry_code=f003v,
            industry_category=f004v,
            industry_sub_category=f005v,
            industry_major_category=f006v,
            industry_medium_category=f007v
        )
        industry_start_dates = code_2_industry_dates_dict.get(seccode)
        # 如果之前没有该股票的行业数据
        if not industry_start_dates:
            # 新增一条数据，设置开始日期
            code_2_industry_dates_dict[seccode] = [IndustryStartDate(f003v, varydate)]
        # 之前已有该股票行业数据
        else:
            # 取最后一条（由于按日期排序，最后一条是日期最大的）list[-1]
            last_industry_start_date_datum = industry_start_dates[-1]
            # 如果当前数据日期等于list[-1]的日期，跳过
            if varydate == last_industry_start_date_datum.start_date:
                continue
            # 当前数据日期不等于list[-1]的日期，list新增当前数据
            industry_start_dates.append(IndustryStartDate(f003v, varydate))
    columns = ['code', 'security_name', 'organization_name', 'start_date', 'industry_code',
               'industry_category', 'industry_sub_category', 'industry_major_category', 'industry_medium_category']
    rows = []
    for code, industry_start_dates in code_2_industry_dates_dict.items():
        stock_info = stock_code_info_dict.get(code)
        for industry_start_date_datum in industry_start_dates:
            industry = industry_start_date_datum.industry
            start_date = industry_start_date_datum.start_date
            industry_info = industry_code_info_dict.get(industry)
            row = (code, stock_info.security_name, stock_info.organization_name, start_date,
                   industry,
                   industry_info.industry_category,
                   industry_info.industry_sub_category,
                   industry_info.industry_major_category,
                   industry_info.industry_medium_category
            )
            rows.append(row)
    try:
        insert_batch(table_name=TABLE_NAME, columns=columns, rows=rows)
    except Exception as e:
        LOGGER.error(e)
        raise
    LOGGER.info('同步完成')

def sync_stock_industry_change():
    """
    在sync_stock_industry_change_cninfo_inc运行完成后，根据它是否更新了运行
    """
    with open(file='data/sync_stock_industry_change_cninfo_inc/status.txt', mode='r', encoding='UTF-8') as f:
        content = f.read()
    if not content:
        LOGGER.info('sync_stock_industry_change_cninfo_inc状态文件为空, 需要它同步过后再同步行业变化数据')
        return
    date_str, updated = content.split('-')
    current_date_str = datetime.now().strftime('%Y%m%d')
    if date_str != current_date_str:
        LOGGER.info('sync_stock_industry_change_cninfo_inc今日未同步, 需要它同步过后再同步行业变化数据')
        return
    if updated == '0':
        LOGGER.info('sync_stock_industry_change_cninfo_inc今日同步没有新数据, 不需要同步行业变化数据')
        return
    with open(file='data/sync_stock_industry_change/sync_objectid.txt', mode='r', encoding='UTF-8') as f:
        content = f.read()
    if not content:
        LOGGER.info(f'sync_objectid.txt没有内容, 请先维护初始日期-已更新到的objectid')
        return
    date_str, last_objectid_str = content.split('-')

    # 上次更新到的objectid
    last_objectid = int(last_objectid_str)
    # 新的最大objectid
    sql = """
            select max(objectid) from stock_industry_change_cninfo_inc
        """
    max_objectid = obtain_value_by_sql(sql)
    if not max_objectid:
        LOGGER.error('未获取到max_objectid, 请核查原因')
        return
    if max_objectid == last_objectid:
        LOGGER.info('新的最大objectid和上次更新到的objectid一致, 不用同步')
        return
    sql = f"""
            select objectid, seccode, secname, orgname, varydate, f001v, f002v, f003v,  f004v, f005v, f006v, f007v, change_code
            from stock_industry_change_cninfo_inc
            where substr(seccode, 1, 2) in ('60', '68', '00', '30') and f001v in ('008001', '008009', '008021')
                and objectid > {last_objectid}
            order by seccode, varydate
        """
    new_industry_changes = obtain_list_by_sql(sql)
    # 这些代码已有的数据
    sql = f"""
        select code, start_date, industry_code
        from stock_industry_change
        where (code, start_date) in
            (
                select code, max(start_date) as start_date
                from stock_industry_change
                group by code
            )
            and code in
            (
                select distinct seccode
                from stock_industry_change_cninfo_inc
                where substr(seccode, 1, 2) in ('60', '68', '00', '30') and f001v in ('008001', '008009', '008021')
                    and objectid > {last_objectid}  
            )
    """
    existed_code_max_date_industries = obtain_list_by_sql(sql)
    # 代码----行业+开始日期
    code_2_industry_dates_dict = {}
    # 证券代码----证券其他信息
    stock_code_info_dict = {}
    # 行业代码----行业其他信息
    industry_code_info_dict = {}
    existed_code_industry_start_date_set = set()
    for row in existed_code_max_date_industries:
        code, start_date, industry_code = row
        code_2_industry_dates_dict[code] = [IndustryStartDate(industry_code, start_date)]
        existed_code_industry_start_date_set.add(f"{code}-{industry_code}-{start_date.strftime('%Y%m%d')}")
    # 注意, 新数据中同一代码的行业数据也可能有多条
    for row in new_industry_changes:
        objectid, seccode, secname, orgname, varydate, f001v, f002v, f003v,  f004v, f005v, f006v, f007v, change_code = row
        # 如果有重复的，用新数据覆盖老数据
        stock_code_info_dict[seccode] = StockInfo(code=seccode, security_name=secname, organization_name=orgname)
        industry_code_info_dict[f003v] = IndustryInfo(
            industry_code=f003v,
            industry_category=f004v,
            industry_sub_category=f005v,
            industry_major_category=f006v,
            industry_medium_category=f007v
        )

        industry_start_dates = code_2_industry_dates_dict.get(seccode)
        # 如果之前没有该股票的行业数据
        if not industry_start_dates:
            # 新增一条数据，设置开始日期
            code_2_industry_dates_dict[seccode] = [IndustryStartDate(f003v, varydate)]
        # 之前已有该股票行业数据
        else:
            # 取最后一条（由于按日期排序，最后一条是日期最大的）list[-1]
            last_industry_start_date_datum = industry_start_dates[-1]
            # 如果当前数据日期小于等于list[-1]的日期，跳过，因为新数据只有大于原来最大日期的数据才有意义
            if varydate <= last_industry_start_date_datum.start_date:
                continue
            # 当前数据日期不等于list[-1]的日期，list新增当前数据
            industry_start_dates.append(IndustryStartDate(f003v, varydate))
    columns = ['code', 'security_name', 'organization_name', 'start_date', 'industry_code',
               'industry_category', 'industry_sub_category', 'industry_major_category', 'industry_medium_category']
    rows = []
    for code, industry_start_dates in code_2_industry_dates_dict.items():
        stock_info = stock_code_info_dict.get(code)
        for industry_start_date_datum in industry_start_dates:
            industry = industry_start_date_datum.industry
            start_date = industry_start_date_datum.start_date
            code_industry_start_date = f"{code}-{industry}-{start_date.strftime('%Y%m%d')}"
            # 表中已有的跳过
            if code_industry_start_date in existed_code_industry_start_date_set:
                continue
            industry_info = industry_code_info_dict.get(industry)
            row = (code, stock_info.security_name, stock_info.organization_name, start_date,
                   industry,
                   industry_info.industry_category,
                   industry_info.industry_sub_category,
                   industry_info.industry_major_category,
                   industry_info.industry_medium_category
            )
            rows.append(row)
    if len(rows) == 0:
        LOGGER.info(f'没有新新数据要同步')
        return
    try:
        insert_batch(table_name=TABLE_NAME, columns=columns, rows=rows)
    except Exception as e:
        LOGGER.error(e)
        raise
    # 写入更新到的objectid
    current_date_str = datetime.now().strftime('%Y%m%d')
    with open(file='data/sync_stock_industry_change/sync_objectid.txt', mode='w', encoding='UTF-8') as f:
        f.write(f'{current_date_str}-{max_objectid}')
    LOGGER.info('同步完成')

if __name__ == '__main__':
    # sync_stock_industry_change_all()
    sync_stock_industry_change()
