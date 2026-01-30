import os
import sys
sys.path.append(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))

from utils import proxy_retry_patch
import requests
from datetime import datetime

import pandas as pd
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

import akshare as ak

from data_2_local.common_data_2_local import df_append_2_local, get_price_last_date, get_price_max_date
from utils.db_utils import get_db_url
from utils.holiday_utils import ChinaHolidayChecker
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

LOGGER = setup_logger_simple_msg(name='bfq_daily_etf_price')

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


def get_file_data(code, greater_date_ts=None):
    file_name = get_file_name(code)
    file_path = f'D:/new_tdx/T0002/export/bfq-etf-20251225/{file_name}'

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
    with open(file='data/bfq_daily_etf_price/codes.txt', mode='r', encoding='utf-8') as f:
        content = f.read()
        etf_codes0 = content.split('\n')
        etf_codes = []
        for code in etf_codes0:
            if code.strip():
                etf_codes.append(code)
    with open(file='data/bfq_daily_etf_price/complete_codes.txt', mode='r', encoding='utf-8') as f:
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
    # t_codes = ['002003']
    print(f'len_codes: {len(codes)}')

    for code in codes:
        df = get_file_data(code, None)
        df_append_2_local(table_name=table_name, df=df)
        with open(file='data/bfq_daily_etf_price/complete_codes.txt', mode='a', encoding='utf-8') as f:
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
    with open(file='data/bfq_daily_etf_price/complete_codes.txt', mode='r', encoding='utf-8') as f:
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
            with open(file='data/bfq_daily_etf_price/complete_codes.txt', mode='a',
                      encoding='utf-8') as f:
                f.write(f'{code}\n')
            complete_code_set.add(code)
        except Exception as e:
            s = f'{code}保存出错'
            LOGGER.error(s)
            raise e



def daily_data_2_local():
    """
    每日收盘后，通过网络接口获取数据，写入数据库
    """
    etf_code_set = {
        # A股特有逻辑
        '512480',  # 半导体ETF 无法替代：A股半导体是国产替代主战场，港股无对应标的。
        '512710',  # 军工ETF 无法替代：核心国防资产在A股，逻辑独特。
        '159326',  # 电网设备ETF 无法替代：中国新型电力系统投资的核心标的。
        '159915',  # 创业板ETF 风格代表：作为A股成长风格的整体贝塔，无法被替代。

        # 港股/中概（估值优势）
        '159892',  # 恒生医药ETF 替换A股医疗：成分股与A股医药高度重合（药明生物、百济神州等），但估值显著更低，直接受益于全球流动性。
        '513050',  # 中概互联网ETF 核心持有：中国互联网巨头集合体，估值已从极端回归，比A股计算机/传媒板块有吸引力。
        '159691',  # 港股红利ETF 独特风格：提供A股稀缺的“高股息”风格，且港股金融、地产等红利股估值极低，防御性强。
        '513090',  # 香港证券ETF 强周期工具：直接受益于港股市场回暖（成交放量、IPO复苏），弹性大，且估值处于历史低位。

        # 全球 & 商品
        '513500',  # 标普500ETF 全球锚：不可替代的全球资产定价基准。
        '159941',  # 纳指ETF 全球科技矛：与A股/港股科技股形成竞争与互补，估值由全球利率定价。
        '159980',  # 有色ETF 全球通胀交易：逻辑清晰，与股票资产相关性较低。
        '159985',  # 豆粕ETF 农产品通胀：独特的供给驱动逻辑。
        '518880',  # 黄金ETF 终极避险：与所有权益资产逻辑不同。

        '159949',  # 创业板50ETF
        '510150',  # 消费ETF
        '510180',  # 上证180ETF
        '512050',  # A500ETF
        '512100',  # 中证1000ETF
        '512930',  # 人工智能ETF
        '513100',  # 纳指ETF
        '513330',  # 恒生互联网ETF
        '562500',  # 机器人ETF
        '588000',  # 科创50ETF
        '588100',  # 科创信息技术ETF
    }
    current_date = datetime.now().date()
    date_str = current_date.strftime('%Y%m%d')
    # 如果当天不是工作日，不操作
    if not ChinaHolidayChecker.is_workday(current_date):
        LOGGER.info(f'{current_date}不是工作日, 不操作')
        return

    # 如果表中今日已有数据，不再运行
    # 查询表中数据最大日期
    max_date = get_price_max_date(table_name)
    max_date_str = max_date.strftime('%Y%m%d')
    # 如果最大日期等于当日日期，跳过。（后面如果有需求，可以先查出表中当日数据，再把接口获取的数据去除掉已存在的，再插入表中）
    if date_str == max_date_str:
        LOGGER.info(f'{current_date}已有数据, 不操作')
        return

    # 进一步检查，判断指数是否有数据，指数有数据才运行
    index_df = ak.stock_zh_index_daily_em(symbol="sh000001", start_date=date_str, end_date=date_str)
    if index_df.shape[0] == 0:
        LOGGER.info(f'{current_date}指数无数据, 不操作')
        return

    # 定义中文到英文的列名映射
    column_mapping = {
        '代码': 'code',
        '名称': 'name',
        '最新价': 'close',
        'IOPV实时估值': 'iopv_real_time',
        '基金折价率': 'fund_discount_rate',
        '涨跌额': 'price_change',
        '涨跌幅': 'price_change_pct',
        '成交量': 'volume',
        '成交额': 'turnover',
        '开盘价': 'open',
        '最高价': 'high',
        '最低价': 'low',
        '昨收': 'prev_close',
        '换手率': 'turnover_rate',
        '量比': 'volume_ratio',
        '委比': 'order_ratio',
        '外盘': 'outside_volume',
        '内盘': 'inside_volume',
        '主力净流入-净额': 'main_net_inflow_amount',
        '主力净流入-净占比': 'main_net_inflow_ratio',
        '超大单净流入-净额': 'super_large_net_inflow_amount',
        '超大单净流入-净占比': 'super_large_net_inflow_ratio',
        '大单净流入-净额': 'large_net_inflow_amount',
        '大单净流入-净占比': 'large_net_inflow_ratio',
        '中单净流入-净额': 'medium_net_inflow_amount',
        '中单净流入-净占比': 'medium_net_inflow_ratio',
        '小单净流入-净额': 'small_net_inflow_amount',
        '小单净流入-净占比': 'small_net_inflow_ratio',
        '现手': 'current_hand',
        '买一': 'bid_1',
        '卖一': 'ask_1',
        '最新份额': 'latest_share',
        '流通市值': 'float_market_cap',
        '总市值': 'total_market_cap',
        '数据日期': 'data_date',
        '更新时间': 'update_time'
    }
    # 定义数据类型映射
    dtype_dict = {
        'close': 'float64',
        'iopv_real_time': 'float64',
        'fund_discount_rate': 'float64',
        'price_change': 'float64',
        'price_change_pct': 'float64',
        'volume': 'float64',
        'turnover': 'float64',
        'open': 'float64',
        'high': 'float64',
        'low': 'float64',
        'prev_close': 'float64',
        'turnover_rate': 'float64',
        'volume_ratio': 'float64',
        'order_ratio': 'float64',
        'outside_volume': 'float64',
        'inside_volume': 'float64',
        'main_net_inflow_amount': 'float64',
        'main_net_inflow_ratio': 'float64',
        'super_large_net_inflow_amount': 'float64',
        'super_large_net_inflow_ratio': 'float64',
        'large_net_inflow_amount': 'float64',
        'large_net_inflow_ratio': 'float64',
        'medium_net_inflow_amount': 'float64',
        'medium_net_inflow_ratio': 'float64',
        'small_net_inflow_amount': 'float64',
        'small_net_inflow_ratio': 'float64',
        'current_hand': 'float64',
        'bid_1': 'float64',
        'ask_1': 'float64',
        'latest_share': 'float64',
        'float_market_cap': 'int64',
        'total_market_cap': 'int64',
    }

    # 查询当日数据
    df = ak.fund_etf_spot_em()
    if df.shape[0] == 0:
        LOGGER.info(f'{current_date}未获取到数据, 不操作')
        return
    df.rename(columns=column_mapping, inplace=True)
    # 去掉值为空的
    df = df[pd.notna(df["open"])]
    if df.shape[0] == 0:
        LOGGER.info(f'{current_date}未获取到数据, 不操作')
        return
    # 转换类型
    df = df.astype(dtype_dict)
    df['code'] = df['code'].astype('str')
    # 只保留在etf列表中的
    df = df[df['code'].isin(etf_code_set)].copy()
    df = df[['code', 'open', 'high', 'low', 'close', 'volume', 'turnover']]
    df['date'] = date_str
    df['date'] = pd.to_datetime(df['date'])
    # 这个接口返回的数据成交量单位是"100"，需要乘以100
    df['volume'] = df['volume'] * 100
    df_append_2_local(table_name=table_name, df=df)
    LOGGER.info(f'{current_date}同步数据完成, 数据条数: {df.shape[0]}')



if __name__ == '__main__':
    pd.set_option('display.max_rows', None)  # 设置行数为无限制
    pd.set_option('display.max_columns', None)  # 设置列数为无限制
    pd.set_option('display.width', 1000)  # 设置列宽
    pd.set_option('display.colheader_justify', 'left')  # 设置列标题靠左

    # initial_tdx_file_data_2_local()
    # tdx_file_data_2_local('D:/new_tdx/T0002/export/bfq-etf-20260113', start_date_str=None, end_date_str='20260112')
    try:
        daily_data_2_local()
    except Exception as e:
        LOGGER.error(e)

    # LOGGER.info('xxx')
    # df = ak.fund_etf_spot_em()
