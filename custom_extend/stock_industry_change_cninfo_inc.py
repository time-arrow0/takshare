import numpy as np
import pandas as pd
import requests
import py_mini_racer

from akshare.stock.stock_industry_cninfo import _get_file_content_ths
from utils.log_utils import setup_logger_simple_msg

LOGGER = setup_logger_simple_msg(name='stock_industry_change_cninfo_inc')

def stock_industry_change_cninfo_inc(
        objectid: int = 0,
        rowcount: int = 1000
) -> pd.DataFrame:
    """
    巨潮资讯-上市公司行业归属的变动情况
    https://webapi.cninfo.com.cn/#/apiDoc
    查询 p_stock2110 接口
    :param objectid: 起始记录ID。每次下载数据时，都要记录最大的一个OBJECTID，下次调用时将保存的更新的最大OBJECTID传入取增量更新数据,第一次调用可以传入0
    :type objectid: int
    :param rowcount: 返回记录条数。每次获取条数不能超过2000,默认为1000
    :type rowcount: int
    :return: 行业归属的变动情况
    :rtype: pandas.DataFrame
    """
    url = "http://webapi.cninfo.com.cn/api/load/p_stock2110_inc"
    params = {
        "objectid": objectid,
        "rowcount": rowcount
    }
    js_code = py_mini_racer.MiniRacer()
    js_content = _get_file_content_ths("cninfo.js")
    js_code.eval(js_content)
    mcode = js_code.call("getResCode1")
    headers = {
        "Accept": "*/*",
        "Accept-Encoding": "gzip, deflate",
        "Accept-Language": "zh-CN,zh;q=0.9,en;q=0.8",
        "Cache-Control": "no-cache",
        "Content-Length": "0",
        "Host": "webapi.cninfo.com.cn",
        "Accept-Enckey": mcode,
        "Origin": "https://webapi.cninfo.com.cn",
        "Pragma": "no-cache",
        "Proxy-Connection": "keep-alive",
        "Referer": "https://webapi.cninfo.com.cn/",
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) "
                      "Chrome/93.0.4577.63 Safari/537.36",
        "X-Requested-With": "XMLHttpRequest",
    }
    r = requests.post(url, params=params, headers=headers)
    data_json = r.json()
    temp_df = pd.DataFrame(data_json["records"])
    if temp_df.shape[0] == 0:
        return temp_df
    # cols_map = {
    #     "OBJECTID": "记录ID",
    #     "ORGNAME": "机构名称",
    #     "SECCODE": "证券代码",
    #     "SECNAME": "新证券简称",
    #     "VARYDATE": "变更日期",
    #     "F001V": "分类标准编码",
    #     "F002V": "分类标准",
    #     "F003V": "行业编码",
    #     "F004V": "行业门类",
    #     "F005V": "行业次类",
    #     "F006V": "行业大类",
    #     "F007V": "行业中类",
    #     "F008C": "最新记录标识",
    #     "CHANGE_CODE": "更新状态",
    #     "ROWKEY": "主键字段MD5值2",
    # }
    # temp_df.rename(columns=cols_map, inplace=True)
    temp_df.fillna(np.nan, inplace=True)
    temp_df["VARYDATE"] = pd.to_datetime(temp_df["VARYDATE"], errors="coerce").dt.date
    temp_df.sort_values('OBJECTID', inplace=True)
    return temp_df

# def stock_industry_change_cninfo_inc_all() -> pd.DataFrame:
#     objectid = 0
#     rowcount = 2000
#     dfs = []
#     while True:
#         df =

if __name__ == '__main__':
    pd.set_option('display.max_rows', None)  # 设置行数为无限制
    pd.set_option('display.max_columns', None)  # 设置列数为无限制
    pd.set_option('display.width', 1000)  # 设置列宽
    pd.set_option('display.colheader_justify', 'left')  # 设置列标题靠左

    # df = stock_industry_change_cninfo(
    #     symbol="002594",
    #     start_date="20201227",
    #     end_date="20220713",
    # )
    df = stock_industry_change_cninfo_inc(objectid=96877, rowcount=2000)
    LOGGER.info(df)
