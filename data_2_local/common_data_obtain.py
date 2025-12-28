import os
import sys

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from dao.dao import obtain_list_by_sql


def obtain_stock_codes_a():
    """
    获取A股股票列表，除去京市，包括沪市主板、沪市科创板、深市主板、深市创业板
    """
    sql = """
        SELECT CODE FROM stocks_sh_main
        UNION
        SELECT CODE FROM stocks_sh_kc
        UNION
        SELECT CODE FROM stocks_sz_main
        UNION
        SELECT CODE FROM stocks_sz_cy
        """
    rows = obtain_list_by_sql(sql)
    codes = []
    for row in rows:
        codes.append(row[0])
    return codes