import pandas as pd

import akshare as ak
from utils.log_utils import setup_logger_simple_msg

LOGGER = setup_logger_simple_msg(name='t9_akshare_interface')

if __name__ == '__main__':
    pd.set_option('display.max_rows', None)  # 设置行数为无限制
    pd.set_option('display.max_columns', None)  # 设置列数为无限制
    pd.set_option('display.width', 1000)  # 设置列宽
    pd.set_option('display.colheader_justify', 'left')  # 设置列标题靠左

    # stock_share_change_cninfo_df = ak.stock_share_change_cninfo(symbol="002594", start_date="20091227", end_date="20251206")
    # stock_share_change_cninfo_df = ak.stock_share_change_cninfo(symbol="600000", start_date="20000101", end_date="20251206")
    # print(stock_share_change_cninfo_df)
    # fund_etf_spot_em_df = ak.fund_etf_spot_em()
    # print(fund_etf_spot_em_df)

    # stock_fhps_detail_ths_df = ak.stock_fhps_detail_ths(symbol="002002")
    # print(stock_fhps_detail_ths_df)

    # stock_fhps_em_df = ak.stock_fhps_em(date="20231231")
    # print(stock_fhps_em_df)


    # stock_zh_a_spot_em_df = ak.stock_zh_a_spot_em()
    # filepath = 'hangqing.txt'
    # stock_zh_a_spot_em_df.to_csv(filepath, index=False)


    # stock_sse_summary_df = ak.stock_sse_summary()
    # print(stock_sse_summary_df)
    #
    #
    # stock_szse_summary_df = ak.stock_szse_summary(date="20251224")
    # print(stock_szse_summary_df)

    # stock_zh_a_hist_df = ak.stock_zh_a_hist(symbol="002002", period="daily", adjust="")
    # LOGGER.info(stock_zh_a_hist_df)
    # df = ak.stock_zh_a_daily(symbol="sz300114")
    # print(df)

    # df = ak.stock_share_change_cninfo(symbol='000001', start_date="20000101", end_date="20260114")
    # print(df)

    # stock_financial_report_sina_df = ak.stock_financial_report_sina(stock="sh600600", symbol="资产负债表")
    # print(stock_financial_report_sina_df)

    # stock_financial_abstract_df = ak.stock_financial_abstract(symbol="600004")
    # print(stock_financial_abstract_df)

    stock_zh_a_spot_df = ak.stock_zh_a_spot()
    print(stock_zh_a_spot_df)