import os
import sys

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
# wrap requests.get, session.get
from utils import proxy_retry_patch
import requests

proxy_key = 'G2IUJ5LX'
api_url = f"https://share.proxy.qg.net/get?key={proxy_key}&num=1&area=&isp=0&format=json&distinct=true"
balance_api_url = f'https://share.proxy.qg.net/balance?key={proxy_key}'

def get_valid_proxy(proxy_api_url: str=api_url) -> dict:
    """
    从API获取代理，并检测是否可用（使用国内网站测试）
    """

    try:
        resp = requests.get(proxy_api_url, timeout=5)
        proxy_json = resp.json()
        proxy_data = proxy_json["data"][0]  # 注意这里是列表索引[0]
        # 分割server字段获取IP和端口
        server = proxy_data["server"]
        ip, port = server.split(":")  # 直接按冒号分割
        proxy = {"http": f"http://{ip}:{port}", "https": f"http://{ip}:{port}"}

        # 使用国内网站检测代理连通性
        test_url = "http://www.baidu.com"  # 或换成 "http://quote.eastmoney.com"
        test = requests.get(test_url, proxies=proxy, timeout=10)
        if test.status_code == 200:
            print(f"代理可用: {ip}:{port}")
            return proxy
        else:
            print(f"代理连接失败（状态码非200）: {ip}:{port}")
    except requests.exceptions.RequestException as e:
        print(f"代理不可用,原因: {e}")
    except Exception as e:
        print(f"解析代理或请求异常: {e}")
    return {}

# 查询余额
def get_proxy_balance(balance_api_url):
    """
    查询代理余额
    """
    try:
        resp = requests.get(balance_api_url, timeout=5)
        balance_json = resp.json()

        if balance_json.get("code") == "SUCCESS":
            balance = balance_json["data"]["balance"]
            print(f"代理余额查询成功: {balance}")
            return balance
        else:
            print(f"代理余额查询失败: {balance_json}")
            return None

    except requests.exceptions.RequestException as e:
        print(f"代理余额查询网络错误: {e}")
        return None
    except Exception as e:
        print(f"代理余额查询异常: {e}")
        return None

if __name__ == '__main__':
    test_url = "http://www.baidu.comx"  # 或换成 "http://quote.eastmoney.com"
    test = requests.get(test_url, timeout=3)
    if test.status_code == 200:
        print(f"200")
    else:
        print(f"非200")