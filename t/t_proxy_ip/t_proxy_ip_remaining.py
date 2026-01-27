import requests

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
    balance_api_url = 'https://share.proxy.qg.net/balance?key=42FGHZSM'
    initial_balance = get_proxy_balance(balance_api_url)
    print(f'代理IP余额: {initial_balance}')
