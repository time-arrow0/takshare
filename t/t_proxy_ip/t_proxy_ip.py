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

if __name__ == '__main__':
    # 提取代理API接口，获取1个代理IP
    api_url = "https://share.proxy.qg.net/get?key=42FGHZSM&num=1&area=&isp=0&format=json&distinct=true" # 填入刚生成的连接
    qingguo1 = get_valid_proxy(api_url)
    print(f'测试地址为{qingguo1}')
