# -*- coding:utf-8 -*-
# 让所有现有的 requests.get和 session.get调用都自动带有代理重试功能
# use sample in other files (must be this import order):
# ...
# from utils import proxy_retry_patch
# import requests
# ...
# proxy_retry_patch.py
import os
import sys

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
import requests
from requests.exceptions import RequestException
import time
import types
from functools import wraps
from typing import Optional, Dict, Any

from utils.proxy_ip_util import get_valid_proxy

# 保存原始函数
_original_requests_get = requests.get
_original_session_class = requests.Session
_original_session_init = requests.Session.__init__
_original_session_get = requests.Session.get


def _make_proxy_retry_request(original_func, is_session_method=False):
    """
    创建带代理重试的请求函数
    """

    @wraps(original_func)
    def wrapper(*args, **kwargs):
        max_retries = 2
        retry_count = 0

        # 对于session.get，第一个参数是self
        if is_session_method:
            self_obj = args[0] if args else None
            url = args[1] if len(args) > 1 else kwargs.get('url', '')
        else:
            url = args[0] if args else kwargs.get('url', '')

        while retry_count <= max_retries:
            try:
                if retry_count > 0:
                    # 重试时获取新代理
                    try:
                        proxies = get_valid_proxy()
                        if proxies:
                            kwargs['proxies'] = proxies
                            print(f"[代理重试] 第{retry_count}次重试，URL: {url}，使用代理: {proxies}")
                        else:
                            print(f"[代理重试] 第{retry_count}次重试，URL: {url}，未获取到有效代理")
                    except Exception as proxy_error:
                        print(f"[代理重试] 获取代理失败: {proxy_error}")

                # 调用原始函数
                if is_session_method:
                    return original_func(*args, **kwargs)
                else:
                    return original_func(*args, **kwargs)

            except RequestException as e:
                retry_count += 1
                if retry_count > max_retries:
                    print(f"[代理重试] 请求失败，URL: {url}，异常: {e}，已达到最大重试次数({max_retries})")
                    raise
                print(f"[代理重试] 请求异常，URL: {url}，异常: {e}，开始第{retry_count}次重试...")
                time.sleep(0.5)  # 添加短暂延迟

        # 理论上不会执行到这里
        return original_func(*args, **kwargs)

    return wrapper


def patch_requests():
    """
    为requests库打补丁，使所有requests.get和session.get自动支持代理重试
    """
    # 1. 打补丁 requests.get
    requests.get = _make_proxy_retry_request(_original_requests_get, is_session_method=False)

    # 2. 打补丁 requests.Session.get
    requests.Session.get = _make_proxy_retry_request(_original_session_get, is_session_method=True)

    # 3. 确保新创建的Session对象也有补丁
    def _new_session_init(self, *args, **kwargs):
        """重写Session的__init__，确保新创建的Session也有补丁"""
        _original_session_init(self, *args, **kwargs)
        # 重新绑定get方法，确保使用带补丁的版本
        self.get = types.MethodType(
            _make_proxy_retry_request(_original_session_get, is_session_method=True),
            self
        )

    requests.Session.__init__ = _new_session_init

    # 4. 也包装其他常用的请求方法（可选）
    requests.post = _make_proxy_retry_request(requests.post, is_session_method=False)
    requests.put = _make_proxy_retry_request(requests.put, is_session_method=False)
    requests.delete = _make_proxy_retry_request(requests.delete, is_session_method=False)
    requests.head = _make_proxy_retry_request(requests.head, is_session_method=False)

    print("[代理重试] requests库已成功打补丁，所有get请求失败时将自动重试2次并尝试使用代理")


def unpatch_requests():
    """
    恢复原始函数
    """
    requests.get = _original_requests_get
    requests.Session.__init__ = _original_session_init
    requests.Session.get = _original_session_get

    # 恢复其他方法
    original_post = getattr(requests, '_original_post', requests.post)
    original_put = getattr(requests, '_original_put', requests.put)
    original_delete = getattr(requests, '_original_delete', requests.delete)
    original_head = getattr(requests, '_original_head', requests.head)

    requests.post = original_post
    requests.put = original_put
    requests.delete = original_delete
    requests.head = original_head

    print("[代理重试] requests库补丁已移除")


# 自动应用补丁
patch_requests()