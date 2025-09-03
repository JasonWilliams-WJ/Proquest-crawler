import os
from dotenv import load_dotenv


def init_env():
    """初始化环境变量，返回请求头和配置参数"""
    load_dotenv()
    HEADERS = {
        'User-Agent': os.getenv('USER_AGENT'),
        'Referer': os.getenv('Referer', 'https://www.proquest.com/'),
        'Accept': os.getenv('ACCEPT'),
        'Accept-Language': os.getenv('ACCEPT_LANGUAGE'),
        'Accept-Encoding': os.getenv('ACCEPT_ENCODING', 'gzip, deflate, br'),
        'Connection': os.getenv('CONNECTION', 'keep-alive'),
        'Cache-Control': os.getenv('CACHE_CONTROL', 'max-age=0'),
        'Cookie': os.getenv('COOKIE'),
        # 添加ProQuest可能需要的其他请求头
        'Sec-Fetch-Dest': 'document',
        'Sec-Fetch-Mode': 'navigate',
        'Sec-Fetch-Site': 'same-origin',
        'Upgrade-Insecure-Requests': '1'
    }
    MAX_RETRY_COUNT = int(os.getenv('MAX_RETRY_COUNT', 7))
    MAX_WORKERS = int(os.getenv('MAX_WORKERS', 100))
    MAX_CONCURRENT_REQUESTS = int(os.getenv('MAX_CONCURRENT_REQUESTS', 5))
    return HEADERS, MAX_RETRY_COUNT, MAX_WORKERS, MAX_CONCURRENT_REQUESTS


def set_cookie(headers, existing_cookie):
    """根据响应头更新Cookie"""
    if 'Set-Cookie' in headers:
        # 获取所有Set-Cookie头（可能是列表或字符串）
        set_cookies = headers.getlist('Set-Cookie') if hasattr(headers, 'getlist') else [headers['Set-Cookie']]

        # 将现有的cookie字符串解析为字典
        existing_cookies = {}
        if existing_cookie:
            for cookie_pair in existing_cookie.split('; '):
                if '=' in cookie_pair:
                    name, value = cookie_pair.split('=', 1)
                    existing_cookies[name] = value

        # 处理每个新的Set-Cookie
        for set_cookie_str in set_cookies:
            # 提取cookie名称和值（只取第一个分号前的部分）
            cookie_parts = set_cookie_str.split(';')[0]
            if '=' in cookie_parts:
                name, value = cookie_parts.split('=', 1)
                name = name.strip()
                # 更新或添加新的cookie值
                existing_cookies[name] = value

        # 将更新后的cookies转换回字符串
        new_cookies = '; '.join(f"{name}={value}" for name, value in existing_cookies.items())
        return new_cookies

    return existing_cookie


def update_cookie():
    """更新Cookie（例如从外部获取新Cookie）"""
    print("检测到验证码或需要更新Cookie，请在浏览器完成验证后，修改.env文件中的COOKIE值...")
    input("修改完成后按回车键继续...")

    # 重新加载.env文件
    load_dotenv(override=True)
    new_cookie = os.getenv('COOKIE')
    return new_cookie