import os
import json
import time
import random
import requests
import re
from bs4 import BeautifulSoup
from math import ceil
from utils import init_env, set_cookie, update_cookie

# 初始化环境变量
HEADERS, MAX_RETRY_COUNT, MAX_WORKERS, MAX_CONCURRENT_REQUESTS = init_env()

# 创建保存数据的目录
os.makedirs("data/data_id", exist_ok=True)
os.makedirs("debug_html", exist_ok=True)

# 固定的结果集ID和accountid
RESULT_SET_ID = "9C676CE969C84363PQ"
ACCOUNT_ID = "26782"
PER_PAGE = 100  # 每页结果数


def make_proquest_request(keyword, page=1, retry_count=0):
    """构造并发送ProQuest搜索请求"""
    if retry_count > MAX_RETRY_COUNT:
        print(f"关键词 '{keyword}' 第 {page} 页重试次数过多")
        return None, "重试次数过多"

    # 构建URL
    base_url = f"https://www.proquest.com/results/{RESULT_SET_ID}/{page}"
    params = {"accountid": ACCOUNT_ID}

    # 设置请求头
    HEADERS['Referer'] = f'https://www.proquest.com/results/{RESULT_SET_ID}?accountid={ACCOUNT_ID}'

    try:
        response = requests.get(base_url, headers=HEADERS, params=params)

        # 保存HTML内容用于调试
        safe_keyword = keyword.replace(' ', '_')  # 使用下划线替换空格
        debug_filename = f"debug_html/{safe_keyword}_page_{page}.html"
        with open(debug_filename, 'w', encoding='utf-8') as f:
            f.write(response.text)
        print(f"已保存HTML内容到: {debug_filename}")

        # 检查响应类型
        if response.status_code == 403:
            print("遇到403禁止访问错误，可能需要更新Cookie")
            HEADERS['Cookie'] = update_cookie()
            return make_proquest_request(keyword, page, retry_count + 1)

        # 检查是否被重定向到验证页面
        if "verify.proquest.com" in response.url:
            print("检测到验证页面，需要人工干预")
            return None, "验证页面拦截"

        response.raise_for_status()

        # 更新Cookie
        HEADERS['Cookie'] = set_cookie(response.headers, HEADERS['Cookie'])

        return response.text, None

    except Exception as e:
        print(f"\n请求ProQuest数据时出错: {str(e)}")
        time.sleep(5 + random.random())
        return make_proquest_request(keyword, page, retry_count + 1)


def extract_total_results(html_content):
    """从HTML内容中提取总结果数"""
    soup = BeautifulSoup(html_content, 'html.parser')
    results_count_elem = soup.find('h1', id='pqResultsCount')

    if results_count_elem:
        match = re.search(r'([\d,]+)', results_count_elem.get_text(strip=True))
        if match:
            return int(match.group(1).replace(',', ''))

    # 尝试其他位置查找总结果数
    results_count_elem = soup.find('div', class_='resultsCount')
    if results_count_elem:
        match = re.search(r'([\d,]+)', results_count_elem.get_text(strip=True))
        if match:
            return int(match.group(1).replace(',', ''))

    return 0


def extract_paper_data(html_content):
    """从HTML内容中提取论文标题和文档ID"""
    soup = BeautifulSoup(html_content, 'html.parser')
    results = []

    # 检查是否被反爬
    if soup.find('div', id='captcha-container'):
        print("检测到验证码页面")
        return None, "验证码拦截"

    # 检查是否有"没有结果"的提示
    no_results = soup.find('div', class_='noResults')
    if no_results:
        print("没有找到结果")
        return [], "没有结果"

    # 查找所有论文条目
    result_items = soup.find_all('li', class_='resultItem')
    print(f"找到 {len(result_items)} 个论文条目")

    for item in result_items:
        # 提取标题
        title_elem = item.find('h3') or item.find('div', class_='resultHeader')
        title = title_elem.get_text(strip=True) if title_elem else "未知标题"

        # 提取文档ID
        doc_id = None
        link_elem = item.find('a', href=re.compile(r'/docview/\d+'))
        if link_elem and 'href' in link_elem.attrs:
            match = re.search(r'/docview/(\d+)', link_elem['href'])
            if match:
                doc_id = match.group(1)

        if title and doc_id:
            results.append({
                "title": title,
                "id": doc_id
            })
        else:
            print(f"未能提取标题或ID: {str(item)[:100]}...")

    return results, None


def save_page_results(keyword, page, results):
    """保存单页结果到JSON文件"""
    # 使用下划线替换空格作为安全关键词
    safe_keyword = keyword.replace(' ', '_')

    # 创建关键词文件夹
    keyword_dir = os.path.join("data", "data_id", safe_keyword)
    os.makedirs(keyword_dir, exist_ok=True)

    # 创建文件名
    filename = os.path.join(keyword_dir, f"{safe_keyword}{page}.json")

    with open(filename, 'w', encoding='utf-8') as f:
        json.dump(results, f, ensure_ascii=False, indent=2)

    print(f"已保存第 {page} 页的 {len(results)} 篇论文到 {filename}")
    return filename


def search_proquest_papers(keyword, start_page=1):
    """搜索ProQuest论文并提取标题和文档ID"""
    # 获取第一页内容
    html_content, error = make_proquest_request(keyword, start_page)
    if error:
        print(f"获取初始页面失败: {error}")
        return

    # 提取总结果数
    total_results = extract_total_results(html_content)
    if total_results == 0:
        print("未找到任何结果")
        return

    print(f"总结果数: {total_results}")

    # 计算需要爬取的页数 (总结果数/3.2)
    pages_to_crawl = ceil(total_results / (3.2 * PER_PAGE))
    print(f"计划爬取 {pages_to_crawl} 页 (总结果数/{3.2})")

    # 爬取第一页
    page_results, error = extract_paper_data(html_content)
    if error:
        print(f"第{start_page}页提取失败: {error}")
        return

    save_page_results(keyword, start_page, page_results)

    # 爬取后续页面
    consecutive_empty = 0  # 连续空页计数器
    max_consecutive_empty = 3  # 最大允许连续空页数

    # 计算实际结束页
    end_page = min(start_page + pages_to_crawl - 1, start_page + 100)  # 限制最多爬取100页

    for page in range(start_page + 1, end_page + 1):
        print(f"正在获取第 {page} 页...")
        html_content, error = make_proquest_request(keyword, page)

        if error:
            print(f"获取第 {page} 页失败: {error}")
            if "验证" in error:
                print("遇到验证页面，暂停爬取")
                break
            continue

        page_results, error = extract_paper_data(html_content)

        if error:
            print(f"第{page}页提取失败: {error}")
            if "验证" in error:
                print("遇到验证页面，暂停爬取")
                break
            continue

        if not page_results:
            consecutive_empty += 1
            print(f"第{page}页没有数据 (连续空页: {consecutive_empty}/{max_consecutive_empty})")
            if consecutive_empty >= max_consecutive_empty:
                print(f"连续{max_consecutive_empty}页没有数据，停止爬取")
                break
        else:
            consecutive_empty = 0  # 重置计数器

        save_page_results(keyword, page, page_results)

        # 添加随机延迟避免被封
        delay = 2 + random.random() * 3  # 2-5秒随机延迟
        print(f"等待 {delay:.1f} 秒后继续...")
        time.sleep(delay)


def main():
    # 示例关键词
    keyword = "Protein Biochemistry"

    # 设置起始页
    start_page = 2  # 可以修改为任意起始页码

    # 搜索论文
    search_proquest_papers(keyword, start_page)


if __name__ == "__main__":
    main()