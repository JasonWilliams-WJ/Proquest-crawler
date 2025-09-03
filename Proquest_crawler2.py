import os
import json
import time
import random
import requests
import re
from bs4 import BeautifulSoup
from utils import init_env, set_cookie, update_cookie
from urllib.parse import urljoin

# 初始化环境变量
HEADERS, MAX_RETRY_COUNT, MAX_WORKERS, MAX_CONCURRENT_REQUESTS = init_env()

# 创建保存数据的目录
os.makedirs("data/data_details", exist_ok=True)

# 关键词用于创建文件夹
KEYWORD = "Protein Biochemistry"

# 全局变量用于跟踪爬取状态
crawling_status = {
    "total_papers": 0,
    "crawled_count": 0,
    "current_page": 1,
    "last_save_time": time.time()
}

# 正确的ProQuest基础URL
PROQUEST_BASE_URL = "https://www.proquest.com"


def make_detail_request(paper_id, retry_count=0):
    """构造并发送论文详情页请求"""
    if retry_count > MAX_RETRY_COUNT:
        print(f"论文 {paper_id} 重试次数过多")
        return None, "重试次数过多"

    # 正确构建URL - 使用urljoin确保URL格式正确
    url = urljoin(PROQUEST_BASE_URL, f"/docview/{paper_id}/abstract")

    try:
        # 添加随机延迟，避免请求过于频繁
        delay = random.uniform(1, 3)
        time.sleep(delay)

        # 发送请求
        response = requests.get(url, headers=HEADERS)

        # 检查响应类型
        if response.status_code == 403:
            print("遇到极速禁止访问错误，可能需要更新Cookie")
            HEADERS['Cookie'] = update_cookie()
            return make_detail_request(paper_id, retry_count + 1)

        if response.status_code == 429:  # Too Many Requests
            print("遇到429错误，请求过于频繁，等待一段时间后重试")
            time.sleep(30)  # 等待30秒
            return make_detail_request(paper_id, retry_count + 1)

        response.raise_for_status()

        # 更新Cookie
        HEADERS['Cookie'] = set_cookie(response.headers, HEADERS['Cookie'])

        return response.text, None

    except Exception as e:
        print(f"\n请求论文详情页时出错: {str(e)}")
        # 遇到错误时等待更长时间
        time.sleep(10 + random.random())
        return make_detail_request(paper_id, retry_count + 1)


def parse_detail_page(html_content, paper_id):
    """解析论文详情页，提取关键信息"""
    soup = BeautifulSoup(html_content, 'html.parser')

    # 初始化数据字典 - 移除了degree date和language字段
    paper_data = {
        "Title": "",
        "Author": "",
        "degree type": "",
        "advisor": "",
        "University/institute": "",
        "University location-country": "",
        "University location-city": "",
        "Department": "",
        "Publication Year": "",
        "Document URL": f"{PROQUEST_BASE_URL}/docview/{paper_id}/abstract",
        "Abstract": "",
        "subject": [],
        "Classification": [],
        "Identifier / keyword": [],
        "Committee member": []
    }

    try:
        # 提取标题
        title_elem = soup.find('h1', class_='documentTitle')
        if title_elem:
            paper_data["Title"] = title_elem.get_text(strip=True)

        # 检查标题是否为空 - 新增检查逻辑
        if not paper_data["Title"]:
            raise ValueError(f"论文 {paper_id} 的标题为空，可能是无效页面或爬取失败")

        # 提取作者
        author_elem = soup.select_one('#authordiv a.author-name')
        if not author_elem:
            author_elem = soup.select_one('.scholUnivAuthors a')
        if author_elem:
            paper_data["Author"] = author_elem.get_text(strip=True)

        # 提取摘要
        abstract_elem = soup.find('div', class_='abstractContainer')
        if abstract_elem:
            abstract_text = abstract_elem.find('div', class_='abstract')
            if abstract_text:
                paper_data["Abstract"] = abstract_text.get_text(strip=True)

        # 提取学位类型 - 根据第二张图片中的信息
        degree_elem = soup.find('div', string=re.compile(r'学位|degree', re.IGNORECASE))
        if degree_elem:
            degree_text = degree_elem.find_next_sibling('div')
            if degree_text:
                paper_data["degree type"] = degree_text.get_text(strip=True)

        # 提取文档URL - 根据第二张图片中的信息
        doc_url_elem = soup.find('a', href=re.compile(r'/docview/'))
        if doc_url_elem:
            paper_data["Document URL"] = urljoin(PROQUEST_BASE_URL, doc_url_elem['href'])

        # 提取索引信息 - 更通用的提取方法
        indexing_rows = soup.select('.display_record_indexing_row')

        for row in indexing_rows:
            field_name_elem = row.select_one('.display_record_indexing_fieldname')
            data_elem = row.select_one('.display_record_indexing_data')

            if field_name_elem and data_elem:
                field_name = field_name_elem.get_text(strip=True)
                data_text = data_elem.get_text(strip=True, separator='\n')

                # 更灵活的字段匹配
                if "advisor" in field_name.lower() or "导师" in field_name:
                    paper_data["advisor"] = data_text
                elif "university" in field_name.lower() or "大学" in field_name:
                    if "location" in field_name.lower() or "位置" in field_name:
                        # 修改：提取国家信息和城市信息
                        if '--' in data_text:
                            parts = data_text.split('--', 1)
                            paper_data["University location-country"] = parts[0].strip()
                            if len(parts) > 1:
                                paper_data["University location-city"] = parts[1].strip()
                        elif '-' in data_text:
                            parts = data_text.split('-', 1)
                            paper_data["University location-country"] = parts[0].strip()
                            if len(parts) > 1:
                                paper_data["University location-city"] = parts[1].strip()
                        else:
                            # 如果没有分隔符，尝试匹配国家名称
                            country_match = re.search(r'\b[A-Z][a-z]+(?: [A-Z][a-z]+)*\b', data_text)
                            if country_match:
                                paper_data["University location-country"] = country_match.group(0)
                    else:
                        paper_data["University/institute"] = data_text
                elif "department" in field_name.lower() or "部门" in field_name:
                    paper_data["Department"] = data_text
                # 移除了language字段的处理
                elif "publication year" in field_name.lower() or "出版年份" in field_name:
                    paper_data["Publication Year"] = data_text
                # 移除了degree date字段的处理
                elif "degree" in field_name.lower() or "学位" == field_name:
                    paper_data["degree type"] = data_text
                elif "subject" in field_name.lower() or "主题" in field_name:
                    paper_data["subject"] = [s.strip() for s in data_text.split(';') if s.strip()]
                elif "classification" in field_name.lower() or "分类" in field_name:
                    paper_data["Classification"] = [c.strip() for c in data_text.split('\n') if c.strip()]
                elif "keyword" in field_name.lower() or "关键字" in field_name or "标识符" in field_name:
                    paper_data["Identifier / keyword"] = [k.strip() for k in data_text.split(';') if k.strip()]
                elif "committee" in field_name.lower() or "委员会" in field_name:
                    paper_data["Committee member"] = [m.strip() for m in data_text.split(';') if m.strip()]

        # 清理数据
        for key in paper_data:
            if isinstance(paper_data[key], str):
                paper_data[key] = paper_data[key].replace('\n', ' ').replace('\r', '').strip()

        return paper_data

    except ValueError as e:
        # 重新抛出标题为空的异常
        raise e
    except Exception as e:
        print(f"解析论文 {paper_id} 详情页时出错: {str(e)}")
        # 返回部分数据
        return paper_data


def save_paper_details(details, keyword, page):
    """保存论文详情到对应页面的JSON文件"""
    # 创建关键词文件夹
    keyword_dir = os.path.join("data/data_details", keyword.replace(' ', '_'))
    os.makedirs(keyword_dir, exist_ok=True)

    # 创建文件名
    filename = os.path.join(keyword_dir, f"{keyword.replace(' ', '_')}{page}.json")

    # 如果文件已存在，则读取现有数据
    existing_data = []
    if os.path.exists(filename):
        try:
            with open(filename, 'r', encoding='utf-8') as f:
                existing_data = json.load(f)
        except Exception as e:
            print(f"读取现有文件时出错: {str(e)}")

    # 添加新数据
    existing_data.append(details)

    # 保存为极速
    with open(filename, 'w', encoding='utf-8') as f:
        json.dump(existing_data, f, ensure_ascii=False, indent=2)

    print(f"已保存论文详情到 {filename}")
    return True


def crawl_paper_details(keyword):
    """爬取所有论文的详情信息"""
    # 查找关键词对应的ID文件
    keyword_dir = os.path.join("data/data_id", keyword.replace(' ', '_'))
    if not os.path.exists(keyword_dir):
        print(f"没有找到关键词 {keyword} 的ID文件")
        return

    # 按页码排序极速
    id_files = sorted(
        [f for f in os.listdir(keyword_dir) if f.endswith('.json')],
        key=lambda x: int(re.search(r'\d+', x).group()) if re.search(r'\d+', x) else 0
    )

    if not id_files:
        print(f"没有找到关键词 {keyword} 的论文ID文件")
        return

    # 从当前页码开始处理
    current_page = crawling_status["current_page"]

    # 预先计算总论文数 - 修复生成器表达式问题
    total_papers = 0
    for page_file in id_files:
        filepath = os.path.join(keyword_dir, page_file)
        try:
            with open(filepath, 'r', encoding='utf-8') as f:
                data = json.load(f)
                total_papers += len(data)
        except Exception as e:
            print(f"读取文件 {filepath} 时出错: {str(e)}")

    print(f"总共需要爬取 {total_papers} 篇论文")

    # 处理每一页
    for page_file in id_files:
        # 提取页码
        page_match = re.search(r'\d+', page_file)
        if not page_match:
            continue

        page_num = int(page_match.group())

        # 跳过已处理的页面
        if page_num < current_page:
            continue

        print(f"正在处理第 {page_num} 页")

        # 读取该页的所有论文ID
        filepath = os.path.join(keyword_dir, page_file)
        try:
            with open(filepath, 'r', encoding='utf-8') as f:
                page_data = json.load(f)
                paper_ids = [paper["id"] for paper in page_data if "id" in paper]
        except Exception as e:
            print(f"读取文件 {filepath} 时出错: {str(e)}")
            continue

        if not paper_ids:
            print(f"第 {page_num} 页没有找到论文ID")
            continue

        # 检查已爬取的论文，避免重复爬取
        details_file = os.path.join("data/data_details", keyword.replace(' ', '_'),
                                    f"{keyword.replace(' ', '_')}{page_num}.json")
        crawled_ids = set()

        if os.path.exists(details_file):
            try:
                with open(details_file, 'r', encoding='utf-8') as f:
                    existing_details = json.load(f)
                    for detail in existing_details:
                        if "Document URL" in detail:
                            doc_id_match = re.search(r'/docview/(\d+)/', detail["Document URL"])
                            if doc_id_match:
                                crawled_ids.add(doc_id_match.group(1))
            except Exception as e:
                print(f"读取详情文件时出错: {str(e)}")

        # 过滤掉已爬取的论文ID
        remaining_ids = [pid for pid in paper_ids if pid not in crawled_ids]

        if not remaining_ids:
            print(f"第 {page_num} 页的所有论文已爬取完成")
            crawling_status["current_page"] = page_num + 1
            continue

        print(f"第 {page_num} 页有 {len(paper_ids)} 篇论文，其中 {len(remaining_ids)} 篇需要爬取")

        # 爬取该页的论文详情
        for i, paper_id in enumerate(remaining_ids):
            print(f"正在爬取第 {page_num} 页的第 {i + 1}/{len(remaining_ids)} 篇论文 (ID: {paper_id})")

            # 获取详情页HTML
            html_content, error = make_detail_request(paper_id)
            if error:
                print(f"获取论文 {paper_id} 详情失败: {error}")
                continue

            # 保存HTML用于调试
            debug_dir = os.path.join("debug_html", keyword.replace(' ', '_'))
            os.makedirs(debug_dir, exist_ok=True)
            debug_path = os.path.join(debug_dir, f"{paper_id}.html")
            with open(debug_path, 'w', encoding='utf-8') as f:
                f.write(html_content)

            # 解析详情页
            try:
                detail_data = parse_detail_page(html_content, paper_id)
            except ValueError as e:
                # 捕获标题为空的异常并终止程序
                raise Exception(f"爬取到空标题数据: {str(e)}")

            # 立即保存论文详情到对应页面文件
            save_paper_details(detail_data, keyword, page_num)

            # 更新状态
            crawling_status["crawled_count"] += 1
            crawling_status["last_save_time"] = time.time()

            # 显示进度 - 使用预先计算的总论文数
            if total_papers > 0:
                progress = (crawling_status["crawled_count"] / total_papers) * 100
                print(f"总进度: {progress:.2f}% ({crawling_status['crawled_count']}/{total_papers})")
            else:
                print(f"已爬取 {crawling_status['crawled_count']} 极论文")

            # 添加随机延迟，避免请求过于频繁
            delay = random.uniform(2, 5)
            time.sleep(delay)

        # 更新当前页码 - 移动到循环外部
        crawling_status["current_page"] = page_num + 1


def main():
    try:
        # 爬取论文详情
        crawl_paper_details(KEYWORD)
        print("\n爬取完成！所有论文详情已保存")

    except KeyboardInterrupt:
        print("\n用户中断爬取过程")
        print(f"已保存部分数据 ({crawling_status['crawled_count']} 篇论文详情)")
    except Exception as e:
        print(f"\n爬取过程中出现错误: {str(e)}")
        print(f"已保存部分数据({crawling_status['crawled_count']}篇论文详情)")
        # 重新抛出异常以确保程序终止
        raise


if __name__ == "__main__":
    main()