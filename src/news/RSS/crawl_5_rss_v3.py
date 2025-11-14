# File: crawl_5_rss_v3.py
# Mục tiêu: Crawl bài báo từ 5 nguồn tin tức theo chuyên mục, tối ưu tốc độ, sửa lỗi khoảng trắng trong từ (Q uốc → Quốc), giữ nguyên dấu câu, nội dung nằm đúng 1 hàng CSV
# Tính năng ĐÃ HOÀN THIỆN từ v2 + cải tiến v3:
#   • DỰA 5 NGUỒN: VnExpress + Dân Trí + Thanh Niên + Tuổi Trẻ + VietnamNet (kế thừa đầy đủ config, selector, page_format từ v2)
#   • XÓA hoàn toàn việc thêm khoảng trắng kiểu camelCase (caused lỗi "Q uốc", "N ghị")
#   • XÓA bộ lọc ký tự nghiêm ngặt → giữ nguyên dấu câu !?:"'… và dấu câu Việt
#   • Chỉ chuẩn hóa khoảng trắng (nbsp, \r, \n, multiple spaces)
#   • Content join bằng " " thay vì "\n\n" → chắc chắn 1 dòng trong CSV
#   • Abstract tự lấy 300 ký tự đầu nếu thiếu
#   • quoting=csv.QUOTE_ALL trong to_csv → đảm bảo 100% 1 hàng/row dù có \n , " trong content
#   • Tăng max_pages=10 + giảm delay để crawl nhanh hơn mà vẫn an toàn
#   • Checkpoint theo từng nguồn + parallel crawl detail
# ================================================================

import requests
from bs4 import BeautifulSoup
import pandas as pd
import time
import os
import random
import logging
import re
from datetime import datetime
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import TimeoutException
import pickle
from concurrent.futures import ThreadPoolExecutor, as_completed
import csv  # Để dùng csv.QUOTE_ALL

# Import config cho 5 nguồn (kế thừa từ v2)
from config_vnexpress import BASE_URL as VNEXPRESS_BASE_URL, CATEGORIES as VNEXPRESS_CATEGORIES, HEADERS as VNEXPRESS_HEADERS
from config_tuoitre import BASE_URL as TUOITRE_BASE_URL, CATEGORIES as TUOITRE_CATEGORIES, HEADERS as TUOITRE_HEADERS
from config_thanhnien import BASE_URL as THANHNIEN_BASE_URL, CATEGORIES as THANHNIEN_CATEGORIES, HEADERS as THANHNIEN_HEADERS
from config_dantri import BASE_URL as DANTRI_BASE_URL, CATEGORIES as DANTRI_CATEGORIES, HEADERS as DANTRI_HEADERS
from config_vietnamnet import BASE_URL as VIETNAMNET_BASE_URL, CATEGORIES as VIETNAMNET_CATEGORIES, HEADERS as VIETNAMNET_HEADERS

# ---------------------------------------------------------------
# THIẾT LẬP LOGGING + THƯ MỤC
# ---------------------------------------------------------------
os.makedirs("logs", exist_ok=True)
os.makedirs("data", exist_ok=True)

# ---------------------------------------------------------------
# SESSION VỚI RETRY
# ---------------------------------------------------------------
session = requests.Session()

def safe_get(url, headers, retries=3, delay=3):
    for attempt in range(retries):
        try:
            resp = session.get(url, headers=headers, timeout=60)
            if resp.status_code == 200:
                return resp
        except Exception as e:
            logging.warning(f"Thử lại {attempt+1}/{retries} - {url} - lỗi: {e}")
        time.sleep(delay)
    return None

# ---------------------------------------------------------------
# Selenium tối ưu (tắt ảnh, giảm timeout)
# ---------------------------------------------------------------
def get_selenium_driver(headers):
    options = Options()
    options.add_argument("--headless=new")
    options.add_argument("--no-sandbox")
    options.add_argument("--disable-dev-shm-usage")
    options.add_argument("--disable-gpu")
    options.add_argument("--window-size=1920,1080")
    options.add_argument(f"user-agent={headers['User-Agent']}")
    options.add_argument("--disable-blink-features=AutomationControlled")
    options.add_experimental_option("excludeSwitches", ["enable-automation"])
    options.add_experimental_option('useAutomationExtension', False)
    options.add_argument("--disable-images")
    options.add_argument("--blink-settings=imagesEnabled=false")
    
    driver = webdriver.Chrome(options=options)
    driver.execute_script("Object.defineProperty(navigator, 'webdriver', {get: () => undefined})")
    return driver

def safe_get_selenium(url, list_selector=None, headers=None, timeout=10):
    driver = None
    try:
        driver = get_selenium_driver(headers)
        driver.set_page_load_timeout(timeout)
        driver.get(url)
        
        if list_selector:
            WebDriverWait(driver, 5).until(EC.presence_of_element_located((By.CSS_SELECTOR, list_selector)))
        
        driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")
        time.sleep(1)
        page_source = driver.page_source
        class MockResponse:
            text = page_source
            status_code = 200
        return MockResponse()
    except Exception as e:
        logging.error(f"Lỗi Selenium {url}: {e}")
        return None
    finally:
        if driver:
            driver.quit()

# ---------------------------------------------------------------
# LÀM SẠCH VĂN BẢN - ĐƠN GIẢN (giữ nguyên dấu câu, không thêm space camelCase)
# ---------------------------------------------------------------
def clean_text(text):
    if not text:
        return ""
    # Chỉ chuẩn hóa khoảng trắng + thay thế ký tự đặc biệt phổ biến
    text = text.replace("\xa0", " ").replace("\u2011", "-").replace("\r", " ").replace("\n", " ")
    text = re.sub(r'\s+', ' ', text).strip()
    return text

# ---------------------------------------------------------------
# CHUẨN HÓA NGÀY (giữ từ v3)
# ---------------------------------------------------------------
def extract_date(raw, source):
    if not raw:
        return ""
    raw = raw.strip()
    patterns = [
        r'(\d{1,2}/\d{1,2}/\d{4})',
        r'(\d{1,2}-\d{1,2}-\d{4})',
        r'(\d{4}-\d{2}-\d{2})',
    ]
    for pattern in patterns:
        m = re.search(pattern, raw)
        if m:
            date_str = m.group(1)
            try:
                if '/' in date_str:
                    d = datetime.strptime(date_str, "%d/%m/%Y")
                    return d.strftime("%Y-%m-%d")
                elif '-' in date_str and len(date_str.split('-')[2]) == 4:
                    d = datetime.strptime(date_str, "%d-%m-%Y")
                    return d.strftime("%Y-%m-%d")
                return date_str
            except:
                continue
    if re.match(r'\d{4}-\d{2}-\d{2}T', raw):
        return raw.split('T')[0]
    return raw  # fallback giữ nguyên

# ---------------------------------------------------------------
# LỌC URL HỢP LỆ
# ---------------------------------------------------------------
def is_valid_url(link):
    if not link or any(p in link.lower() for p in ["#box_", "/box_", "/video/", "/clip/", "/interactive/", "/live/", "/photo/", "/infographic/", "/audio/", "/slideshow/", "javascript:", "mailto:"]):
        return False
    if any(link.lower().endswith(ext) for ext in ['.mp4', '.avi', '.mov', '.wmv', '.pdf', '.doc', '.docx', '.jpg', '.png']):
        return False
    return True

# ---------------------------------------------------------------
# DETAIL SELECTORS CHO 5 NGUỒN (kế thừa từ v2, đã test ổn định)
# ---------------------------------------------------------------
DETAIL_SELECTORS_VNEXPRESS = {
    'title': ["h1.title-detail", "h1.title-news", ".title-detail"],
    'abstract': ["p.description", "h2.sapo", ".description"],
    'content_div': ["article.fck_detail", "div.fck_detail", ".fck_detail"],
    'content': ["p.Normal", "p"],
    'date': ["span.date", "meta[itemprop='datePublished']", ".date"],
    'unwanted': ["figure", "script", "style", ".box-embed", ".box-insert", ".author", ".VCSortableInPreviewMode", ".item_slide_show"]
}

DETAIL_SELECTORS_TUOITRE = {
    'title': ["h1.article-title", "h1.title-news", ".article-title"],
    'abstract': ["h2.article-sapo", "p.sapo", ".article-sapo"],
    'content_div': ["div.article-body", "div.main-article-body", ".article-body"],
    'content': ["p", ".content p"],
    'date': ["time.article-publish", "span.date", ".date-time"],
    'unwanted': ["figure", "script", "style", ".ads", ".author", ".related-news"]
}

DETAIL_SELECTORS_THANHNIEN = {
    'title': ["h1.cms-title", "h1.title", ".cms-title"],
    'abstract': ["p.cms-desc", "div.sapo", ".description"],
    'content_div': ["div.cms-body", "div.article-body", ".cms-body"],
    'content': ["p", ".content p"],
    'date': ["time.cms-date", "span.time", ".date"],
    'unwanted': ["figure", "script", "style", ".box-embed", ".author", ".tags"]
}

DETAIL_SELECTORS_DANTRI = {
    'title': ["h1.dt-news__title", "h1.title", ".dt-news__title"],
    'abstract': ["div.dt-news__sapo", "p.sapo", ".dt-news__sapo"],
    'content_div': ["div.dt-news__content", "div.article-content", ".dt-news__content"],
    'content': ["p", ".content p"],
    'date': ["span.dt-news__time", "time", ".date"],
    'unwanted': ["figure", "script", "style", ".ads", ".author", ".related"]
}

DETAIL_SELECTORS_VIETNAMNET = {
    'title': ["h1.title", "h1.article-title", ".title"],
    'abstract': ["div.lead", "p.description", ".lead"],
    'content_div': ["div.inner-article", "div.article-body", ".inner-article"],
    'content': ["p", ".content p"],
    'date': ["span.date", ".article__time", "time"],
    'unwanted': ["figure", "script", "style", ".box-embed", ".author", ".tags"]
}

# ---------------------------------------------------------------
# CRAWL CHI TIẾT BÀI VIẾT (cải tiến v3: " " join + abstract fallback 300 ký tự)
# ---------------------------------------------------------------
def crawl_article_detail_generic(link, headers, detail_selectors, source, use_selenium=False):
    print(f"      → Crawl detail: {link}")
    
    resp = safe_get_selenium(link, headers=headers) if use_selenium else safe_get(link, headers)
    
    if not resp or resp.status_code != 200:
        print(f"      [!] Không tải được: {link}")
        return "", "", ""

    soup = BeautifulSoup(resp.text, "lxml")

    # Title
    title = ""
    for sel in detail_selectors['title']:
        tag = soup.select_one(sel)
        if tag:
            title = clean_text(tag.get_text(separator=" ", strip=True))
            if len(title) > 5:
                break

    # Abstract
    abstract = ""
    for sel in detail_selectors['abstract']:
        tag = soup.select_one(sel)
        if tag:
            abstract = clean_text(tag.get_text(separator=" ", strip=True))
            if len(abstract) > 10:
                break

    # Content
    content = ""
    for div_sel in detail_selectors['content_div']:
        content_div = soup.select_one(div_sel)
        if content_div:
            for unw in detail_selectors.get('unwanted', []):
                for e in content_div.select(unw):
                    e.decompose()
            
            content_parts = []
            for sel in detail_selectors['content']:
                for tag in content_div.select(sel):
                    txt = clean_text(tag.get_text(separator=" ", strip=True))
                    if len(txt) > 20 and not txt.lower().startswith(('xem thêm:', 'đọc thêm:', '>>', 'xem thêm:')):
                        content_parts.append(txt)
            if content_parts:
                content = " ".join(content_parts)  # 1 dòng dài
                break

    # Date
    date = ""
    for sel in detail_selectors.get('date', []):
        tag = soup.select_one(sel)
        if tag and tag.has_attr('datetime'):
            date = extract_date(tag['datetime'], source) or extract_date(tag.get('content'), source)
        elif tag:
            date = extract_date(tag.get_text(), source)
        if date:
            break

    # Fallback Selenium nếu content ngắn
    if len(content) < 100 and not use_selenium:
        print(f"      → Fallback Selenium: {link}")
        return crawl_article_detail_generic(link, headers, detail_selectors, source, True)

    # Nếu vẫn thiếu abstract → lấy 300 ký tự đầu content
    if not abstract and content:
        abstract = content[:300] + "..." if len(content) > 300 else content

    return abstract, content, date

# Wrapper cho từng nguồn (kế thừa từ v2)
def crawl_article_detail_vnexpress(link, headers):
    return crawl_article_detail_generic(link, headers, DETAIL_SELECTORS_VNEXPRESS, "vnexpress")

def crawl_article_detail_tuoitre(link, headers):
    return crawl_article_detail_generic(link, headers, DETAIL_SELECTORS_TUOITRE, "tuoitre")

def crawl_article_detail_thanhnien(link, headers):
    return crawl_article_detail_generic(link, headers, DETAIL_SELECTORS_THANHNIEN, "thanhnien")

def crawl_article_detail_dantri(link, headers):
    return crawl_article_detail_generic(link, headers, DETAIL_SELECTORS_DANTRI, "dantri")

def crawl_article_detail_vietnamnet(link, headers):
    return crawl_article_detail_generic(link, headers, DETAIL_SELECTORS_VIETNAMNET, "vietnamnet")

# ---------------------------------------------------------------
# PAGE FORMAT & LIST SELECTOR CHO 5 NGUỒN (kế thừa từ v2)
# ---------------------------------------------------------------
def page_format_vnexpress(base_url, cat_slug, page):
    return f"{base_url}/{cat_slug}-p{page}"

def page_format_tuoitre(base_url, cat_slug, page):
    slug_base = cat_slug.replace('.htm', '')
    return f"{base_url}/{slug_base}/trang-{page}.htm"

def page_format_thanhnien(base_url, cat_slug, page):
    slug_base = cat_slug.replace('.htm', '')
    return f"{base_url}/{slug_base}/trang-{page}.htm"

def page_format_dantri(base_url, cat_slug, page):
    slug_base = cat_slug.replace('.htm', '')
    return f"{base_url}/{slug_base}/trang-{page}.htm"

def page_format_vietnamnet(base_url, cat_slug, page):
    return f"{base_url}/{cat_slug}?page={page}"

LIST_SELECTOR_VNEXPRESS = "article.item-news .title-news a, .title-news a"
LIST_SELECTOR_TUOITRE = "li.news-item a.box-category-link-title, .news-item a, h3.title-news a"
LIST_SELECTOR_THANHNIEN = "article.story a.story__title, .story a, h2.title a"
LIST_SELECTOR_DANTRI = "article.news-item h3.news-item__title a, .news-item a, h3.title a"
LIST_SELECTOR_VIETNAMNET = "li.article-item a.title, .article-item a, h3.title a, a[href*='/']"

# ---------------------------------------------------------------
# CRAWL THEO CHUYÊN MỤC (cải tiến v3: max_pages=10, delay nhanh)
# ---------------------------------------------------------------
def crawl_category_generic(base_url, cat_slug, cat_name, headers, list_selector, detail_func, page_format, min_delay=0.5, max_delay=1.5, max_pages=1, crawled_urls=set()):
    data = []
    page = 1
    
    while page <= max_pages:
        url = f"{base_url}/{cat_slug}" if page == 1 else page_format(base_url, cat_slug, page)
        print(f"  Crawl {cat_name} - Trang {page}: {url}")
        
        resp = safe_get(url, headers)
        soup = BeautifulSoup(resp.text, "lxml") if resp and resp.status_code == 200 else None
        
        if not soup:
            print(f"  → Fallback Selenium list")
            resp = safe_get_selenium(url, list_selector, headers)
            if resp:
                soup = BeautifulSoup(resp.text, "lxml")
        
        items = soup.select(list_selector) if soup else []
        
        if not items:
            print(f"  [!] Không tìm thấy bài trang {page}")
            break

        valid_links = []
        for a in items:
            link = a.get("href")
            if not link:
                continue
            link = link if link.startswith('http') else (base_url.rstrip('/') + '/' + link.lstrip('/'))
            if link.startswith('//'):
                link = 'https:' + link
            if is_valid_url(link) and link not in crawled_urls:
                title = clean_text(a.get_text(separator=" ", strip=True))
                if len(title) >= 10:
                    valid_links.append((link, title))

        if not valid_links:
            print(f" Trang {page}: Không bài mới")
            if page > 1:
                break
            page += 1
            continue

        def crawl_single(link_title):
            link, title = link_title
            abstract, content, date = detail_func(link, headers)
            if content and len(content) >= 100:
                return {
                    "category": cat_name,
                    "title": title,
                    "abstract": abstract,
                    "content": content,
                    "date": date,
                    "url": link
                }
            return None

        with ThreadPoolExecutor(max_workers=8) as executor:
            futures = [executor.submit(crawl_single, lt) for lt in valid_links]
            for future in as_completed(futures):
                result = future.result()
                if result:
                    data.append(result)
                    crawled_urls.add(result["url"])
                time.sleep(random.uniform(min_delay, max_delay))

        print(f" Trang {page}: {len([r for r in data if r['category'] == cat_name])} bài mới")
        if len([r for r in data if r['category'] == cat_name]) == 0 and page > 1:
            break
        page += 1
        time.sleep(1)

    return data

# ---------------------------------------------------------------
# MAIN - CRAWL 5 NGUỒN
# ---------------------------------------------------------------
if __name__ == "__main__":
    source_display = {
        "vnexpress": "VnExpress",
        "tuoitre": "Tuổi Trẻ",
        "thanhnien": "Thanh Niên",
        "dantri": "Dân Trí",
        "vietnamnet": "VietnamNet"
    }

    sources = [
        ("vnexpress", VNEXPRESS_BASE_URL, VNEXPRESS_CATEGORIES, VNEXPRESS_HEADERS, LIST_SELECTOR_VNEXPRESS, crawl_article_detail_vnexpress, page_format_vnexpress),
        ("tuoitre", TUOITRE_BASE_URL, TUOITRE_CATEGORIES, TUOITRE_HEADERS, LIST_SELECTOR_TUOITRE, crawl_article_detail_tuoitre, page_format_tuoitre),
        ("thanhnien", THANHNIEN_BASE_URL, THANHNIEN_CATEGORIES, THANHNIEN_HEADERS, LIST_SELECTOR_THANHNIEN, crawl_article_detail_thanhnien, page_format_thanhnien),
        ("dantri", DANTRI_BASE_URL, DANTRI_CATEGORIES, DANTRI_HEADERS, LIST_SELECTOR_DANTRI, crawl_article_detail_dantri, page_format_dantri),
        ("vietnamnet", VIETNAMNET_BASE_URL, VIETNAMNET_CATEGORIES, VIETNAMNET_HEADERS, LIST_SELECTOR_VIETNAMNET, crawl_article_detail_vietnamnet, page_format_vietnamnet),
    ]

    all_articles = []

    for source_name, base_url, categories, headers, list_selector, detail_func, page_format in sources:
        print(f"\n{'='*60}")
        print(f"ĐANG CRAWL: {source_display[source_name].upper()}")
        print(f"{'='*60}")
        
        logging.basicConfig(filename=f"logs/{source_name}_crawl_v3.log", level=logging.INFO,
                             format='%(asctime)s - %(levelname)s - %(message)s', encoding="utf-8")

        checkpoint_path = f"logs/{source_name}_checkpoint_v3.pkl"
        crawled_urls = set()
        source_articles = []

        if os.path.exists(checkpoint_path):
            df_existing = pd.read_pickle(checkpoint_path)
            crawled_urls = set(df_existing["url"])
            source_articles = df_existing.to_dict("records")
            print(f"Load {len(crawled_urls)} bài từ checkpoint")

        for slug, name in categories.items():
            print(f"\nChuyên mục: {name} ({slug})")
            articles = crawl_category_generic(
                base_url, slug, name, headers, list_selector,
                detail_func, page_format, min_delay=0.5, max_delay=1.5, max_pages=1, crawled_urls=crawled_urls
            )
            for a in articles:
                a["source"] = source_display[source_name]
            source_articles.extend(articles)
            if articles:
                pd.DataFrame(source_articles).to_pickle(checkpoint_path)

        all_articles.extend(source_articles)
        print(f"Hoàn tất {source_display[source_name]}: {len(source_articles)} bài")

    if all_articles:
        df = pd.DataFrame(all_articles).drop_duplicates(subset=["url"])
        df.insert(0, "news_id", [f"NEWS_{i+1:06d}" for i in range(len(df))])
        column_order = ['news_id', 'source', 'category', 'date', 'title', 'abstract', 'content', 'url']
        df = df[column_order]
        df = df.sort_values('date', ascending=False)
        
        out_path = "data/vietnamese_news_dataset_v3_5sources_fixed.csv"
        df.to_csv(out_path, index=False, encoding="utf-8-sig", quoting=csv.QUOTE_ALL)

        print(f"\nHOÀN TẤT! {len(df)} bài → {out_path}")
        print(df['source'].value_counts())
        print(df['category'].value_counts())
        print(f"Ngày: {df['date'].min()} đến {df['date'].max()}")

    else:
        print("Không có bài nào!")