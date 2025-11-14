# File: crawl_5_rss_v5_fixed.py
# CẢI TIẾN V5: Fix chi tiết cho Thanh Niên, Dân Trí, VietnamNet - Tối ưu selector và xử lý lỗi

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
import csv

# Import config cho 5 nguồn
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
# SESSION VỚI RETRY - CẢI TIẾN
# ---------------------------------------------------------------
session = requests.Session()

def safe_get(url, headers, retries=3, delay=2):
    for attempt in range(retries):
        try:
            resp = session.get(url, headers=headers, timeout=30)
            if resp.status_code == 200:
                return resp
            elif resp.status_code == 403:
                print(f"      [!] Lỗi 403 - Thử lại với User-Agent khác")
                headers = headers.copy()
                headers['User-Agent'] = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
        except Exception as e:
            logging.warning(f"Thử lại {attempt+1}/{retries} - {url} - lỗi: {e}")
        time.sleep(delay)
    return None

# ---------------------------------------------------------------
# Selenium tối ưu cho từng site
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
    
    # Không tắt ảnh để tránh nghi ngờ
    options.add_argument("--disable-images")
    
    driver = webdriver.Chrome(options=options)
    driver.execute_script("Object.defineProperty(navigator, 'webdriver', {get: () => undefined})")
    return driver

def safe_get_selenium(url, headers=None, timeout=20):
    driver = None
    try:
        driver = get_selenium_driver(headers)
        driver.set_page_load_timeout(timeout)
        driver.get(url)
        
        # Chờ trang load
        time.sleep(3)
        driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")
        time.sleep(2)
        
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
# LÀM SẠCH VĂN BẢN
# ---------------------------------------------------------------
def clean_text(text):
    if not text:
        return ""
    text = text.replace("\xa0", " ").replace("\u2011", "-").replace("\r", " ").replace("\n", " ")
    text = re.sub(r'\s+', ' ', text).strip()
    return text

# ---------------------------------------------------------------
# CHUẨN HÓA NGÀY
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
    return raw

# ---------------------------------------------------------------
# LỌC URL HỢP LỆ - MỞ RỘNG
# ---------------------------------------------------------------
def is_valid_url(link):
    if not link:
        return False
        
    # Loại bỏ URL không phải bài báo
    exclude_patterns = [
        "#box_", "/box_", "/video/", "/clip/", "/interactive/", 
        "/live/", "/photo/", "/infographic/", "/audio/", "/slideshow/", 
        "javascript:", "mailto:", "policy", "thong-tin", "lien-he",
        "rss", "sitemap", "tag", "category"
    ]
    
    if any(p in link.lower() for p in exclude_patterns):
        return False
        
    # Loại bỏ file
    if any(link.lower().endswith(ext) for ext in ['.mp4', '.avi', '.mov', '.wmv', '.pdf', '.doc', '.docx', '.jpg', '.png']):
        return False
        
    # Phải có chứa từ khóa bài báo
    if any(keyword in link for keyword in ['.html', '.htm', '/tin-', '-2025']):
        return True
        
    return False

# ---------------------------------------------------------------
# DETAIL SELECTORS CẬP NHẬT CHI TIẾT - FIX LỖI
# ---------------------------------------------------------------
DETAIL_SELECTORS_VNEXPRESS = {
    'title': ["h1.title-detail", "h1.title-news"],
    'abstract': ["p.description", "h2.sapo"],
    'content_div': ["article.fck_detail", "div.fck_detail"],
    'content': ["p.Normal", "p"],
    'date': ["span.date", "meta[itemprop='datePublished']"],
    'unwanted': ["figure", "script", "style", ".box-embed", ".box-insert"]
}

DETAIL_SELECTORS_TUOITRE = {
    'title': ["h1.detail-title", "h1.article-title"],
    'abstract': ["h2.detail-sapo", "h2.sapo"],
    'content_div': ["div.detail-content", "div.article-content"],
    'content': ["p"],
    'date': ["div.detail-time", "time.publish-date"],
    'unwanted': ["figure", "script", "style", ".ads", ".related-news"]
}

# THANH NIÊN - SELECTOR MỚI
DETAIL_SELECTORS_THANHNIEN = {
    'title': ["h1.detail-title", "h1.title", ".detail__title"],
    'abstract': ["div.detail-sapo", "h2.sapo", ".detail__sapo"],
    'content_div': ["div.detail-cmain", "div.detail-content", ".detail__content"],
    'content': ["p"],
    'date': ["div.detail-time", "time", ".detail__meta"],
    'unwanted': ["figure", "script", "style", ".box-embed", ".ads"]
}

# DÂN TRÍ - SELECTOR MỚI
DETAIL_SELECTORS_DANTRI = {
    'title': ["h1.dt-news__title", "h1.title-news"],
    'abstract': ["div.dt-news__sapo", "h2.sapo"],
    'content_div': ["div.dt-news__content", "div.article-content"],
    'content': ["p"],
    'date': ["span.dt-news__time", "time.dt-news__time"],
    'unwanted': ["figure", "script", "style", ".ads", ".related-news"]
}

# VIETNAMNET - SELECTOR MỚI
DETAIL_SELECTORS_VIETNAMNET = {
    'title': ["h1.content-title", "h1.title", ".main-title"],
    'abstract': ["div.article-lead", "h2.sapo", ".lead"],
    'content_div': ["div.article-content", "div.maincontent", ".maincontent"],
    'content': ["p"],
    'date': ["span.article-date", ".article-time", "time"],
    'unwanted': ["figure", "script", "style", ".box-embed", ".ads"]
}

# ---------------------------------------------------------------
# CRAWL CHI TIẾT BÀI VIẾT - CẢI TIẾN XỬ LÝ LỖI
# ---------------------------------------------------------------
def crawl_article_detail_generic(link, headers, detail_selectors, source, use_selenium=False):
    print(f"      → Crawl detail: {link}")
    
    # Luôn dùng Selenium cho các site khó
    if source in ["thanhnien", "dantri", "vietnamnet"]:
        use_selenium = True
    
    if use_selenium:
        resp = safe_get_selenium(link, headers)
    else:
        resp = safe_get(link, headers)
    
    if not resp or resp.status_code != 200:
        print(f"      [!] Không tải được: {link}")
        return "", "", ""

    soup = BeautifulSoup(resp.text, "lxml")

    # Title - tìm kiếm mạnh mẽ hơn
    title = ""
    for sel in detail_selectors['title']:
        tag = soup.select_one(sel)
        if tag:
            title = clean_text(tag.get_text(separator=" ", strip=True))
            if len(title) > 10:  # Tiêu đề phải đủ dài
                break
    
    # Fallback title từ thẻ title
    if not title or len(title) < 10:
        title_tag = soup.find('title')
        if title_tag:
            title = clean_text(title_tag.get_text())

    # Abstract
    abstract = ""
    for sel in detail_selectors['abstract']:
        tag = soup.select_one(sel)
        if tag:
            abstract = clean_text(tag.get_text(separator=" ", strip=True))
            if len(abstract) > 20:
                break

    # Content - phương pháp mới: tìm tất cả p và lọc
    content = ""
    content_parts = []
    
    # Thử các container chính
    for div_sel in detail_selectors['content_div']:
        content_div = soup.select_one(div_sel)
        if content_div:
            # Xóa element không mong muốn
            for unw in detail_selectors.get('unwanted', []):
                for e in content_div.select(unw):
                    e.decompose()
            
            # Tìm tất cả paragraph
            for p in content_div.find_all('p'):
                txt = clean_text(p.get_text(separator=" ", strip=True))
                if (len(txt) > 30 and 
                    not txt.lower().startswith(('xem thêm:', 'đọc thêm:', '>>', 'tag:', 'có thể bạn quan tâm')) and
                    not any(keyword in txt.lower() for keyword in ['quảng cáo', 'advertisement', 'bình luận'])):
                    content_parts.append(txt)
            
            if content_parts:
                content = " ".join(content_parts)
                break
    
    # Fallback: tìm tất cả p trong body nếu không tìm thấy container
    if not content_parts:
        body = soup.find('body')
        if body:
            for p in body.find_all('p'):
                txt = clean_text(p.get_text(separator=" ", strip=True))
                if len(txt) > 50 and len(txt) < 2000:  # Đoạn văn hợp lý
                    if not any(keyword in txt.lower() for keyword in ['quảng cáo', 'menu', 'header', 'footer']):
                        content_parts.append(txt)
            if content_parts:
                content = " ".join(content_parts[:20])  # Giới hạn 20 đoạn

    # Date
    date = ""
    for sel in detail_selectors.get('date', []):
        tag = soup.select_one(sel)
        if tag:
            if tag.has_attr('datetime'):
                date = extract_date(tag['datetime'], source)
            elif tag.has_attr('content'):
                date = extract_date(tag.get('content'), source)
            else:
                date = extract_date(tag.get_text(), source)
            if date:
                break
    
    # Fallback date từ URL
    if not date:
        date_match = re.search(r'(\d{4})(\d{2})(\d{2})', link)
        if date_match:
            date = f"{date_match.group(1)}-{date_match.group(2)}-{date_match.group(3)}"

    # Fallback abstract từ content
    if not abstract and content:
        abstract = content[:300] + "..." if len(content) > 300 else content

    # Kiểm tra chất lượng content
    if len(content) < 100:
        print(f"      [!] Content quá ngắn: {len(content)} ký tự")
        return "", "", ""

    return abstract, content, date

# Wrapper cho từng nguồn
def crawl_article_detail_vnexpress(link, headers):
    return crawl_article_detail_generic(link, headers, DETAIL_SELECTORS_VNEXPRESS, "vnexpress")

def crawl_article_detail_tuoitre(link, headers):
    return crawl_article_detail_generic(link, headers, DETAIL_SELECTORS_TUOITRE, "tuoitre")

def crawl_article_detail_thanhnien(link, headers):
    return crawl_article_detail_generic(link, headers, DETAIL_SELECTORS_THANHNIEN, "thanhnien", use_selenium=True)

def crawl_article_detail_dantri(link, headers):
    return crawl_article_detail_generic(link, headers, DETAIL_SELECTORS_DANTRI, "dantri", use_selenium=True)

def crawl_article_detail_vietnamnet(link, headers):
    return crawl_article_detail_generic(link, headers, DETAIL_SELECTORS_VIETNAMNET, "vietnamnet", use_selenium=True)

# ---------------------------------------------------------------
# PAGE FORMAT & LIST SELECTOR
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

# LIST SELECTOR CẬP NHẬT - TỐI ƯU CHO TỪNG SITE
LIST_SELECTOR_VNEXPRESS = "article.item-news h2.title-news a, article.item-news h3.title-news a"
LIST_SELECTOR_TUOITRE = "li.news-item h3.title-news a, article.news-item a.title-news"
LIST_SELECTOR_THANHNIEN = "article.story h2.title a, .story h2 a, .story__title a"
LIST_SELECTOR_DANTRI = "article.article-item h3.article-title a, .article-item a.article-link"
LIST_SELECTOR_VIETNAMNET = "article.ArticleItem h3.title a, .ArticleItem a.title"

# ---------------------------------------------------------------
# CRAWL THEO CHUYÊN MỤC - TỐI ƯU
# ---------------------------------------------------------------
def crawl_category_generic(base_url, cat_slug, cat_name, headers, list_selector, detail_func, page_format, min_delay=0.3, max_delay=1.0, max_pages=2, crawled_urls=set()):
    data = []
    page = 1
    
    while page <= max_pages:
        if page == 1:
            url = f"{base_url}/{cat_slug}"
        else:
            url = page_format(base_url, cat_slug, page)
            
        print(f"  Crawl {cat_name} - Trang {page}: {url}")
        
        # Luôn dùng Selenium cho các site khó
        if "thanhnien" in base_url or "dantri" in base_url:
            resp = safe_get_selenium(url, headers)
        else:
            resp = safe_get(url, headers)
            
        if not resp:
            print(f"  [!] Không tải được trang {page}")
            break

        soup = BeautifulSoup(resp.text, "lxml")
        
        if not soup:
            print(f"  [!] Không parse được HTML trang {page}")
            break

        # Tìm link bài viết - ưu tiên selector chính
        items = soup.select(list_selector)
        
        # Fallback selector
        if not items:
            fallback_selectors = [
                "a[href*='.html']",
                "article a",
                "h3 a", 
                "h2 a",
                ".title a"
            ]
            for selector in fallback_selectors:
                items = soup.select(selector)
                if items:
                    print(f"  → Dùng fallback selector: {selector}")
                    break

        if not items:
            print(f"  [!] Không tìm thấy bài trang {page}")
            break

        print(f"  Tìm thấy {len(items)} link")

        valid_links = []
        for a in items:
            link = a.get("href")
            if not link:
                continue
                
            # Chuẩn hóa URL
            if link.startswith('//'):
                link = 'https:' + link
            elif link.startswith('/'):
                link = base_url.rstrip('/') + link
            elif not link.startswith('http'):
                link = base_url.rstrip('/') + '/' + link
                
            if is_valid_url(link) and link not in crawled_urls:
                title = clean_text(a.get_text(separator=" ", strip=True))
                if len(title) >= 10:
                    valid_links.append((link, title))
                    crawled_urls.add(link)

        if not valid_links:
            print(f"  Trang {page}: Không bài mới")
            page += 1
            continue

        print(f"  Trang {page}: {len(valid_links)} bài hợp lệ")

        def crawl_single(link_title):
            link, title = link_title
            try:
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
            except Exception as e:
                print(f"      [!] Lỗi crawl {link}: {e}")
            return None

        # Crawl song song với số worker phù hợp
        max_workers = 3 if "thanhnien" in base_url or "dantri" in base_url else 5
        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            futures = [executor.submit(crawl_single, lt) for lt in valid_links]
            for future in as_completed(futures):
                result = future.result()
                if result:
                    data.append(result)
                time.sleep(random.uniform(min_delay, max_delay))

        success_count = len([r for r in data if r['category'] == cat_name])
        print(f"  Trang {page}: {success_count} bài thành công")
        
        if success_count == 0 and page > 1:
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
        
        logging.basicConfig(filename=f"logs/{source_name}_crawl_v5.log", level=logging.INFO)

        checkpoint_path = f"logs/{source_name}_checkpoint_v5.pkl"
        crawled_urls = set()
        source_articles = []

        # Load checkpoint
        if os.path.exists(checkpoint_path):
            try:
                df_existing = pd.read_pickle(checkpoint_path)
                crawled_urls = set(df_existing["url"].tolist())
                source_articles = df_existing.to_dict("records")
                print(f"Load {len(crawled_urls)} bài từ checkpoint")
            except Exception as e:
                print(f"Không đọc được checkpoint: {e}")

        for slug, name in categories.items():
            print(f"\nChuyên mục: {name} ({slug})")
            articles = crawl_category_generic(
                base_url, slug, name, headers, list_selector,
                detail_func, page_format, min_delay=0.3, max_delay=1.0, max_pages=2, crawled_urls=crawled_urls
            )
            
            for a in articles:
                a["source"] = source_display[source_name]
            source_articles.extend(articles)
            
            # Lưu checkpoint
            if articles:
                pd.DataFrame(source_articles).to_pickle(checkpoint_path)
                print(f"Đã lưu checkpoint: {len(source_articles)} bài")

        all_articles.extend(source_articles)
        print(f"Hoàn tất {source_display[source_name]}: {len(source_articles)} bài")

    # Lưu kết quả cuối cùng
    if all_articles:
        df = pd.DataFrame(all_articles).drop_duplicates(subset=["url"])
        df.insert(0, "news_id", [f"NEWS_{i+1:06d}" for i in range(len(df))])
        column_order = ['news_id', 'source', 'category', 'date', 'title', 'abstract', 'content', 'url']
        df = df[column_order]
        df = df.sort_values('date', ascending=False)
        
        out_path = "data/vietnamese_news_dataset_v5_5sources_fixed.csv"
        df.to_csv(out_path, index=False, encoding="utf-8-sig", quoting=csv.QUOTE_ALL)

        print(f"\n{'='*60}")
        print(f"HOÀN TẤT! {len(df)} bài → {out_path}")
        print(f"{'='*60}")
        print("Thống kê theo nguồn:")
        print(df['source'].value_counts())
        print("\nThống kê theo chuyên mục:")
        print(df['category'].value_counts())
        print(f"\nNgày: {df['date'].min()} đến {df['date'].max()}")

    else:
        print("Không có bài nào!")