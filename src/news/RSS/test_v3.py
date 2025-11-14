# File: test_v3_ultra_fixed.py
# CẢI TIẾN V13_V3 + FIX HOÀN TOÀN: Tắt Selenium list page cho Thanh Niên & VietnamNet
# Chỉ dùng Selenium khi cần, tăng timeout, fallback mạnh, giữ new_id

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

# -------------------------- CONFIG SOURCES --------------------------
from config_vnexpress import BASE_URL as VNEXPRESS_BASE_URL, CATEGORIES as VNEXPRESS_CATEGORIES, HEADERS as VNEXPRESS_HEADERS
from config_tuoitre import BASE_URL as TUOITRE_BASE_URL, CATEGORIES as TUOITRE_CATEGORIES, HEADERS as TUOITRE_HEADERS
from config_soha import BASE_URL as SOHA_BASE_URL, CATEGORIES as SOHA_CATEGORIES, HEADERS as SOHA_HEADERS
from config_dantri import BASE_URL as DANTRI_BASE_URL, CATEGORIES as DANTRI_CATEGORIES, HEADERS as DANTRI_HEADERS
from config_zingnews import BASE_URL as ZINGNEWS_BASE_URL, CATEGORIES as ZINGNEWS_CATEGORIES, HEADERS as ZINGNEWS_HEADERS
from config_twentyfourh import BASE_URL as TWENTYFOURH_BASE_URL, CATEGORIES as TWENTYFOURH_CATEGORIES, HEADERS as TWENTYFOURH_HEADERS
from config_thanhnien import BASE_URL as THANHNIEN_BASE_URL, CATEGORIES as THANHNIEN_CATEGORIES, HEADERS as THANHNIEN_HEADERS
from config_vietnamnet import BASE_URL as VIETNAMNET_BASE_URL, CATEGORIES as VIETNAMNET_CATEGORIES, HEADERS as VIETNAMNET_HEADERS

# -------------------------- LOG & DIR --------------------------
os.makedirs("logs", exist_ok=True)
os.makedirs("data", exist_ok=True)

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler("logs/crawl_all_rss_v13_v3.log", encoding="utf-8"),
        logging.StreamHandler()
    ]
)

# -------------------------- SESSION & RETRY --------------------------
session = requests.Session()

def safe_get(url, headers, retries=6, backoff=5, timeout=60):
    for i in range(retries):
        try:
            resp = session.get(url, headers=headers, timeout=timeout)
            if resp.status_code == 200:
                return resp
            if resp.status_code in (403, 429, 503):
                headers['User-Agent'] = get_random_user_agent()
                time.sleep(backoff * (2 ** i))
        except Exception as e:
            logging.warning(f"Retry {i+1}/{retries} GET {url} - {e}")
            time.sleep(backoff * (2 ** i))
    return None

def get_random_user_agent():
    ua_list = [
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/130.0.0.0 Safari/537.36",
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/130.0.0.0 Safari/537.36",
        "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/130.0.0.0 Safari/537.36",
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:129.0) Gecko/20100101 Firefox/129.0",
    ]
    return random.choice(ua_list)

# -------------------------- SELENIUM (CHỈ DÙNG KHI CẦN) --------------------------
def get_selenium_driver(headers=None):
    options = Options()
    options.add_argument("--headless=new")
    options.add_argument("--no-sandbox")
    options.add_argument("--disable-dev-shm-usage")
    options.add_argument("--disable-gpu")
    options.add_argument("--window-size=1920,1080")
    options.add_argument("--disable-blink-features=AutomationControlled")
    options.add_argument("--disable-extensions")
    options.add_argument("--disable-plugins")

    ua = headers.get('User-Agent') if headers else get_random_user_agent()
    options.add_argument(f"--user-agent={ua}")

    prefs = {
        "profile.managed_default_content_settings.images": 2,
        "profile.default_content_setting_values.notifications": 2,
    }
    options.add_experimental_option("prefs", prefs)
    options.add_experimental_option("excludeSwitches", ["enable-automation"])
    options.add_experimental_option('useAutomationExtension', False)

    try:
        driver = webdriver.Chrome(options=options)
        driver.execute_script("Object.defineProperty(navigator, 'webdriver', {get: () => undefined});")
        return driver
    except Exception as e:
        logging.error(f"ChromeDriver init error: {e}")
        return None

def safe_get_selenium(url, headers=None, timeout=120, retries=2):
    for attempt in range(retries):
        driver = get_selenium_driver(headers)
        if not driver:
            time.sleep(10)
            continue
        try:
            driver.set_page_load_timeout(timeout)
            driver.get(url)
            WebDriverWait(driver, 60).until(EC.presence_of_element_located((By.TAG_NAME, "body")))
            time.sleep(5)
            page_source = driver.page_source
            class MockResp:
                text = page_source
                status_code = 200
            return MockResp()
        except Exception as e:
            logging.error(f"Selenium attempt {attempt+1}/{retries} error {url}: {e}")
        finally:
            try:
                driver.quit()
            except:
                pass
        time.sleep(15)
    return None

# -------------------------- TEXT CLEAN --------------------------
def clean_text(text):
    if not text:
        return ""
    return re.sub(r'\s+', ' ', text.replace("\xa0", " ").replace("\r", " ").replace("\n", " ")).strip()

# -------------------------- URL VALIDATION --------------------------
def is_valid_url(link, source):
    if not link or not link.startswith(('http://', 'https://')):
        return False
    lower = link.lower()

    exclude = [
        "login", "logout", "register", "signin", "signup", "forgot", "quen-mat-khau",
        "policy", "privacy", "term", "dieu-khoan", "chinh-sach", "contact", "about",
        "sitemap", "rss", "feed", "tag", "category", "video", "clip", "photo", "audio",
        "ads", "advertisement", "banner", ".pdf", ".doc", ".jpg", ".png", ".mp4",
        "javascript:", "mailto:", "#", "facebook.com", "twitter.com", "youtube.com"
    ]
    if any(k in lower for k in exclude):
        return False

    id_patterns = {
        "vietnamnet": r'\d{6,}',
        "twentyfourh": r'\d{6,}',
        "default": r'\d{7,}'
    }
    pattern = id_patterns.get(source, id_patterns["default"])
    if not re.search(pattern, link):
        return False

    domain_map = {
        "vnexpress": ("vnexpress.net", (".html",)),
        "tuoitre": ("tuoitre.vn", (".htm",)),
        "soha": ("soha.vn", (".htm", ".html")),
        "dantri": ("dantri.com.vn", (".htm", ".html")),
        "zingnews": (("zingnews.vn", "znews.vn"), (".html",)),
        "twentyfourh": ("24h.com.vn", (".html",)),
        "thanhnien": ("thanhnien.vn", (".htm", ".html")),
        "vietnamnet": ("vietnamnet.vn", (".html",)),
    }
    domains, exts = domain_map.get(source, (None, None))
    if not domains or not exts:
        return False
    if isinstance(domains, tuple):
        if not any(d in lower for d in domains):
            return False
    else:
        if domains not in lower:
            return False
    if not any(e in lower for e in exts):
        return False
    return True

# -------------------------- DATE EXTRACTION --------------------------
def extract_date(raw, source):
    if not raw:
        return ""
    iso = re.search(r'\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}', raw)
    if iso:
        return iso.group(0)[:10]

    patterns = [r'(\d{1,2}[\/\-]\d{1,2}[\/\-]\d{4})', r'(\d{4}-\d{2}-\d{2})']
    for p in patterns:
        m = re.search(p, raw)
        if m:
            d = m.group(1)
            if '/' in d or '-' in d:
                parts = re.split(r'[/\-]', d)
                if len(parts) == 3:
                    if len(parts[0]) == 4:
                        return d
                    else:
                        return f"{parts[2]}-{parts[1].zfill(2)}-{parts[0].zfill(2)}"
    return ""

# -------------------------- DETAIL SELECTORS --------------------------
DETAIL_SELECTORS = {
    "vnexpress": {
        'title': ["h1.title-detail", "h1.title-news"],
        'abstract': ["p.description", "h2.sapo"],
        'content_div': ["article.fck_detail", "div.fck_detail"],
        'content': ["p.Normal", "p"],
        'date': ["span.date", "meta[itemprop='datePublished']"],
        'unwanted': ["figure", "script", "style", ".box-embed", ".box-insert"]
    },
    "tuoitre": {
        'title': ["h1.detail-title", "h1.article-title"],
        'abstract': ["h2.detail-sapo", "h2.sapo"],
        'content_div': ["div.detail-content", "div.article-content"],
        'content': ["p"],
        'date': ["div.detail-time", "time.publish-date"],
        'unwanted': ["figure", "script", "style", ".ads", ".related-news"]
    },
    "soha": {
        'title': ["h1"],
        'abstract': ["h2", "p.sapo"],
        'content_div': ["div.article-content", "div.content"],
        'content': ["p"],
        'date': ["span.time", "meta[property='article:published_time']"],
        'unwanted': ["figure", "script", "style", ".box-embed", ".ads", ".related"]
    },
    "dantri": {
        'title': ["h1.dt-news__title", "h1.title"],
        'abstract': ["h2.dt-news__sapo", "h2.sapo"],
        'content_div': ["div.dt-news__content", "div.singular-content"],
        'content': ["p"],
        'date': ["span.dt-news__time", "time"],
        'unwanted': ["figure", "script", "style", ".ads", ".related"]
    },
    "zingnews": {
        'title': ["h1.the-article-title", "h1.article-title"],
        'abstract': ["p.the-article-summary", "h2.sapo"],
        'content_div': ["div.the-article-body", "div.article-content"],
        'content': ["p"],
        'date': [".the-article-publish", "time"],
        'unwanted': ["figure", "script", "style", ".ads", ".related-news"]
    },
    "twentyfourh": {
        'title': ["h1"],
        'abstract': [".sapo"],
        'content_div': [".baiviet-body"],
        'content': ["p"],
        'date': [".time"],
        'unwanted': ["figure", "script", "style", ".ads", ".related"]
    },
    "thanhnien": {
        'title': ["h1.detail__title", "h1.title", ".detail-title"],
        'abstract': ["h2.detail__sapo", "div.detail-sapo", ".sapo"],
        'content_div': ["div.detail__content", "div.detail-cmain"],
        'content': ["p"],
        'date': ["div.detail__meta time", "time"],
        'unwanted': ["figure", "script", "style", ".ads", ".related"]
    },
    "vietnamnet": {
        'title': ["h1"],
        'abstract': [".lead", "p.lead"],
        'content_div': [".content"],
        'content': ["p"],
        'date': [".date"],
        'unwanted': ["figure", "script", "style", ".ads", ".sidebar"]
    },
}

# -------------------------- CRAWL DETAIL --------------------------
def crawl_article_detail_generic(link, headers, selectors, source):
    print(f" → Crawl detail: {link}")

    if not is_valid_url(link, source):
        return "", "", "", ""

    # ƯU TIÊN REQUESTS
    resp = safe_get(link, headers, retries=6, timeout=60)
    if not resp:
        print(" → Fallback Selenium cho chi tiết")
        resp = safe_get_selenium(link, headers)

    if not resp:
        print(" [!] Không tải được chi tiết")
        return "", "", "", ""

    soup = BeautifulSoup(resp.text, "lxml")

    # ---- TITLE ----
    title = ""
    for sel in selectors['title']:
        el = soup.select_one(sel)
        if el and len(clean_text(el.get_text())) > 10:
            title = clean_text(el.get_text())
            break
    if not title:
        title_tag = soup.find("title")
        title = clean_text(title_tag.get_text()).split("|")[0].split("-")[0].strip() if title_tag else "Không tiêu đề"

    # ---- CONTENT ----
    content_parts = []
    for div_sel in selectors['content_div']:
        div = soup.select_one(div_sel)
        if div:
            for bad in selectors.get('unwanted', []):
                for x in div.select(bad):
                    x.decompose()
            for p_sel in selectors['content']:
                for p in div.select(p_sel):
                    txt = clean_text(p.get_text())
                    if 50 <= len(txt) <= 2000 and not any(k in txt.lower() for k in
                                                          ["xem thêm", "đọc thêm", "quảng cáo", "bình luận", "đăng nhập"]):
                        content_parts.append(txt)
            if content_parts:
                break

    if len(content_parts) < 3:
        for p in soup.find_all('p'):
            txt = clean_text(p.get_text())
            if 80 <= len(txt) <= 1500 and not any(k in txt.lower() for k in
                                                  ["quảng cáo", "menu", "footer", "đăng nhập"]):
                content_parts.append(txt)
                if len(content_parts) >= 10:
                    break

    content = " ".join(content_parts)

    # ---- ABSTRACT ----
    abstract = ""
    if content_parts:
        for para in content_parts:
            if len(para) > 100:
                abstract = para[:400] + ("..." if len(para) > 400 else "")
                break
    else:
        for sel in selectors['abstract']:
            el = soup.select_one(sel)
            if el:
                abstract = clean_text(el.get_text())
                if len(abstract) > 50:
                    break

    # ---- DATE ----
    date = ""
    for meta_sel in ["meta[property='article:published_time']", "meta[name='pubdate']",
                     "meta[itemprop='datePublished']"]:
        m = soup.select_one(meta_sel)
        if m and m.get("content"):
            date = extract_date(m["content"], source)
            if date:
                break
    if not date:
        for sel in selectors.get('date', []):
            el = soup.select_one(sel)
            if el:
                raw = el.get("datetime") or el.get("content") or el.get_text()
                date = extract_date(raw, source)
                if date:
                    break
    if not date:
        m = re.search(r'/(\d{4})/(\d{2})/(\d{2})/', link) or re.search(r'(\d{4})(\d{2})(\d{2})', link)
        if m and len(m.groups()) == 3:
            date = f"{m.group(1)}-{m.group(2)}-{m.group(3)}"
    if not date and len(content) > 300:
        date = datetime.now().strftime("%Y-%m-%d")

    if len(content) < 120 or len(title) < 15:
        print(f" [!] Bài không đủ chất lượng – title:{len(title)} content:{len(content)}")
        return "", "", "", ""

    return abstract, content, date, title

def make_detail_func(source):
    sel = DETAIL_SELECTORS[source]
    return lambda link, headers: crawl_article_detail_generic(link, headers, sel, source)

# -------------------------- LIST SELECTORS --------------------------
LIST_SELECTORS = {
    "vnexpress": "article.item-news a, article.item-news h2 a, article.item-news h3 a",
    "tuoitre": "li.news-item h3 a, a[href*='.htm'], h3.title-news a",
    "soha": "h3 a, a[href*='.htm'], a[href*='.html'], .title a",
    "dantri": "article.article-item h3 a, .dt-news__title a, h3 a, a[href*='.htm'], a[href*='.html']",
    "zingnews": "article.article-item h3 a, .article-title a, h1 a, h2 a, a[href*='.html']",
    "twentyfourh": "h3 a, .title a, a[href*='.html'], h2 a",
    "thanhnien": ".story__title a, .story a, h3 a, h2 a, a[href*='.htm'], a[href*='.html']",
    "vietnamnet": "h3 a, .VnnTitle, .title-news a, a[href*='.html']",
}

# -------------------------- PAGE FORMATS --------------------------
PAGE_FORMATS = {
    "vnexpress": lambda b, c, p: f"{b}/{c}-p{p}",
    "tuoitre": lambda b, c, p: f"{b}/{c.replace('.htm','')}/trang-{p}.htm",
    "soha": lambda b, c, p: f"{b}/{c.replace('.htm','')}-p{p}.htm",
    "dantri": lambda b, c, p: f"{b}/{c.replace('.htm','')}/trang-{p}.htm",
    "zingnews": lambda b, c, p: f"{b}/{c}?page={p}",
    "twentyfourh": lambda b, c, p: f"{b}/{c}?page={p}",
    "thanhnien": lambda b, c, p: f"{b}/{c.replace('.htm','')}/trang-{p}.htm",
    "vietnamnet": lambda b, c, p: f"{b}/{c}/page/{p}",
}

# -------------------------- CRAWL CATEGORY (ĐÃ FIX HOÀN TOÀN) --------------------------
def crawl_category_generic(base_url, cat_slug, cat_name, headers, list_selector,
                           detail_func, page_fmt, max_pages=1,
                           crawled_urls=set(), source_name="unknown"):
    data = []
    page = 1

    while page <= max_pages:
        url = f"{base_url}/{cat_slug}" if page == 1 else page_fmt(base_url, cat_slug, page)
        print(f" Crawl {cat_name} - Trang {page}: {url}")

        # TẮT SELENIUM CHO THANH NIÊN & VIETNAMNET Ở LIST PAGE
        if source_name in ("thanhnien", "vietnamnet"):
            resp = safe_get(url, headers, retries=8, timeout=90)
        elif source_name == "dantri":
            resp = safe_get_selenium(url, headers)
        else:
            resp = safe_get(url, headers)

        if not resp:
            print(" [!] Không tải được trang")
            break

        soup = BeautifulSoup(resp.text, "lxml")
        items = soup.select(list_selector)

        if len(items) < 5:
            fallback = [
                "a[href*='.html']", "a[href*='.htm']",
                "h1 a", "h2 a", "h3 a", "h4 a",
                ".title a", ".story a", ".article-title a"
            ]
            for fb in fallback:
                cand = soup.select(fb)
                if len(cand) >= 5:
                    items = cand
                    print(f" → Fallback {fb} → {len(items)}")
                    break

        if not items:
            print(" [!] Không tìm thấy link")
            break

        valid_links = []
        for a in items:
            href = a.get("href")
            if not href:
                continue
            if href.startswith("//"):
                href = "https:" + href
            elif href.startswith("/"):
                href = base_url.rstrip("/") + href
            elif not href.startswith("http"):
                href = base_url.rstrip("/") + "/" + href.lstrip("/")

            if (is_valid_url(href, source_name) and href not in crawled_urls):
                valid_links.append(href)
                crawled_urls.add(href)

        if not valid_links:
            print(" Trang không có bài mới")
            page += 1
            continue

        print(f" Trang {page}: {len(valid_links)} bài hợp lệ")

        # CRAWL DETAILS
        def crawl_one(link):
            try:
                abs_, cont, dt, title = detail_func(link, headers)
                if cont and len(cont) >= 120 and title and len(title) >= 15:
                    return {
                        "source": source_display.get(source_name, source_name),
                        "category": cat_name,
                        "title": title,
                        "abstract": abs_,
                        "content": cont,
                        "date": dt,
                        "url": link
                    }
            except Exception as e:
                logging.error(f"Detail error {link}: {e}")
            return None

        with ThreadPoolExecutor(max_workers=5) as exe:
            for fut in as_completed([exe.submit(crawl_one, link) for link in valid_links]):
                r = fut.result()
                if r:
                    data.append(r)
        time.sleep(random.uniform(3, 6))

        page += 1
        time.sleep(30)

    return data

# -------------------------- MAIN --------------------------
if __name__ == "__main__":
    source_display = {
        "vnexpress": "VnExpress", "tuoitre": "Tuổi Trẻ", "soha": "Soha",
        "dantri": "Dân Trí", "zingnews": "ZingNews", "twentyfourh": "24h",
        "thanhnien": "Thanh Niên", "vietnamnet": "VietnamNet"
    }

    sources = [
        ("vnexpress", VNEXPRESS_BASE_URL, VNEXPRESS_CATEGORIES, VNEXPRESS_HEADERS,
         LIST_SELECTORS["vnexpress"], make_detail_func("vnexpress"), PAGE_FORMATS["vnexpress"]),
        ("tuoitre", TUOITRE_BASE_URL, TUOITRE_CATEGORIES, TUOITRE_HEADERS,
         LIST_SELECTORS["tuoitre"], make_detail_func("tuoitre"), PAGE_FORMATS["tuoitre"]),
        ("soha", SOHA_BASE_URL, SOHA_CATEGORIES, SOHA_HEADERS,
         LIST_SELECTORS["soha"], make_detail_func("soha"), PAGE_FORMATS["soha"]),
        ("dantri", DANTRI_BASE_URL, DANTRI_CATEGORIES, DANTRI_HEADERS,
         LIST_SELECTORS["dantri"], make_detail_func("dantri"), PAGE_FORMATS["dantri"]),
        ("zingnews", ZINGNEWS_BASE_URL, ZINGNEWS_CATEGORIES, ZINGNEWS_HEADERS,
         LIST_SELECTORS["zingnews"], make_detail_func("zingnews"), PAGE_FORMATS["zingnews"]),
        ("twentyfourh", TWENTYFOURH_BASE_URL, TWENTYFOURH_CATEGORIES, TWENTYFOURH_HEADERS,
         LIST_SELECTORS["twentyfourh"], make_detail_func("twentyfourh"), PAGE_FORMATS["twentyfourh"]),
        ("thanhnien", THANHNIEN_BASE_URL, THANHNIEN_CATEGORIES, THANHNIEN_HEADERS,
         LIST_SELECTORS["thanhnien"], make_detail_func("thanhnien"), PAGE_FORMATS["thanhnien"]),
        ("vietnamnet", VIETNAMNET_BASE_URL, VIETNAMNET_CATEGORIES, VIETNAMNET_HEADERS,
         LIST_SELECTORS["vietnamnet"], make_detail_func("vietnamnet"), PAGE_FORMATS["vietnamnet"]),
    ]

    all_articles = []
    total_new = 0

    for src_name, base, cats, hdr, lst_sel, det_func, pg_fmt in sources:
        print("\n" + "="*80)
        print(f"ĐANG CRAWL: {source_display[src_name].upper()}")
        print("="*80)

        chk_path = f"logs/{src_name}_checkpoint_v13_v3.pkl"
        crawled = set()
        src_articles = []

        if os.path.exists(chk_path):
            try:
                df = pd.read_pickle(chk_path)
                crawled = set(df["url"])
                src_articles = df.to_dict('records')
                print(f" → Load checkpoint: {len(src_articles)} bài cũ")
            except Exception as e:
                logging.error(f"Checkpoint load error {src_name}: {e}")

        for slug, cname in cats.items():
            print(f"\n--- Category: {cname} ---")
            cat_data = crawl_category_generic(
                base_url=base, cat_slug=slug, cat_name=cname,
                headers=hdr, list_selector=lst_sel, detail_func=det_func,
                page_fmt=pg_fmt, max_pages=1, crawled_urls=crawled, source_name=src_name
            )
            new_cnt = len(cat_data)
            src_articles.extend(cat_data)
            total_new += new_cnt
            print(f" + {new_cnt} bài mới từ {cname}")
            time.sleep(2)

        df_src = pd.DataFrame(src_articles).drop_duplicates(subset=["url"])
        df_src.to_pickle(chk_path)
        df_src.to_csv(f"data/{src_name}_v13_v3.csv", index=False, quoting=csv.QUOTE_ALL, encoding="utf-8")
        all_articles.extend(df_src.to_dict('records'))
        print(f" HOÀN TẤT {source_display[src_name]}: {len(df_src)} bài\n")

    if all_articles:
        df_all = pd.DataFrame(all_articles).drop_duplicates(subset=["url"])
        df_all = df_all.sort_values("date", ascending=False).reset_index(drop=True)

        df_all['new_id'] = [f"N_{i+1}" for i in range(len(df_all))]
        cols = ['new_id'] + [col for col in df_all.columns if col != 'new_id']
        df_all = df_all[cols]

        df_all.to_csv("data/all_nguon_tin_tuc_v13_v3.csv", index=False, quoting=csv.QUOTE_ALL, encoding="utf-8")
        df_all.to_pickle("data/all_nguon_tin_tuc_v13_v3.pkl")
        
        print("\n" + "="*80)
        print(f"HOÀN TẤT TOÀN BỘ - TỔNG {len(df_all)} BÀI ({total_new} mới)")
        print(f"→ ĐÃ THÊM CỘT 'new_id' TỪ N_1 ĐẾN N_{len(df_all)}")
        print("="*80)
    else:
        print("KHÔNG CÓ BÀI NÀO ĐƯỢC CRAWL")

# đang fix thanh nien, VietNamNet, còn lại là ok rồi