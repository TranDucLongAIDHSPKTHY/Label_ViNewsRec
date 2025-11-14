# ViNewsRec – Hệ thống gợi ý tin tức tiếng Việt (Vietnamese News Recommendation)

Dự án xây dựng bộ dữ liệu tin tức tiếng Việt theo chuẩn **MIND**, bao gồm:
- Thu thập bài báo từ **VnExpress**
- Tự động gắn nhãn thực thể (NER)
- Chuẩn bị dữ liệu cho **Label Studio** (chỉnh sửa thủ công)
- Tạo hành vi người dùng giả lập (synthetic user behavior) bằng **GPT**

---

## Cấu trúc thư mục
ViNewsRec/
│
├── data/
│   ├── vnexpress_news_full.csv          # Dữ liệu gốc sau crawl
│   ├── vnexpress_news_labeled.csv       # Có thêm title_entities, abstract_entities
│   ├── vnexpress_for_labeling.json      # Import vào Label Studio
│   ├── behaviors.tsv                    # Hành vi người dùng (MIND format)
│   └── news.tsv                         # Thông tin bài báo (MIND format)
│
├── src/
│   ├── crawl_vnexpress.py               # Thu thập bài báo
│   ├── auto_ner.py                      # Tự động gắn nhãn NER
│   ├── label_studio.py                  # Chuyển CSV → JSON cho Label Studio
│   └── user_behavior_synthetic_llm.py   # Tạo hành vi người dùng bằng GPT
│
├── logs/
│   └── vnexpress_crawl.log              # Log quá trình crawl
│
├── requirements.txt
└── README.md

---

## Pipeline xử lý
    1[Crawl VnExpress] --> 2[vnexpress_news_full.csv]
    2 --> 3[auto_ner.py]
    3 --> 4[vnexpress_news_labeled.csv]
    4 --> 5[label_studio.py]
    5 --> 6[vnexpress_for_labeling.json]
    6 --> 7[user_behavior_synthetic_llm.py]
    7 --> 8[behaviors.tsv + news.tsv]

Cách sử dụng
1. Cài đặt môi trường : pip install -r requirements.txt
2. Thu thập dữ liệu: python src/crawl_vnexpress.py
Tạo data/vnexpress_news_full.csv
3. Tự động gắn nhãn NER: python src/auto_ner.py
Tạo data/vnexpress_news_labeled.csv
4. Chuẩn bị cho Label Studio (chỉnh sửa thủ công): python src/label_studio.py
Tạo data/vnexpress_for_labeling.json → Import vào Label Studio
5. Tạo dữ liệu hành vi người dùng (synthetic): python src/user_behavior_synthetic_llm.py
Tạo data/behaviors.tsv và data/news.tsv → Dùng train RecSys
---
# Model NER được sử dụng
- NlpHUST/ner-vietnamese-electra-base 
- Hỗ trợ: PER, ORG, LOC, MISC
- Đã tinh chỉnh hậu xử lý: gộp tên người, chuẩn hóa địa danh, tách cụm
---
# Định dạng đầu ra (MIND-style)
- news.tsv
news_id    category        subcategory         title       abstract    content     date        url                    title_entities                            abstract_entities
1          Thời sự         Chính trị           ...         ...         ...         2025-10-01  https://... [{"Label": "PER", "Text": "Nguyễn Phú Trọng"}, ...]  [...]

- behaviors.tsv
impression_id    user_id    time                    history        impressions
10001            U_A1B2C3   10/05/2025 09:23:11 AM  1 3           5-1 8-0 12-1 15-0
---
# Tùy chỉnh
- crawl_vnexpress.py: Sửa max_pages trong crawl_category() để crawl nhiều trang
- auto_ner.py: Thêm địa danh vào VIETNAM_PLACES_STANDARD
- user_behavior_synthetic_llm.py: Thay NUM_USERS, TARGET_CLICK_RATE, 
---

# Tác giả : Long-Tran Duc