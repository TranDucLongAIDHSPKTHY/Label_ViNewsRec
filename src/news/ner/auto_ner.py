"""
Mục tiêu: Tự động trích xuất thực thể (NER) từ title & abstract
Điểm nổi bật:
    • Gộp tên người (PER) trước khi tách địa danh (LOC)
    • Chuẩn hóa địa danh Việt Nam (Hà Nội → TP. Hà Nội)
    • Tách địa danh phức hợp (Hà Nội và TP.HCM → 2 LOC riêng)
    • Phát hiện MISC (số liệu, thiên tai), ORG (công ty, trường học)
    • Loại bỏ trùng lặp thực thể
"""

import pandas as pd
import json
from transformers import AutoTokenizer, AutoModelForTokenClassification, pipeline
import torch
import re

# ==================== CẤU HÌNH ====================
# Đường dẫn file CSV đầu vào (sau crawl) và đầu ra (có entities)
INPUT_PATH = r"D:\ViNewsRec\ViNewsRec_dataset\data\all_nguon_tin_tuc_v14_v5.csv"
OUTPUT_PATH = r"D:\ViNewsRec\ViNewsRec_dataset\data\all_nguon_tin_tuc_v14_v5_label.csv" 

# Mô hình NER tiếng Việt (Electra-based, hiệu suất cao)
MODEL_NAME = "NlpHUST/ner-vietnamese-electra-base"

# ==================== DANH SÁCH ĐỊA DANH CHUẨN ====================
VIETNAM_PLACES_STANDARD = {
    "TP. Hà Nội", "TP. Hồ Chí Minh", "TP. Đà Nẵng", "TP. Hải Phòng", "TP. Cần Thơ",
    "An Giang", "Bà Rịa - Vũng Tàu", "Bắc Giang", "Bắc Kạn", "Bạc Liêu", "Bắc Ninh",
    "Bến Tre", "Bình Định", "Bình Dương", "Bình Phước", "Bình Thuận", "Cà Mau",
    "Cao Bằng", "Đắk Lắk", "Đắk Nông", "Điện Biên", "Đồng Nai", "Đồng Tháp",
    "Gia Lai", "Hà Giang", "Hà Nam", "Hà Tĩnh", "Hải Dương", "Hậu Giang", "Hòa Bình",
    "Hưng Yên", "Khánh Hòa", "Kiên Giang", "Kon Tum", "Lai Châu", "Lâm Đồng",
    "Lạng Sơn", "Lào Cai", "Long An", "Nam Định", "Nghệ An", "Ninh Bình", "Ninh Thuận",
    "Phú Thọ", "Phú Yên", "Quảng Bình", "Quảng Nam", "Quảng Ngãi", "Quảng Ninh",
    "Quảng Trị", "Sóc Trăng", "Sơn La", "Tây Ninh", "Thái Bình", "Thái Nguyên",
    "Thanh Hóa", "TP. Huế", "Tiền Giang", "Trà Vinh", "Tuyên Quang",
    "Vĩnh Long", "Vĩnh Phúc", "Yên Bái"
}

ECONOMIC_OR_SUBREGIONS = {
    "Dung Quất", "Vũng Áng", "Nghi Sơn", "Cát Linh", "Cầu Giấy"
}

# Bảng ánh xạ biệt danh → tên chuẩn
PLACE_ALIAS_TO_STANDARD = {
    # Hà Nội
    "Hà Nội": "TP. Hà Nội",
    "Tp Hà Nội": "TP. Hà Nội",
    "Thành phố Hà Nội": "TP. Hà Nội",

    # Hồ Chí Minh
    "Sài Gòn": "TP. Hồ Chí Minh",
    "TP HCM": "TP. Hồ Chí Minh",
    "TPHCM": "TP. Hồ Chí Minh",
    "Tp Hồ Chí Minh": "TP. Hồ Chí Minh",
    "Thành phố Hồ Chí Minh": "TP. Hồ Chí Minh",
    "Ho Chi Minh": "TP. Hồ Chí Minh",
    "HCM": "TP. Hồ Chí Minh",
    "HCM City": "TP. Hồ Chí Minh",

    # Đà Nẵng
    "Đà Nẵng": "TP. Đà Nẵng",
    "Tp Đà Nẵng": "TP. Đà Nẵng",
    "Thành phố Đà Nẵng": "TP. Đà Nẵng",
    "Da Nang": "TP. Đà Nẵng",

    # Hải Phòng
    "Hải Phòng": "TP. Hải Phòng",
    "Tp Hải Phòng": "TP. Hải Phòng",
    "Thành phố Hải Phòng": "TP. Hải Phòng",

    # Cần Thơ
    "Cần Thơ": "TP. Cần Thơ",
    "Tp Cần Thơ": "TP. Cần Thơ",
    "Thành phố Cần Thơ": "TP. Cần Thơ",

    # Huế / Thừa Thiên Huế | ngày 1/1/2025, theo NQ175/2024/QH15: Các cơ quan, tổ chức gắn với tên "Thừa Thiên Huế" được đổi tên sang "Thành phố Huế"
    "Huế": "TP. Huế",
    "TP Huế": "TP. Huế",
    "TP. Huế": "TP. Huế",
    "Thành phố Huế": "TP. Huế",
    "Tp Huế": "TP. Huế",
    "Thừa Thiên Huế": "TP. Huế",
}

# Các quốc gia nước ngoài thường xuất hiện
FOREIGN_COUNTRIES = {
    "Nam Phi", "Hàn Quốc", "Mỹ", "Trung Quốc", "Nhật Bản", "Úc",
    "Anh", "Pháp", "Đức", "Canada", "Nga", "Ấn Độ", "Singapore",
    "Malaysia", "Thái Lan", "Indonesia", "Philippines", "Brazil",
    "Campuchia", "Lào", "Myanmar", "Triều Tiên", "Ukraine", "Iran", "Israel",
    "Thổ Nhĩ Kỳ", "Tây Ban Nha", "Ý", "Hà Lan", "Thuỵ Điển", "Thuỵ Sĩ"
}

# Từ khóa địa lý để tách cụm
GEO_KEYWORDS = ["sông", "cầu", "núi", "hồ", "đèo", "khu", "xã", "huyện", "tỉnh", "thành phố", "tp", "thị xã", "phường", "quận"]

# ==================== KHỞI TẠO MODEL ====================
print("Đang tải mô hình NER...")
tokenizer = AutoTokenizer.from_pretrained(MODEL_NAME)
model = AutoModelForTokenClassification.from_pretrained(MODEL_NAME)
# Pipeline NER với aggregation_strategy="simple" → nhóm subwords
ner = pipeline("ner", model=model, tokenizer=tokenizer, aggregation_strategy="simple", device=-1)

# Map nhãn mô hình → nhãn chuẩn
LABEL_MAP = {"PERSON": "PER", "ORGANIZATION": "ORG", "LOCATION": "LOC", "MISCELLANEOUS": "MISC"}

# ==================== PHÁT HIỆN MISC & ORG ====================
MISC_PATTERNS = [
    r"\d+[\.,]?\d*\s*(người|tuổi|m3|ha|triệu|tỷ|km|giờ|ngày|tháng|đồng|năm)",
    r"\b(bão|lũ|vỡ hồ|sạt lở|ngập|mưa lớn|cầu gãy|hồ chứa|thiên tai)\b"
]
def is_misc(text):
    """Kiểm tra xem cụm có phải là số liệu/thiên tai không"""
    return any(re.search(p, text, re.IGNORECASE) for p in MISC_PATTERNS)

ORG_KEYWORDS = ["công ty", "tập đoàn", "trang trại", "sở", "cục", "ubnd", "trường", "bộ"]
def detect_org(text):
    """Phát hiện tổ chức qua từ khóa"""
    return any(kw in text.lower() for kw in ORG_KEYWORDS)

# ==================== CHUẨN HÓA TÊN ĐỊA DANH ====================
def standardize_place_name(text):
    """Hà Nội → TP. Hà Nội, loại từ trùng"""
    text = text.strip()
    words = text.split()
    seen = set()
    cleaned = [w for w in words if w.lower() not in seen and not seen.add(w.lower())]
    text = " ".join(cleaned)
    return PLACE_ALIAS_TO_STANDARD.get(text, text)

# ==================== GỘP TÊN NGƯỜI (TRƯỚC KHI TÁCH LOC) ====================
VIETNAMESE_SURNAMES = {
    "Nguyễn", "Trần", "Lê", "Phạm", "Hoàng", "Huỳnh", "Vũ", "Võ", "Đặng", "Bùi",
    "Đỗ", "Hồ", "Ngô", "Dương", "Lý", "Đinh", "Phan", "Vương", "Tô", "Hà"
}

TITLE_WORDS = {"ông", "bà", "anh", "chị", "em", "cô", "chú", "bác", "đồng chí", "ngài"}

def merge_per_early(entities):
    """Gộp các token PER liên tiếp thành tên đầy đủ, loại danh xưng"""
    if not entities:
        return []
    merged = []
    i = 0
    while i < len(entities):
        ent = entities[i]
        if ent["Label"] == "PER":
            name_parts = [ent["Text"]]
            j = i + 1
            while j < len(entities) and entities[j]["Label"] == "PER":
                name_parts.append(entities[j]["Text"])
                j += 1
            full_name = " ".join(name_parts)
            words = full_name.split()
            filtered = [w for w in words if w.lower() not in TITLE_WORDS]
            clean_name = " ".join(filtered).strip()
            if clean_name:
                merged.append({"Label": "PER", "Text": clean_name})
            i = j
        else:
            merged.append(ent)
            i += 1
    return merged

# ==================== TÁCH ĐỊA DANH SAU KHI GỘP PER ====================
def split_loc_final(entities):
    """Tách LOC theo từ khóa địa lý + danh sách chuẩn, loại trùng"""
    if not entities:
        return []
    refined = []

    for ent in entities:
        if ent["Label"] != "LOC":
            refined.append(ent)
            continue

        text = ent["Text"]
        # B1: Tách theo từ khóa (sông, tỉnh,...)
        pattern = r"\s+(" + "|".join(re.escape(k) for k in GEO_KEYWORDS) + r")\s+"
        parts = re.split(pattern, " " + text + " ", flags=re.IGNORECASE)
        current = ""

        for part in parts:
            part = part.strip()
            if not part: continue
            if part.lower() in [k.lower() for k in GEO_KEYWORDS]:
                if current:
                    refined.append({"Label": "LOC", "Text": standardize_place_name(current)})
                    current = part
                else:
                    current = part
            else:
                if current:
                    current += " " + part
                else:
                    current = part
        if current:
            refined.append({"Label": "LOC", "Text": standardize_place_name(current)})

        # B2: Tách theo danh sách chuẩn (nếu có nhiều địa danh trong 1 cụm)
        temp = []
        all_places = VIETNAM_PLACES_STANDARD.union(ECONOMIC_OR_SUBREGIONS).union(FOREIGN_COUNTRIES)
        for r in refined:
            if r["Label"] == "LOC":
                matches = [(m.start(), m.end(), m.group()) for m in re.finditer(
                    r"\b(" + "|".join(re.escape(p) for p in all_places) + r")\b", r["Text"], re.IGNORECASE)]
                if len(matches) > 1:
                    matches.sort()
                    last = 0
                    for s, e, p in matches:
                        pre = r["Text"][last:s].strip()
                        if pre and pre not in ["và", "với", "tại", "của"]:
                            temp.append({"Label": "LOC", "Text": standardize_place_name(pre)})
                        temp.append({"Label": "LOC", "Text": p})
                        last = e
                    suf = r["Text"][last:].strip()
                    if suf and suf not in ["và", "với", "tại", "của"]:
                        temp.append({"Label": "LOC", "Text": standardize_place_name(suf)})
                else:
                    temp.append(r)
            else:
                temp.append(r)
        refined = temp

    # B3: Loại trùng (case-insensitive)
    seen = set()
    final = []
    for ent in refined:
        key = ent["Text"].lower()
        if key not in seen:
            seen.add(key)
            final.append(ent)
    return final

# ==================== TRÍCH XUẤT THỰC THỂ ====================
def extract_entities(text):
    """Pipeline đầy đủ: NER → gộp → xử lý hậu kỳ → trả về list[dict]"""
    if pd.isna(text) or not str(text).strip():
        return []
    text = str(text)[:1024]  # Giới hạn độ dài tránh lỗi GPU/timeout
    try:
        results = ner(text)
        if not results:
            return []

        # B1: GỘP SUBWORD từ mô hình
        entities = []
        current = {"text": "", "label": ""}
        for r in results:
            word = r['word'].replace('▁', ' ').strip()
            if not word: continue
            raw_label = r.get('entity_group')
            if not raw_label or raw_label == 'O':
                if current["text"]:
                    entities.append({"Label": current["label"], "Text": current["text"].strip()})
                    current = {"text": "", "label": ""}
                continue
            label = LABEL_MAP.get(raw_label)
            if not label: continue
            if current["label"] == label:
                current["text"] += " " + word
            else:
                if current["text"]:
                    entities.append({"Label": current["label"], "Text": current["text"].strip()})
                current = {"text": word, "label": label}
        if current["text"]:
            entities.append({"Label": current["label"], "Text": current["text"].strip()})

        if not entities:
            return []

        # B2: GỘP TÊN NGƯỜI
        entities = merge_per_early(entities)

        # B3: GÁN MISC & ORG THEO QUY TẮC
        for ent in entities:
            if is_misc(ent["Text"]):
                ent["Label"] = "MISC"
            if ent["Label"] in ["LOC", "MISC"] and detect_org(ent["Text"]):
                ent["Label"] = "ORG"

        # B4: TÁCH LOC CHI TIẾT
        entities = split_loc_final(entities)

        # B5: LOẠI TRÙNG TOÀN BỘ
        seen = set()
        final = []
        for ent in entities:
            if not ent.get("Text"): continue
            key = (ent["Label"], ent["Text"].lower())
            if key not in seen:
                seen.add(key)
                final.append(ent)

        return final

    except Exception as e:
        print(f"Lỗi xử lý: {str(e)[:50]}...")
        return []

# ==================== CHẠY TOÀN BỘ FILE ====================
print(f"Đang đọc: {INPUT_PATH}")
df = pd.read_csv(INPUT_PATH, encoding='utf-8')
print(f"Đang gán nhãn cho {len(df)} bài...")

# Áp dụng NER cho title và abstract → lưu dạng JSON string
df['title_entities'] = df['title'].apply(extract_entities).apply(lambda x: json.dumps(x, ensure_ascii=False))
df['abstract_entities'] = df['abstract'].apply(extract_entities).apply(lambda x: json.dumps(x, ensure_ascii=False))

# Lưu các cột cần thiết
cols = ['new_id', 'source', 'category', 'title', 'abstract', 'title_entities', 'abstract_entities', 'url']
#cols = ['new_id', 'category', 'title', 'abstract', 'title_entities', 'abstract_entities', 'content', 'date', 'url']
df[cols].to_csv(OUTPUT_PATH, index=False, encoding='utf-8')

print(f"\nHOÀN THÀNH! File: {OUTPUT_PATH}")