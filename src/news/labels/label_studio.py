# Purpose:
#   - Chuyển file CSV (đã qua NER) sang định dạng JSON phù hợp với Label Studio
#   - Mỗi dòng là một object: news_id, title, abstract, entities, ...
#   - Dùng để: chỉnh sửa thủ công NER, bổ sung nhãn, chuẩn bị cho entity linking (Wikidata)
#   - Tương thích với MIND dataset format
# ================================================================

import pandas as pd
import json

# ---------------------------------------------------------------
# CẤU HÌNH ĐƯỜNG DẪN
# ---------------------------------------------------------------
# File CSV đầu vào: phải là file ĐÃ QUA auto_ner.py (có cột entities)
INPUT_CSV = 'data/vnexpress_news_full.csv'

# File JSON đầu ra: dùng import vào Label Studio (Tasks → Import)
OUTPUT_JSON = 'data/vnexpress_for_labeling.json'

# ---------------------------------------------------------------
# CHUYỂN ĐỔI & XUẤT FILE
# ---------------------------------------------------------------
if __name__ == "__main__":
    # B1: Đọc CSV bằng pandas
    df = pd.read_csv(INPUT_CSV)

    # B2: Chuyển thành list[dict] – định dạng chuẩn Label Studio
    #     Mỗi dict là một "task" với đầy đủ thông tin bài báo
    data = df.to_dict(orient='records')

    # B3: Ghi ra JSON với định dạng đẹp, hỗ trợ tiếng Việt
    with open(OUTPUT_JSON, 'w', encoding='utf-8') as f:
        json.dump(data, f, ensure_ascii=False, indent=2)

    # B4: Thông báo hoàn thành
    print(f"JSON file ready for LabelStudio: {OUTPUT_JSON}")
    # → Import tại: Label Studio → Project → Import → Chọn file JSON