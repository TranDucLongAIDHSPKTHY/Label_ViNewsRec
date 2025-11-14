import streamlit as st
import pandas as pd
import json
from pathlib import Path

# Giả sử đã cài đặt: pip install streamlit-text-annotation==0.1.5
from streamlit_text_annotation import text_annotation

# ===============================
# ⚙️ Cấu hình giao diện
# ===============================
st.set_page_config(page_title="NER Annotation Tool - Title & Abstract", layout="wide")
st.title("📌 Công Cụ Gán Nhãn NER - Phong Cách Label Studio (Chỉ Title & Abstract)")

LABELS = ["PER", "LOC", "ORG", "MISC"]
labels_list = [{"text": label} for label in LABELS]
BASE_SAVE_PATH = Path(r"D:\ViNewsRec\vietnamese_news_dataset\data")
BASE_SAVE_PATH.mkdir(parents=True, exist_ok=True)

# ===============================
# 🧩 Upload file dữ liệu
# ===============================
st.sidebar.header("Bước 1: Tải Lên File Dữ Liệu")
uploaded_file = st.sidebar.file_uploader("Chọn file CSV hoặc TSV", type=["csv", "tsv"])

if not uploaded_file:
    st.warning("⚠️ Hãy tải lên file chứa ít nhất hai cột: 'title' và 'abstract'.")
    st.stop()

# Đọc file
sep = "\t" if uploaded_file.name.endswith(".tsv") else ","
df = pd.read_csv(uploaded_file, sep=sep)

# Kiểm tra cột
required_cols = {"title", "abstract"}
if not required_cols.issubset(df.columns):
    st.error("❌ File phải chứa hai cột: 'title' và 'abstract'.")
    st.stop()

st.success(f"✅ Đã tải thành công {len(df)} dòng từ file {uploaded_file.name}.")

# ===============================
# 👤 Thông tin annotator
# ===============================
st.sidebar.header("Bước 2: Thông Tin Người Gán Nhãn")
annotator_name = st.sidebar.text_input("Tên người gán nhãn", value="annotator1")
save_dir = BASE_SAVE_PATH / annotator_name
save_dir.mkdir(exist_ok=True)

# ===============================
# 🔢 Chọn dòng dữ liệu
# ===============================
st.sidebar.header("Bước 3: Chọn Dòng Để Gán Nhãn")
current_row = st.sidebar.number_input("Số dòng:", min_value=0, max_value=len(df)-1, value=0)
selected_row = df.iloc[current_row]

title_content = str(selected_row["title"])
abstract_content = str(selected_row["abstract"])

# Khởi tạo session state nếu chưa có
if "annotations" not in st.session_state:
    st.session_state.annotations = {}

# Khởi tạo annotations cho dòng hiện tại nếu chưa có
if current_row not in st.session_state.annotations:
    title_tokens = [{"text": word + " "} for word in title_content.split()]  # Thêm khoảng trắng để hiển thị đúng
    abstract_tokens = [{"text": word + " "} for word in abstract_content.split()]
    common_data = {
        "labels": labels_list,
        "allowEditing": True,
        "labelOrientation": "horizontal",  # Hoặc "vertical"
        "collectLabelsFromTokens": False
    }
    st.session_state.annotations[current_row] = {
        "title_data": {"tokens": title_tokens, **common_data},
        "abstract_data": {"tokens": abstract_tokens, **common_data}
    }

# ===============================
# 📰 Gán nhãn cho Title
# ===============================
st.markdown(f"### 📰 Title (Dòng {current_row + 1})")
title_result = text_annotation(
    st.session_state.annotations[current_row]["title_data"],
    key=f"title_annot_{current_row}"
)

if title_result:
    st.session_state.annotations[current_row]["title_data"] = title_result

# ===============================
# 🧾 Gán nhãn cho Abstract
# ===============================
st.markdown("### 🧾 Abstract")
abstract_result = text_annotation(
    st.session_state.annotations[current_row]["abstract_data"],
    key=f"abstract_annot_{current_row}"
)

if abstract_result:
    st.session_state.annotations[current_row]["abstract_data"] = abstract_result

# ===============================
# 💾 Lưu (tự động cập nhật, nhưng nút để xác nhận)
# ===============================
if st.button("💾 Lưu Nhãn Cho Dòng Này"):
    st.success(f"✅ Nhãn cho dòng {current_row + 1} đã được cập nhật.")

# ===============================
# 📋 Xem annotations đã lưu
# ===============================
if st.session_state.annotations:
    st.markdown("### 📋 Danh Sách Dòng Đã Gán Nhãn")
    preview_data = []
    for idx, ann in sorted(st.session_state.annotations.items()):
        preview_data.append({
            "Dòng": idx + 1,
            "Nhãn Title": json.dumps(ann["title_data"]["tokens"], ensure_ascii=False),
            "Nhãn Abstract": json.dumps(ann["abstract_data"]["tokens"], ensure_ascii=False)
        })
    st.dataframe(pd.DataFrame(preview_data))

# ===============================
# 📤 Xuất file
# ===============================
st.subheader("Bước 4: Xuất File Kết Quả")
output_filename = st.text_input("Tên file đầu ra (không cần đuôi .tsv):", value=f"{uploaded_file.name.split('.')[0]}_annotated")

if st.button("📤 Xuất File .tsv"):
    export_df = df.copy()
    export_df["title_entities"] = ""
    export_df["abstract_entities"] = ""

    for idx, ann in st.session_state.annotations.items():
        export_df.at[idx, "title_entities"] = json.dumps(ann["title_data"]["tokens"], ensure_ascii=False)
        export_df.at[idx, "abstract_entities"] = json.dumps(ann["abstract_data"]["tokens"], ensure_ascii=False)

    output_path = save_dir / f"{output_filename}.tsv"
    export_df.to_csv(output_path, sep="\t", index=False, encoding="utf-8")

    st.success(f"✅ File đã được lưu tại: {output_path}")

    with open(output_path, "rb") as file:
        st.download_button(
            label="⬇️ Tải File Kết Quả (.tsv)",
            data=file,
            file_name=f"{output_filename}.tsv",
            mime="text/tab-separated-values"
        )

st.markdown("---")
st.caption("🚀 Phiên bản sửa lỗi v0.1.5 | Hỗ trợ NER cho PER, LOC, ORG, MISC | Sử dụng dict input cho text_annotation | Tokens với khoảng trắng")