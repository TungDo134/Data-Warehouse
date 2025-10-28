import os
import re
import yaml
import pandas as pd
import mysql.connector
from datetime import datetime
from zoneinfo import ZoneInfo
from unidecode import unidecode
from difflib import SequenceMatcher


# ====================== 🔧 CONFIG ======================
structure_path = r"D:\Workspace-Python\Data-Warehouse\Data Warehouse.xlsx"
table_name = "dim_products"
source_label = "TGDD"

def get_connection():
    return mysql.connector.connect(
        host="localhost",
        user="root",
        password="",
        database="staging"
    )

# ====================== ⚙️ ĐỌC CONFIG DB ======================
def load_db_config(path="config/db_config.yaml"):
    with open(path, "r", encoding="utf-8") as f:
        return yaml.safe_load(f)["mysql"]


# ====================== 📥 ĐỌC STAGING TỪ DATABASE ======================
def read_staging_from_db():
    DB_CONFIG = load_db_config()
    conn = mysql.connector.connect(**DB_CONFIG)
    query = "SELECT * FROM `staging.rawtgdd`"
    df = pd.read_sql(query, conn)
    conn.close()

    # ✅ Chuẩn hóa tên cột staging
    df.columns = [
        re.sub(r'[^a-z0-9]+', '_', unidecode(c).lower()).strip('_')
        for c in df.columns
    ]
    print("🧾 Cột staging sau chuẩn hóa:", df.columns.tolist())
    print(f"📊 Đã đọc {len(df)} dòng từ staging.rawtgdd")
    return df


# ====================== 📘 ĐỌC CẤU TRÚC DIM_PRODUCTS ======================
def get_table_columns(conn, table_name):
    """Lấy danh sách cột của một bảng trong MySQL"""
    query = f"SHOW COLUMNS FROM {table_name};"
    with conn.cursor() as cursor:
        cursor.execute(query)
        cols = [row[0] for row in cursor.fetchall()]
    print(f"📘 Đã đọc {len(cols)} cột từ {table_name}: {cols[:5]}...")
    return cols


# ====================== 🤖 AUTO MAPPING ======================
def normalize_column_name(name):
    name = unidecode(name)
    name = re.sub(r'[^a-zA-Z0-9\s_]', ' ', name)
    name = name.replace("_", " ")
    name = re.sub(r'\s+', ' ', name).strip().lower()
    return name


def similarity(a, b):
    return SequenceMatcher(None, a, b).ratio()


def auto_map_columns(dim_fields, staging_cols, cutoff=0.5):
    mapping = {}
    for dim_col in dim_fields:
        best_match, best_score = None, 0
        dim_norm = normalize_column_name(dim_col)
        for stg_col in staging_cols:
            stg_norm = normalize_column_name(stg_col)
            score = similarity(dim_norm, stg_norm)
            if score > best_score:
                best_match, best_score = stg_col, score
        mapping[dim_col] = best_match if best_score >= cutoff else None
    return mapping


# ====================== 💾 LOAD TO MYSQL ======================
def load_to_mysql(df, table_name):
    DB_CONFIG = load_db_config()
    conn = mysql.connector.connect(**DB_CONFIG)
    cursor = conn.cursor()

    cols = list(df.columns)
    placeholders = ",".join(["%s"] * len(cols))
    update_clause = ",".join([f"{c}=VALUES({c})" for c in cols if c != "product_id"])

    sql = f"""
        INSERT INTO {table_name} ({','.join(cols)})
        VALUES ({placeholders})
        ON DUPLICATE KEY UPDATE {update_clause}
    """

    for _, row in df.iterrows():
        try:
            cursor.execute(sql, tuple(row))
        except Exception as e:
            print(f"⚠️ Lỗi khi ghi dòng {row.get('product_id', '')}: {e}")

    conn.commit()
    cursor.close()
    conn.close()
    print(f"✅ Đã nạp {len(df)} dòng vào {table_name}")


# ====================== 🚀 MAIN ETL ======================
if __name__ == "__main__":
    conn = get_connection()
    df_staging = read_staging_from_db()
    dim_fields = get_table_columns(conn, "dim_products")

    print("\n🔍 --- Auto Mapping ---")
    mapping = auto_map_columns(dim_fields, df_staging.columns)


    # --- Áp dụng manual mapping bổ sung ---
    manual_map = {
        "product_name": "ten_san_pham",
        "product_price": "gia",
        "operating_system": "he_dieu_hanh",
        "cpu_chip": "chip_xu_ly_cpu",
        "cpu_speed": "toc_do_cpu",
        "gpu_chip": "chip_do_hoa_gpu",
        "ram": "ram",
        "storage_capacity": "dung_luong_luu_tru",
        "available_storage": "dung_luong_con_lai_kha_dung_khoang",
        "brand": "hang",
        "cong_ket_noi_sac": "cong_ket_noi_sac"
    }

    for k, v in manual_map.items():
        if v in df_staging.columns:
            mapping[k] = v

    print("\n📘 Mapping sau khi hợp nhất:")
    for k, v in mapping.items():
        print(f"  {k:25} ← {v}")

    # --- Tạo df_dim ---
    df_dim = pd.DataFrame(columns=dim_fields)
    for dim_col, stg_col in mapping.items():
        if stg_col and stg_col in df_staging.columns:
            df_dim[dim_col] = df_staging[stg_col]
        else:
            df_dim[dim_col] = None

    # --- Metadata ---
    now_vn = datetime.now(ZoneInfo("Asia/Ho_Chi_Minh"))
    df_dim["dt_expired"] = now_vn.strftime("%Y-%m-%d %H:%M:%S")
    df_dim["source_file"] = source_label

    # --- Product ID tự động ---
    if "product_id" in df_dim.columns and df_dim["product_id"].isna().all():
        df_dim["product_id"] = [f"P{i:05d}" for i in range(1, len(df_dim) + 1)]

    # --- Ghi vào DB ---
    load_to_mysql(df_dim, table_name)
