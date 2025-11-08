import os
import re
import yaml
import pandas as pd
import mysql.connector
from datetime import datetime
from zoneinfo import ZoneInfo
from unidecode import unidecode
from sqlalchemy import create_engine


# ======================CONFIG ======================
structure_path = r"D:\Workspace-Python\Data-Warehouse\Data Warehouse.xlsx"
table_name = "dim_products"
source_label = "TGDD"


def get_connection():
    return mysql.connector.connect(
        host="localhost",
        user="root",
        password="",
        database="data_storage"
    )


def load_db_config(path="config/db_config.yaml"):
    with open(path, "r", encoding="utf-8") as f:
        return yaml.safe_load(f)["mysql"]


# ======================TẠO BẢNG DIM TỪ EXCEL ======================
def create_table_from_excel(excel_path, sheet_name, conn, db_name="data_storage"):
    df_struct = pd.read_excel(excel_path, sheet_name=sheet_name, usecols="A:C", header=9)
    df_struct.columns = [c.strip().lower().replace(" ", "_") for c in df_struct.columns]
    col_field = next((c for c in df_struct.columns if "field" in c), None)
    col_type = next((c for c in df_struct.columns if "type" in c), None)
    df_struct = df_struct[df_struct[col_field].notna()]

    sql_cols = []
    pk_cols = []

    for _, row in df_struct.iterrows():
        field = str(row[col_field]).strip()
        dtype = str(row[col_type]).strip().upper()
        if "VARCHAR" in dtype:
            dtype = "VARCHAR(255)"
            print(f"{field}: {dtype}")

        is_pk = field.lower() == "product_id"
        not_null = "NOT NULL" if is_pk else ""
        sql_cols.append(f"`{field}` {dtype} {not_null}")
        if is_pk:
            pk_cols = ["product_id"]

    pk_clause = f", PRIMARY KEY ({','.join([f'`{c}`' for c in pk_cols])})" if pk_cols else ""
    create_sql = f"CREATE TABLE IF NOT EXISTS `{db_name}`.`{sheet_name}` ({', '.join(sql_cols)} {pk_clause}) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;"

    print("\n______SQL sinh ra:")
    print(create_sql)
    with conn.cursor() as cursor:
        cursor.execute(create_sql)
        conn.commit()
    print(f"+++++++++Bảng `{sheet_name}` đã được tạo trong database `{db_name}`.")


# ====================== ĐỌC STAGING ======================
def read_staging_from_db():
    DB_CONFIG = load_db_config()
    conn_str = f"mysql+mysqlconnector://{DB_CONFIG['user']}:{DB_CONFIG['password']}@{DB_CONFIG['host']}/{DB_CONFIG['database']}"
    engine = create_engine(conn_str)
    df = pd.read_sql("SELECT * FROM `staging`.`staging.rawtgdd`", engine)
    engine.dispose()
    df.columns = [re.sub(r'[^a-z0-9]+', '_', unidecode(c).lower()).strip('_') for c in df.columns]
    print("+++++++++Cột staging sau chuẩn hóa:", df.columns.tolist())
    print(f"+++++++++Đã đọc {len(df)} dòng từ staging.rawtgdd")
    return df


# ======================LẤY CỘT DIM ======================
def get_table_columns(conn, table_name):
    with conn.cursor() as cursor:
        cursor.execute(f"SHOW COLUMNS FROM data_storage.{table_name};")
        cols = [row[0] for row in cursor.fetchall()]
    print(f"+++++++++Đã đọc {len(cols)} cột từ {table_name}: {cols[:5]}...")
    return cols


# ====================== LOAD TO MYSQL ======================
def load_to_mysql(df, table_name):
    DB_CONFIG = load_db_config()
    conn = mysql.connector.connect(**DB_CONFIG)
    cursor = conn.cursor()
    cols = list(df.columns)
    placeholders = ",".join(["%s"] * len(cols))
    col_names = ",".join([f"`{c}`" for c in cols])
    update_clause = ",".join([f"`{c}`=VALUES(`{c}`)" for c in cols if c != "product_id"])
    sql = f"INSERT INTO `data_storage`.`{table_name}` ({col_names}) VALUES ({placeholders}) ON DUPLICATE KEY UPDATE {update_clause}"
    for _, row in df.iterrows():
        try:
            cursor.execute(sql, tuple(row))
        except Exception as e:
            print(f"**Lỗi khi ghi dòng {row.get('product_id', '')}: {e}**")
    conn.commit()
    cursor.close()
    conn.close()
    print(f"---Đã nạp {len(df)} dòng vào {table_name}")


# ======================MAPPING CỨNG ======================
manual_mapping = {
    "product_name": "ten_san_pham",
    "product_price": "gia",
    "operating_system": "he_dieu_hanh",
    "cpu_chip": "chip_xu_ly_cpu",
    "cpu_speed": "toc_do_cpu",
    "gpu_chip": "chip_do_hoa_gpu",
    "ram": "ram",
    "storage_capacity": "dung_luong_luu_tru",
    "available_storage": "dung_luong_con_lai_kha_dung_khoang",
    "contacts": "danh_ba",
    "rear_camera_resolution": "do_phan_giai_camera_sau",
    "rear_camera_video": "quay_phim_camera_sau",
    "rear_camera_flash": "den_flash_camera_sau",
    "rear_camera_features": "tinh_nang_camera_sau",
    "front_camera_resolution": "do_phan_giai_camera_truoc",
    "front_camera_features": "tinh_nang_camera_truoc",
    "display_technology": "cong_nghe_man_hinh",
    "display_resolution": "do_phan_giai_man_hinh",
    "screen_size": "man_hinh_rong",
    "max_brightness": "do_sang_toi_da",
    "touch_glass": "mat_kinh_cam_ung",
    "battery_capacity": "dung_luong_pin",
    "battery_type": "loai_pin",
    "max_charging_support": "ho_tro_sac_toi_da",
    "battery_technology": "cong_nghe_pin",
    "security_features": "bao_mat_nang_cao",
    "special_features": "tinh_nang_dac_biet",
    "water_dust_resistance": "khang_nuoc_bui",
    "voice_recorder": "ghi_am",
    "video_playback": "xem_phim",
    "music_playback": "nghe_nhac",
    "mobile_network": "mang_di_dong",
    "sim_type": "sim",
    "wifi_support": "wifi",
    "gps_support": "gps",
    "bluetooth_version": "bluetooth",
    "cong_ket_noi/sac": "cong_ket_noi_sac",
    "headphone_jack": "jack_tai_nghe",
    "other_connections": "ket_noi_khac",
    "design_style": "thiet_ke",
    "material": "chat_lieu",
    "dimensions_weight": "kich_thuoc_khoi_luong",
    "release_date": "thoi_diem_ra_mat",
    "brand": "hang"
}


# ======================MAIN ETL ======================
if __name__ == "__main__":
    conn = get_connection()
    create_table_from_excel(structure_path, table_name, conn)
    df_staging = read_staging_from_db()
    dim_fields = get_table_columns(conn, table_name)

    # Áp dụng manual mapping
    df_dim = pd.DataFrame(columns=dim_fields)
    df_staging.columns = [c.strip().lower() for c in df_staging.columns]

    for dim_col in dim_fields:
        stg_col = manual_mapping.get(dim_col)
        if stg_col and stg_col in df_staging.columns:
            df_dim[dim_col] = df_staging[stg_col]
        else:
            df_dim[dim_col] = None

    # Metadata
    now_vn = datetime.now(ZoneInfo("Asia/Ho_Chi_Minh"))
    if "dt_expired" in df_dim.columns:
        df_dim["dt_expired"] = now_vn.strftime("%Y-%m-%d %H:%M:%S")
    if "source_file" in df_dim.columns:
        df_dim["source_file"] = source_label
    if "product_id" in df_dim.columns and df_dim["product_id"].isna().all():
        df_dim["product_id"] = [f"P{i:05d}" for i in range(1, len(df_dim) + 1)]

    df_dim = df_dim.where(pd.notnull(df_dim), None)
    print(df_dim.head())

    # === Xuất file Excel với timestamp ===
    output_dir = r"D:\Workspace-Python\Data-Warehouse\Data_storage_DIM"
    os.makedirs(output_dir, exist_ok=True)  # tạo thư mục nếu chưa có

    # tạo tên file với timestamp
    now_vn = datetime.now(ZoneInfo("Asia/Ho_Chi_Minh"))
    timestamp = now_vn.strftime("%Y_%m_%d_%H_%M_%S")
    output_file = os.path.join(output_dir, f"dim_product_{timestamp}.xlsx")

    df_dim.to_excel(output_file, index=False)
    print(f"+++++++++Đã xuất file {output_file}")

    load_to_mysql(df_dim, table_name)
