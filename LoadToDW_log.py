import os
import re
import yaml
import pandas as pd
import mysql.connector
from datetime import datetime
from zoneinfo import ZoneInfo
from unidecode import unidecode
from sqlalchemy import create_engine, text

# Import log utilities (giả sử có file utils/log_utils.py)
try:
    from utils.log_utils import write_log
except ImportError:
    # Fallback nếu chưa có module log_utils
    def write_log(process_name, source_system, target_table, status, message="", rows=None, product_data=None):
        """Hàm log dự phòng - ghi vào database"""
        log_time = datetime.now()
        try:
            engine = create_engine("mysql+mysqlconnector://root:@localhost:3306/product_logdb")
            insert_log_sql = text("""
                                  INSERT INTO etl_logs(process_name, source_system, target_table, status, message,
                                                       rows_affected, log_time)
                                  VALUES (:process_name, :source_system, :target_table, :status, :message,
                                          :rows_affected,
                                          :log_time)
                                  """)
            with engine.begin() as conn:
                conn.execute(insert_log_sql, {
                    "process_name": process_name,
                    "source_system": source_system,
                    "target_table": target_table,
                    "status": status,
                    "message": message,
                    "rows_affected": rows,
                    "log_time": log_time
                })
        except Exception as e:
            print(f"❌ Failed to write log: {e}")

# ======================CONFIG ======================
structure_path = r"D:\Workspace-Python\Data-Warehouse\Data Warehouse.xlsx"
table_name = "dim_products"
source_label = "TGDD"

# 5.3.1.0 KẾT NỐI DB
def get_connection():
    return mysql.connector.connect(
        host="localhost",
        user="root",
        password="",
        database="data_storage"
    )

# 5.3.1.1 Đọc config từ YAML
def load_db_config(path="config/db_config.yaml"):
    with open(path, "r", encoding="utf-8") as f:
        return yaml.safe_load(f)["mysql"]


# ======================TẠO BẢNG DIM TỪ EXCEL ======================
# 5.3.1.2 TẠO BẢNG DIM TỪ EXCEL
def create_table_from_excel(excel_path, sheet_name, conn, db_name="data_storage"):
    """Tạo bảng DIM từ cấu trúc Excel"""
    try:
        print("\n📋 Đang đọc cấu trúc bảng từ Excel...")

        df_struct = pd.read_excel(excel_path, sheet_name=sheet_name, usecols="A:C", header=9)
        df_struct.columns = [c.strip().lower().replace(" ", "_") for c in df_struct.columns]
        col_field = next((c for c in df_struct.columns if "field" in c), None)
        col_type = next((c for c in df_struct.columns if "type" in c), None)
        df_struct = df_struct[df_struct[col_field].notna()]

        sql_cols = ["`id` INT NOT NULL AUTO_INCREMENT"]
        pk_cols = ["id"]

        for _, row in df_struct.iterrows():
            field = str(row[col_field]).strip()
            dtype = str(row[col_type]).strip().upper()

            if field == "product_id":
                dtype = "VARCHAR(50)"
                sql_cols.append(f"`{field}` {dtype} NOT NULL UNIQUE")
            elif dtype in ["VARCHAR", "TEXT"] or field in ["product_name", "operating_system", "cpu_chip", "cpu_speed"]:
                dtype = "TEXT"
                sql_cols.append(f"`{field}` {dtype} NULL")
            elif field == "product_price":
                dtype = "DECIMAL(18,2)"
                sql_cols.append(f"`{field}` {dtype} NULL")
            elif field == "release_date":
                dtype = "DATE"
                sql_cols.append(f"`{field}` {dtype} NULL")
            else:
                sql_cols.append(f"`{field}` {dtype} NULL")

        pk_clause = f", PRIMARY KEY ({','.join([f'`{c}`' for c in pk_cols])})"
        create_sql = f"CREATE TABLE IF NOT EXISTS `{db_name}`.`{sheet_name}` ({', '.join(sql_cols)} {pk_clause}) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;"

        print(f"✅ SQL sinh ra thành công")
        with conn.cursor() as cursor:
            cursor.execute(create_sql)
            conn.commit()

        print(f"✅ Bảng `{sheet_name}` đã được tạo/kiểm tra trong database `{db_name}`")

        write_log(
            process_name="CREATE_TABLE_DIM",
            source_system=source_label,
            target_table=sheet_name,
            status="SUCCESS",
            message=f"Tạo/kiểm tra bảng {sheet_name} thành công"
        )

    except Exception as e:
        error_msg = f"Lỗi khi tạo bảng {sheet_name}: {str(e)}"
        print(f"❌ {error_msg}")

        write_log(
            process_name="CREATE_TABLE_DIM",
            source_system=source_label,
            target_table=sheet_name,
            status="FAILED",
            message=error_msg
        )
        raise


# ====================== ĐỌC STAGING ======================
# 5.3.1.3 ĐỌC STAGING
def read_staging_from_db():
    """Đọc dữ liệu từ staging database"""
    try:
        print("\n📥 Đang đọc dữ liệu từ staging...")

        DB_CONFIG = load_db_config()
        conn_str = f"mysql+mysqlconnector://{DB_CONFIG['user']}:{DB_CONFIG['password']}@{DB_CONFIG['host']}/{DB_CONFIG['database']}"
        engine = create_engine(conn_str)
        df = pd.read_sql("SELECT * FROM `staging`.`staging.rawtgdd`", engine)
        engine.dispose()

        # 5.3.1.4 Chuẩn hóa tên cột staging
        df.columns = [re.sub(r'[^a-z0-9]+', '_', unidecode(c).lower()).strip('_') for c in df.columns]

        print(f"✅ Đã đọc {len(df)} dòng từ staging.rawtgdd")
        print(f"📊 Cột staging: {df.columns.tolist()[:5]}...")

        write_log(
            process_name="READ_STAGING",
            source_system=source_label,
            target_table="staging.rawtgdd",
            status="SUCCESS",
            rows=len(df),
            message=f"Đọc staging thành công với {len(df)} dòng"
        )

        return df

    except Exception as e:
        error_msg = f"Lỗi khi đọc staging: {str(e)}"
        print(f"❌ {error_msg}")

        write_log(
            process_name="READ_STAGING",
            source_system=source_label,
            target_table="staging.rawtgdd",
            status="FAILED",
            message=error_msg
        )
        raise


# ======================LẤY CỘT DIM ======================
# 5.3.1.5 LẤY CỘT DIM
def get_table_columns(conn, table_name):
    """Lấy danh sách cột từ bảng DIM"""
    try:
        with conn.cursor() as cursor:
            cursor.execute(f"SHOW COLUMNS FROM data_storage.{table_name};")
            cols = [row[0] for row in cursor.fetchall()]

        print(f"✅ Đã đọc {len(cols)} cột từ {table_name}")
        return cols

    except Exception as e:
        print(f"❌ Lỗi khi đọc cột từ {table_name}: {e}")
        raise


# ====================== LOAD TO MYSQL ======================
# 5.3.1.10 LOAD TO MYSQL (UPSERT)
def load_to_mysql(df, table_name):
    """Load dữ liệu vào MySQL với INSERT ... ON DUPLICATE KEY UPDATE"""
    try:
        print(f"\n💾 Đang load {len(df)} dòng vào {table_name}...")

        DB_CONFIG = load_db_config()
        conn = mysql.connector.connect(**DB_CONFIG)
        cursor = conn.cursor()

        cols = [c for c in df.columns if c != "id"]
        placeholders = ",".join(["%s"] * len(cols))
        col_names = ",".join([f"`{c}`" for c in cols])
        update_clause = ",".join([f"`{c}`=VALUES(`{c}`)" for c in cols if c != "id"])

        sql = f"""
        INSERT INTO `data_storage`.`{table_name}` ({col_names})
        VALUES ({placeholders})
        ON DUPLICATE KEY UPDATE {update_clause}
        """

        success_count = 0
        error_count = 0

        for _, row in df.iterrows():
            values = [row[c] for c in cols]
            try:
                cursor.execute(sql, values)
                success_count += 1
            except Exception as e:
                error_count += 1
                print(f"⚠️ Lỗi khi ghi dòng {row.get('product_name', '')}: {e}")

        conn.commit()
        cursor.close()
        conn.close()

        print(f"✅ Đã nạp {success_count} dòng vào {table_name}")
        if error_count > 0:
            print(f"⚠️ {error_count} dòng bị lỗi")

        write_log(
            process_name="LOAD_DIM_MYSQL",
            source_system=source_label,
            target_table=table_name,
            status="SUCCESS",
            rows=success_count,
            message=f"Load thành công {success_count}/{len(df)} dòng vào {table_name}"
        )

    except Exception as e:
        error_msg = f"Lỗi khi load vào MySQL: {str(e)}"
        print(f"❌ {error_msg}")

        write_log(
            process_name="LOAD_DIM_MYSQL",
            source_system=source_label,
            target_table=table_name,
            status="FAILED",
            message=error_msg
        )
        raise


# ======================MAPPING CỨNG ======================
# 5.3.1.6 MANUAL MAPPING
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
    print("=" * 60)
    print("🚀 BẮT ĐẦU LOAD DIM_PRODUCT")
    print("=" * 60)

    # Ghi log bắt đầu
    write_log(
        process_name="LOAD_DIM_START",
        source_system=source_label,
        target_table=table_name,
        status="RUNNING",
        message="Bắt đầu quá trình load DIM_PRODUCT"
    )

    try:
        # ========== BƯỚC 1: TẠO BẢNG ==========
        # 5.3.1.0 Kết nối DB
        conn = get_connection()
        # 5.3.1.2 Tạo bảng DIM
        create_table_from_excel(structure_path, table_name, conn)

        # ========== BƯỚC 2: ĐỌC STAGING ==========
        # 5.3.1.3 Đọc staging
        df_staging = read_staging_from_db()

        # ========== BƯỚC 3: LẤY CẤU TRÚC DIM ==========
        # 5.3.1.5 Đọc cấu trúc cột DIM
        dim_fields = get_table_columns(conn, table_name)

        # ========== BƯỚC 4: MAPPING DỮ LIỆU ==========

        print("\n🔄 Đang mapping dữ liệu từ staging sang DIM...")

        # 5.3.1.6 Bắt đầu Mapping
        df_dim = pd.DataFrame(columns=dim_fields)
        df_staging.columns = [c.strip().lower() for c in df_staging.columns]

        for dim_col in dim_fields:
            stg_col = manual_mapping.get(dim_col)
            if stg_col and stg_col in df_staging.columns:
                df_dim[dim_col] = df_staging[stg_col]
            else:
                df_dim[dim_col] = None

        original_count = len(df_dim)

        # ========== BƯỚC 5: CLEAN DỮ LIỆU ==========
        print("\n🧹 Đang clean dữ liệu...")
        # 5.3.1.7 Xử lý dữ liệu thiếu
        required_cols = ["release_date", "product_name", "product_price"]
        df_dim = df_dim.dropna(subset=required_cols)

        df_dim["release_date"] = pd.to_datetime(
            df_dim["release_date"], errors="coerce", format="%m/%Y"
        )
        # chuyển release_date
        df_dim = df_dim[df_dim["release_date"].notna()]
        df_dim["release_date"] = df_dim["release_date"].dt.strftime("%Y-%m-%d")
        # tạo product_id
        if "product_id" in df_dim.columns:
            df_dim["product_id"] = df_dim["product_id"].fillna(
                pd.Series([f"P{i:05d}" for i in range(1, len(df_dim) + 1)], index=df_dim.index)
            )
        # 5.3.1.8 Thêm metadata
        now_vn = datetime.now(ZoneInfo("Asia/Ho_Chi_Minh"))
        if "dt_expired" in df_dim.columns:
            df_dim["dt_expired"] = now_vn.strftime("%Y-%m-%d %H:%M:%S")
        if "source_file" in df_dim.columns:
            df_dim["source_file"] = source_label
        # 5.3.1.9 Chuyển NaN → None
        df_dim = df_dim.where(pd.notnull(df_dim), None)

        cleaned_count = len(df_dim)
        removed_count = original_count - cleaned_count
        print(f"✅ Clean hoàn tất: Giữ lại {cleaned_count}/{original_count} dòng (loại bỏ {removed_count} dòng)")

        write_log(
            process_name="TRANSFORM_DIM",
            source_system=source_label,
            target_table=table_name,
            status="SUCCESS",
            rows=cleaned_count,
            message=f"Mapping và clean thành công. Loại bỏ {removed_count} dòng không hợp lệ"
        )

        # ========== BƯỚC 6: XUẤT FILE EXCEL ==========
        print("\n📄 Đang xuất file Excel...")
        # 5.3.1.10 Xuất Excel DIM
        output_dir = r"D:\Workspace-Python\Data-Warehouse\Data_storage_DIM"
        os.makedirs(output_dir, exist_ok=True)

        timestamp = now_vn.strftime("%Y_%m_%d_%H_%M_%S")
        output_file = os.path.join(output_dir, f"dim_product_{timestamp}.xlsx")

        df_dim.to_excel(output_file, index=False)
        print(f"✅ Đã xuất file: {output_file}")

        write_log(
            process_name="EXPORT_DIM_EXCEL",
            source_system=source_label,
            target_table=table_name,
            status="SUCCESS",
            rows=cleaned_count,
            message=f"Xuất file Excel thành công: {os.path.basename(output_file)}"
        )

        # ========== BƯỚC 7: LOAD VÀO MYSQL ==========
        # 5.3.1.11 LOAD vào MYSQL
        load_to_mysql(df_dim, table_name)

        # ========== HOÀN THÀNH ==========
        print("\n" + "=" * 60)
        print(f"✅ HOÀN TẤT LOAD DIM_PRODUCT - Xử lý {cleaned_count} sản phẩm")
        print("=" * 60)

        write_log(
            process_name="LOAD_DIM_COMPLETE",
            source_system=source_label,
            target_table=table_name,
            status="SUCCESS",
            rows=cleaned_count,
            message=f"Load DIM_PRODUCT hoàn thành thành công với {cleaned_count} sản phẩm"
        )

    except Exception as e:
        error_msg = f"Lỗi trong quá trình load DIM: {str(e)}"
        print(f"\n❌ {error_msg}")

        write_log(
            process_name="LOAD_DIM_FAILED",
            source_system=source_label,
            target_table=table_name,
            status="FAILED",
            message=error_msg
        )

        raise