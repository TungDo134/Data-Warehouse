import mysql.connector
from datetime import datetime
from zoneinfo import ZoneInfo
import yaml

# --- Đọc file config.yaml ---
def load_db_config(path="config/db_config.yaml"):
    with open(path, "r", encoding="utf-8") as f:
        return yaml.safe_load(f)["mysql"]

def load_to_mysql(df, table_name, source_file, config_path="config/db_config.yaml"):
    DB_CONFIG = load_db_config(config_path)

    # --- 1️⃣ Kết nối MySQL server (chưa cần DB) ---
    conn = mysql.connector.connect(
        host=DB_CONFIG["host"],
        user=DB_CONFIG["user"],
        password=DB_CONFIG["password"],
        port=DB_CONFIG["port"]
    )
    cursor = conn.cursor()

    # --- 2️⃣ Tạo database nếu chưa có ---
    try:
        cursor.execute(f"CREATE DATABASE IF NOT EXISTS `{DB_CONFIG['database']}` DEFAULT CHARACTER SET utf8mb4;")
        print(f"✅ Database `{DB_CONFIG['database']}` đã sẵn sàng.")
    except mysql.connector.Error as err:
        print(f"❌ Lỗi khi tạo database: {err}")
        return

    conn.database = DB_CONFIG["database"]

    # --- 3️⃣ Thêm cột meta ---
    now_vn = datetime.now(ZoneInfo("Asia/Ho_Chi_Minh"))
    df["created_at"] = now_vn.strftime("%Y-%m-%d %H:%M:%S")
    df["source_file"] = source_file

    # --- 4️⃣ Chuyển toàn bộ sang TEXT để tránh lỗi data type ---
    df = df.astype(str)

    # --- 5️⃣ Tạo bảng nếu chưa có ---
    columns_sql = ", ".join([f"`{col}` TEXT" for col in df.columns])
    create_sql = f"""
        CREATE TABLE IF NOT EXISTS `{table_name}` (
            id INT AUTO_INCREMENT PRIMARY KEY,
            {columns_sql}
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
    """
    cursor.execute(create_sql)

    # --- 6️⃣ Đồng bộ cột giữa DB và DataFrame ---
    cursor.execute(f"SHOW COLUMNS FROM `{table_name}`")
    db_columns = [c[0] for c in cursor.fetchall()]

    # Nếu DataFrame có cột mới → thêm vào DB
    # for col in df.columns:
    #     if col not in db_columns:
    #         cursor.execute(f"ALTER TABLE `{table_name}` ADD COLUMN `{col}` TEXT;")
    #         print(f"🆕 Thêm cột mới vào bảng: {col}")

    # Nếu DB có cột mà DataFrame không có → thêm cột trống vào df
    for col in db_columns:
        if col not in df.columns and col not in ("id",):
            df[col] = ""

    # Reorder df để khớp với bảng DB
    df = df[[c for c in db_columns if c != "id"]]

    # --- 7️⃣ Upsert thông minh ---
    cursor.execute(f"SELECT * FROM `{table_name}`")
    existing_rows = cursor.fetchall()
    existing_cols = [desc[0] for desc in cursor.description]

    def row_key(row):
        """Xác định khóa nhận dạng: có thể đổi tuỳ dataset (vd: Tên sản phẩm)"""
        return row.get("Tên sản phẩm")  # 👈 chỉnh lại nếu dataset khác

    existing_dict = {}
    for r in existing_rows:
        row_data = dict(zip(existing_cols, r))
        key = row_key(row_data)
        if key:
            existing_dict[key] = row_data

    insert_count, update_count, skip_count = 0, 0, 0

    for _, row in df.iterrows():
        key = row_key(row)
        if not key:
            continue

        if key in existing_dict:
            # So sánh các giá trị trừ meta
            old = existing_dict[key]
            different = any(
                str(row[c]) != str(old.get(c, ""))
                for c in df.columns if c not in ("created_at", "source_file")
            )
            if different:
                set_clause = ", ".join([f"`{c}`=%s" for c in df.columns])
                update_sql = f"UPDATE `{table_name}` SET {set_clause} WHERE `Tên sản phẩm`=%s"
                cursor.execute(update_sql, [str(row[c]) for c in df.columns] + [key])
                update_count += 1
            else:
                skip_count += 1
        else:
            cols = ", ".join([f"`{c}`" for c in df.columns])
            placeholders = ", ".join(["%s"] * len(df.columns))
            insert_sql = f"INSERT INTO `{table_name}` ({cols}) VALUES ({placeholders})"
            cursor.execute(insert_sql, tuple(row[c] for c in df.columns))
            insert_count += 1

    conn.commit()
    print(f"✅ Đã insert {insert_count}, update {update_count}, bỏ qua {skip_count} dòng ({source_file})")

    cursor.close()
    conn.close()
