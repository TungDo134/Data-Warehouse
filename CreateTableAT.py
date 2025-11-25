import mysql.connector
from yaml import safe_load

# CONFIG
SQL_FILE_PATH = r"D:\Workspace-Python\Data-Warehouse\sqlAT.sql"  # đường dẫn file SQL chứa CREATE TABLE/PROC...
DB_NAME = "data_mart"

# LOAD DB CONFIG
def load_db_config(path="config/db_config.yaml"):
    """
    Workflow step: đọc cấu hình DB từ YAML.
    - Input: path tới file config
    - Output: dict cấu hình mysql (host,user,password,...)
    Ghi chú: nếu cần môi trường (dev/prod) -> thêm key env và chọn tương ứng.
    """
    with open(path, "r", encoding="utf-8") as f:
        return safe_load(f)["mysql"]


# PARSE & EXEC SQL
def execute_sql_file(cursor, sql_path):
    """
    Workflow step: chạy file SQL theo delimiter.
    - Đọc file SQL nguyên văn (có thể chứa nhiều lệnh, thay đổi delimiter cho procedure)
    - Tách từng câu lệnh và execute bằng cursor.execute
    - Ghi log kết quả / lỗi để debug (hiện đang in ra console)
    """
    print(f"\n===== Đang chạy file SQL: {sql_path} =====\n")

    with open(sql_path, "r", encoding="utf-8") as f:
        sql_lines = f.readlines()

    sql_command = ""
    delimiter = ";"

    for line in sql_lines:
        # Bỏ comment dòng bắt đầu bằng --, /*, * (giữ nguyên phần comment trong SQL nếu cần)
        striped = line.strip()
        if striped.startswith("--") or striped.startswith("/*") or striped.startswith("*"):
            # giữ comment trong file SQL nhưng không gửi tới DB
            continue

        # Tìm directive thay đổi delimiter (ví dụ: DELIMITER $$)
        if striped.lower().startswith("delimiter "):
            # cập nhật delimiter; bước này quan trọng khi file chứa procedure
            delimiter = striped.split()[1]
            continue

        sql_command += line

        # Nếu gặp delimiter hiện tại → chạy câu SQL
        if striped.endswith(delimiter):
            final_sql = sql_command.strip().rstrip(delimiter).strip()
            try:
                cursor.execute(final_sql)
                # SUCCESS: trong production nên log chi tiết hơn (file hoặc monitoring)
                print(f"✓ Chạy thành công 1 lệnh SQL.")
            except Exception as e:
                # ERROR: in ra câu SQL gây lỗi + exception (rất hữu ích khi debug)
                print("\n❌ LỖI SQL:")
                print(final_sql)
                print("----")
                print(e)
                # Gợi ý: ở đây có thể append lỗi vào file log hoặc gửi alert
            sql_command = ""

# ===================== MAIN =====================
def setup_data_mart():
    """
    Workflow tổng quát (gồm các bước):
    1. Load config
    2. Kết nối tới MySQL server (chưa chọn DB)
    3. Tạo database nếu chưa có
    4. USE database
    5. Thực thi file SQL (tạo bảng aggregate, procedure ETL SCD2)
    6. Commit & close
    """
    DB_CONFIG = load_db_config()

    conn = mysql.connector.connect(
        host=DB_CONFIG["host"],
        user=DB_CONFIG["user"],
        password=DB_CONFIG["password"]
    )
    cursor = conn.cursor()

    print("\n=============== TẠO DATABASE data_mart =================")

    try:
        # Tạo DB nếu chưa có (bước 2)
        cursor.execute(f"CREATE DATABASE IF NOT EXISTS `{DB_NAME}` "
                       "CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_ai_ci;")
        print("✓ Database data_mart đã sẵn sàng.")
    except Exception as e:
        print(f"❌ Lỗi tạo database: {e}")
        # Gợi ý: ở production có thể raise hoặc dừng pipeline

    cursor.execute(f"USE `{DB_NAME}`;")  # chuyển context tới DB mới tạo

    # Bước 5: thực thi file SQL (tạo bảng aggregate, stored procedure, index...)
    execute_sql_file(cursor, SQL_FILE_PATH)
    conn.commit()

    cursor.close()
    conn.close()

    print("\n=============== COMPLETE =================\n")


if __name__ == "__main__":
    # Entry point: khi chạy script này, workflow khởi tạo DB + chạy SQL sẽ được thực hiện
    setup_data_mart()
