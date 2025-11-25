# utils/log_utils.py
"""
Module quản lý logging cho ETL Pipeline
Ghi log vào database product_logdb.etl_logs
"""

import datetime
from sqlalchemy import create_engine, text


def write_log(process_name, source_system, target_table, status, message="", rows=None):
    """
    Ghi log vào database

    Args:
        process_name (str): Tên process (e.g., "EXTRACT", "MAIN_PIPELINE_START")
        source_system (str): Nguồn dữ liệu (e.g., "TGDĐ", "SYSTEM")
        target_table (str): Bảng đích (e.g., "STAGING", "DW")
        status (str): Trạng thái (SUCCESS/FAILED/RUNNING)
        message (str): Thông điệp bổ sung
        rows (int, optional): Số dòng xử lý

    Returns:
        int: log_id nếu thành công, None nếu thất bại
    """
    log_time = datetime.datetime.now()

    try:
        engine = create_engine("mysql+mysqlconnector://root:@localhost:3306/product_logdb")

        insert_log_sql = text("""
                              INSERT INTO etl_logs(process_name, source_system, target_table, status, message,
                                                   rows_affected, log_time)
                              VALUES (:process_name, :source_system, :target_table, :status, :message, :rows_affected,
                                      :log_time)
                              """)

        with engine.begin() as conn:
            result = conn.execute(insert_log_sql, {
                "process_name": process_name,
                "source_system": source_system,
                "target_table": target_table,
                "status": status,
                "message": message,
                "rows_affected": rows,
                "log_time": log_time
            })

            return result.lastrowid

    except Exception as e:
        print(f"❌ Failed to write log to database: {e}")
        return None


def write_pipeline_log(step_name, status, message="", script_name=None):
    """
    Ghi log cho Main Pipeline (wrapper function cho dễ sử dụng)

    Args:
        step_name (str): Tên bước (e.g., "START", "CRAWL_DATA", "COMPLETE")
        status (str): SUCCESS/FAILED/RUNNING
        message (str): Thông điệp
        script_name (str, optional): Tên file script đang chạy
    """
    if script_name:
        full_message = f"[{script_name}] {message}"
    else:
        full_message = message

    process_name = f"MAIN_PIPELINE_{step_name}"

    write_log(
        process_name=process_name,
        source_system="TGDĐ",
        target_table="DW",
        status=status,
        message=full_message
    )