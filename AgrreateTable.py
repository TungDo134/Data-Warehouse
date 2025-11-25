import mysql.connector
from yaml import safe_load
# CONFIG 
structure_path = r"D:\Workspace-Python\Data-Warehouse\Data Warehouse.xlsx"
table_name = "dim_products"
source_label = "TGDD"
#  LOAD DB CONFIG
def load_db_config(path="config/db_config.yaml"):
    with open(path, "r", encoding="utf-8") as f:
        return safe_load(f)["mysql"]


#  CALL PROCEDURES
def call_etl_procedures():
    DB_CONFIG = load_db_config()

    conn = mysql.connector.connect(
        host=DB_CONFIG["host"],
        user=DB_CONFIG["user"],
        password=DB_CONFIG["password"],
        database=DB_CONFIG["database"]
    )
    cursor = conn.cursor()

    procedures = [
        "data_mart.sp_load_agg_product_summary",
        "data_mart.sp_load_agg_price_history",
        "data_mart.sp_load_agg_brand_summary",
        "data_mart.sp_load_agg_crawl_daily"
    ]

    print("\n================ GỌI 3 STORED PROCEDURE =================")

    for proc in procedures:
        try:
            print(f"--- Đang chạy: {proc} ...")
            cursor.callproc(proc)
            conn.commit()
            print(f"------ {proc} ✓ hoàn thành.")
        except Exception as e:
            print(f"❌ Lỗi khi chạy {proc}: {e}")

    cursor.close()
    conn.close()

    print("=============== COMPLETE =================\n")


#  MAIN
if __name__ == "__main__":
    call_etl_procedures()
