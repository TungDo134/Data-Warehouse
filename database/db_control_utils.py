import mysql.connector
import yaml

from utils.format_path import resource_path

# ĐỌC CONFIG CỦA data_control

config_file = resource_path("config/db_config.yaml")

def load_db_config(path="config/db_config.yaml"):
    with open(config_file, "r", encoding="utf-8") as f:
        return yaml.safe_load(f)["data_control"]


# 🔹 Kết nối đến DB và lấy config crawl
def get_crawl_config(config_name):
    db_conf = load_db_config()
    conn = mysql.connector.connect(**db_conf, auth_plugin='mysql_native_password')
    cursor = conn.cursor(dictionary=True)

    query = """
        SELECT source_url, target_table, max_clicks, record_limit, src_folder
        FROM `data_control.config`
        WHERE config_name = %s AND is_active = 1
        LIMIT 1
    """
    cursor.execute(query, (config_name,))
    result = cursor.fetchone()

    cursor.close()
    conn.close()
    return result
