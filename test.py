# Giả lập DataFrame
import os
from datetime import datetime
from zoneinfo import ZoneInfo
import pandas as pd
from database.db_utils import load_to_mysql

# Thư mục bạn muốn lưu file
output_dir = r"D:\Workspace-Python\DataWarehouse\TEST"

now_vn = datetime.now(ZoneInfo("Asia/Ho_Chi_Minh"))
timestamp = now_vn.strftime("%Y_%m_%d_%H_%M_%S")

# Tạo tên file có timestamp
filename = os.path.join(output_dir, f"tgdd_products_{timestamp}.xlsx")

df = pd.DataFrame({
    "Tên sản phẩm": ["iPhone 16 Pro Max 256GB"],
    "Giá": ["32000000"]
})

# load vào mysql
table_name = "staging.rawtgdd"
load_to_mysql(df, table_name, os.path.basename(filename))
