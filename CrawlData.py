# Cài đặt thư viện
import pandas as pd
import os
import time
import random
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from bs4 import BeautifulSoup
from datetime import datetime
from zoneinfo import ZoneInfo

from db_utils import load_to_mysql

# from selenium.webdriver.common.by import By
# from unidecode import unidecode
# from webdriver_manager.chrome import ChromeDriverManager




# ===================== ⚙️ CẤU HÌNH BAN ĐẦU =====================

category_url = "https://www.thegioididong.com/dtdd"

chrome_options = Options()
chrome_options.add_argument("--headless=new")
chrome_options.add_argument("--no-sandbox")
chrome_options.add_argument("--disable-dev-shm-usage")
chrome_options.add_argument("--disable-gpu")
chrome_options.add_argument("window-size=1920,1080")

driver = webdriver.Chrome(options=chrome_options)

# ===================== 🔹 BƯỚC 1: LẤY LINK + TÊN + GIÁ =====================

print(f"🔍 Đang thu thập danh sách sản phẩm từ: {category_url}")
driver.get(category_url)
time.sleep(3)

# --- Tự động click "Xem thêm" (giới hạn số lần thử) ---
max_clicks = 1
for i in range(max_clicks):
    try:
        view_more_btn = driver.find_element("css selector", ".view-more a")
        driver.execute_script("arguments[0].scrollIntoView(true);", view_more_btn)
        time.sleep(1)
        driver.execute_script("arguments[0].click();", view_more_btn)
        print(f"🔁 (Lần {i+1}/{max_clicks}) Đã click 'Xem thêm' để tải thêm sản phẩm...")
        time.sleep(random.uniform(2.5, 4.5))
    except Exception:
        print(f"✅ Dừng ở lần {i+1}: Không còn nút 'Xem thêm' hoặc đã load hết.")
        break
else:
    print("⚠️ Đã đạt giới hạn click tối đa, có thể trang chưa load hết.")

# --- Sau khi đã tải hết ---
soup = BeautifulSoup(driver.page_source, "lxml")
product_links = []
products = []

for a in soup.select("ul.listproduct a.main-contain"):
    href = a.get("href")
    if href and href.startswith("/dtdd/"):
        full_link = "https://www.thegioididong.com" + href
        product_links.append(full_link)

        # --- Tên sản phẩm ---
        name_tag = a.select_one("h3")
        product_name = name_tag.get_text(strip=True) if name_tag else "Không rõ"

        # --- Giá sản phẩm ---
        price_tag = a.select_one("strong.price")
        if price_tag:
            price_text = price_tag.get_text(strip=True).replace("₫", "").replace(".", "").strip()
            try:
                price = int(price_text)
            except ValueError:
                price = None
        else:
            price = None

        products.append({
            "Tên sản phẩm": product_name,
            "Giá": price,
            "Link": full_link
        })

print(f"✅ Tìm thấy {len(product_links)} sản phẩm sau khi load toàn trang.")


# ⚙️ Giới hạn số lượng sản phẩm để TEST (muốn full thì cmt lại)
limit = 10
product_links = product_links[:limit]
products = products[:limit]
print(f"🧪 Đang test với {len(products)} sản phẩm đầu tiên.")

# ===================== 🔹 BƯỚC 2: CRAWL CHI TIẾT =====================

all_data = []

# Hàm giúp phục hồi khi gặp lỗi mạng hoặc trang bị treo
# Giúp ctrinh vẫn chạy tiếp thay vì break
def safe_get(url, retries=3):
    """Tải trang với retry và timeout (ẩn traceback, chỉ in lỗi gọn)."""
    for attempt in range(retries):
        try:
            driver.set_page_load_timeout(20)
            driver.get(url)
            time.sleep(random.uniform(2.5, 4.5))
            return BeautifulSoup(driver.page_source, "lxml")
        except Exception as e:
            print(f"⚠️ Lỗi tải ({attempt+1}/{retries})")
            if attempt == retries - 1:
                print(f"❌ Bỏ qua {url}")
                return None
            time.sleep(2)
    return None

# Logic để lấy chi tiết sản phẩm
for i, base_info in enumerate(products, start=1):
    url = base_info["Link"]
    print(f"📦 ({i}/{len(products)}) Đang xử lý: {url}")

    soup = safe_get(url)
    if not soup:
        continue

    config = base_info.copy()
    for item in soup.select("ul.text-specifi li"):
        label_tag = item.find("strong") or item.find("a")
        label = label_tag.get_text(strip=True) if label_tag else None

        value_tags = item.select("span, a")
        values = [v.get_text(strip=True) for v in value_tags if v.get_text(strip=True)]

        if label and values and values[0] == label:
            values = values[1:]

        if label and values:
            config[label] = " | ".join(values)

    all_data.append(config)

print(f"🎯 Đã thu thập được {len(all_data)} sản phẩm hợp lệ.")
driver.quit()

# ===================== 🔹 BƯỚC 3: TẠO + LƯU FILE EXCEL =====================

# Thư mục bạn muốn lưu file
output_dir = r"D:\Workspace-Python\Data-Warehouse\Crawl Data"

# Tạo thư mục nếu chưa tồn tại
os.makedirs(output_dir, exist_ok=True)
df = pd.DataFrame(all_data)

# xóa cột không cần thiết
df = df.drop(columns=["Thẻ nhớ:", "Sạc kèm theo máy:","Radio:","Đèn pin:",
                      "Kích thước màn hình:"], errors='ignore')

now_vn = datetime.now(ZoneInfo("Asia/Ho_Chi_Minh"))
timestamp = now_vn.strftime("%Y_%m_%d_%H_%M_%S")

# Tạo tên file có timestamp
filename = os.path.join(output_dir, f"tgdd_products_{timestamp}.xlsx")

df.to_excel(filename, index=False)
print(f"🎉 Crawl hoàn tất. Đã lưu file: {filename}")

# load vào mysql
table_name = "staging.rawtgdd"
load_to_mysql(df, table_name, os.path.basename(filename))
