import os
import time
import random
import pandas as pd
from datetime import datetime
from zoneinfo import ZoneInfo
from bs4 import BeautifulSoup
from selenium import webdriver
from selenium.webdriver.chrome.options import Options

from database.db_utils import load_to_staging_database
from database.db_control_utils import get_crawl_config


# ===================== INIT CHROME DRIVER =====================
def init_driver():
    chrome_options = Options()
    chrome_options.add_argument("--headless=new")
    chrome_options.add_argument("--no-sandbox")
    chrome_options.add_argument("--disable-dev-shm-usage")
    chrome_options.add_argument("--disable-gpu")
    chrome_options.add_argument("window-size=1920,1080")
    driver = webdriver.Chrome(options=chrome_options)
    return driver


# =====================  FUNC GET LINK PRODUCTS =====================
def get_product_links(driver, category_url, base_url, record_limit=None, max_clicks=1):
    print(f"🔍 Đang thu thập danh sách sản phẩm từ: {category_url}")
    driver.get(category_url)
    time.sleep(3)

    # Click "Xem thêm"
    for i in range(max_clicks):
        try:
            view_more_btn = driver.find_element("css selector", ".view-more a")
            driver.execute_script("arguments[0].scrollIntoView(true);", view_more_btn)
            time.sleep(1)
            driver.execute_script("arguments[0].click();", view_more_btn)
            print(f"🔁 (Lần {i + 1}/{max_clicks}) Đã click 'Xem thêm' để tải thêm sản phẩm...")
            time.sleep(random.uniform(2.5, 4.5))
        except Exception:
            print(f"✅ Dừng ở lần {i + 1}: Không còn nút 'Xem thêm' hoặc đã load hết.")
            break

    soup = BeautifulSoup(driver.page_source, "lxml")
    product_links, products = [], []

    for a in soup.select("ul.listproduct a.main-contain"):
        href = a.get("href")
        if href and href.startswith("/dtdd/"):
            full_link = base_url.rstrip("/") + href
            product_links.append(full_link)

            name_tag = a.select_one("h3")
            product_name = name_tag.get_text(strip=True) if name_tag else "Không rõ"

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
                "Source": full_link
            })

    total = len(product_links)
    print(f"✅ Tìm thấy {total} sản phẩm sau khi load toàn trang.")

    # ✂️ Nếu có limit → cắt bớt danh sách
    if record_limit and isinstance(record_limit, int) and record_limit > 0:
        product_links = product_links[:record_limit]
        products = products[:record_limit]
        print(f"🧪 Đang test với {len(products)} sản phẩm đầu tiên (record_limit = {record_limit}).")
    else:
        print(f"📦 Không giới hạn số lượng — lấy toàn bộ {total} sản phẩm.")

    return products


# ===================== SAFE GET (PREVENT BROWSER FREEZING) =====================
def safe_get(driver, url, retries=3):
    for attempt in range(retries):
        try:
            driver.set_page_load_timeout(20)
            driver.get(url)
            time.sleep(random.uniform(2.5, 4.5))
            return BeautifulSoup(driver.page_source, "lxml")
        except Exception as e:
            print(f"⚠️ Lỗi tải ({attempt + 1}/{retries}): {e}")
            if attempt == retries - 1:
                print(f"❌ Bỏ qua {url}")
                return None
            time.sleep(2)
    return None


# ===================== FUNC CRAWL DETAIL PRODUCT =====================
def crawl_product_details(driver, products):
    all_data = []
    for i, base_info in enumerate(products, start=1):
        url = base_info["Source"]
        print(f"📦 ({i}/{len(products)}) Đang xử lý: {url}")

        soup = safe_get(driver, url)
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
    return all_data


# ===================== SAVE FILE EXCEL =====================
def save_to_excel(all_data, output_dir):
    os.makedirs(output_dir, exist_ok=True)
    df = pd.DataFrame(all_data)
    df = df.drop(columns=["Thẻ nhớ:", "Sạc kèm theo máy:", "Radio:",
                          "Đèn pin:", "Kích thước màn hình:"],
                 errors='ignore')

    now_vn = datetime.now(ZoneInfo("Asia/Ho_Chi_Minh"))
    timestamp = now_vn.strftime("%Y_%m_%d_%H_%M_%S")
    filename = os.path.join(output_dir, f"tgdd_products_{timestamp}.xlsx")

    df.to_excel(filename, index=False)
    print(f"🎉 Crawl hoàn tất. Đã lưu file: {filename}")
    return df, filename


# ===================== MAIN =====================
def run_crawl_pipeline():
    # Đọc config từ DB
    config = get_crawl_config("TGDD")

    if not config:
        print("❌ Không tìm thấy config phù hợp trong data_control.config!")
        return

    url = config["source_url"]
    table_name = config["target_table"]
    max_clicks = config["max_clicks"]
    record_limit = config["record_limit"]

    category_url = url + "dtdd"
    output_dir = r"D:\Workspace-Python\Data-Warehouse\Crawl Data"

    driver = init_driver()

    try:
        products = get_product_links(driver, category_url, url, record_limit, max_clicks)
        all_data = crawl_product_details(driver, products)
        df, filename = save_to_excel(all_data, output_dir)
        load_to_staging_database(df, table_name, os.path.basename(filename))
    finally:
        driver.quit()


#
# if __name__ == "__main__":
#     run_crawl_pipeline()

if __name__ == "__main__":
    try:
        run_crawl_pipeline()
    except Exception as e:
        import traceback
        traceback.print_exc()
        input("Press Enter to exit...")
