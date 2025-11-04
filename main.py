# file1.py
import subprocess
import os
import sys


def run_script(script_path):
    try:
        print(f"\n🚀 Đang chạy: {os.path.basename(script_path)} ...")
        subprocess.run([sys.executable, script_path], check=True)
        print(f"✅ {os.path.basename(script_path)} chạy thành công.\n")
    except subprocess.CalledProcessError as e:
        print(f"❌ Lỗi khi chạy {os.path.basename(script_path)}: {e}")
        sys.exit(1)  # Dừng hẳn nếu 1 bước lỗi


if __name__ == "__main__":
    print("🚀 Bắt đầu tiến trình ")

    # --- Bước 1: Crawl data ---
    run_script(r"D:\Workspace-Python\Data-Warehouse\CrawlData.py")

    # --- Bước 2: Load vào Data Warehouse ---
    run_script(r"D:\Workspace-Python\Data-Warehouse\LoadToDW.py")

    print("🎉 Hoàn tất toàn bộ quá trình")
