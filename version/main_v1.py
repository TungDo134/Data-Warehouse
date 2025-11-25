# main.py
"""
Main Pipeline Controller
Điều phối quá trình Crawl Data và Load to Data Warehouse
"""

import subprocess
import os
import sys

# Import log utilities
from utils.log_utils import write_pipeline_log


def run_script(script_path, step_name):
    """
    Chạy script Python và ghi log

    Args:
        script_path (str): Đường dẫn đến file script
        step_name (str): Tên bước (dùng cho log)

    Raises:
        SystemExit: Nếu script chạy thất bại
    """
    script_basename = os.path.basename(script_path)

    try:
        # Ghi log bắt đầu
        print(f"\n🚀 Đang chạy: {script_basename} ...")
        write_pipeline_log(
            step_name=f"{step_name}_START",
            status="RUNNING",
            message=f"Bắt đầu chạy",
            script_name=script_basename
        )

        # Chạy script
        subprocess.run([sys.executable, script_path], check=True)

        # Ghi log thành công
        print(f"✅ {script_basename} chạy thành công.\n")
        write_pipeline_log(
            step_name=f"{step_name}_COMPLETE",
            status="SUCCESS",
            message=f"Hoàn thành thành công",
            script_name=script_basename
        )

    except subprocess.CalledProcessError as e:
        # Ghi log lỗi
        error_msg = f"Lỗi subprocess: {str(e)}"
        print(f"❌ {error_msg}")

        write_pipeline_log(
            step_name=f"{step_name}_FAILED",
            status="FAILED",
            message=error_msg,
            script_name=script_basename
        )

        # Ghi log tổng thất bại
        write_pipeline_log(
            step_name="FAILED",
            status="FAILED",
            message=f"Pipeline dừng tại bước: {step_name}"
        )

        sys.exit(1)  # Dừng hẳn nếu 1 bước lỗi


if __name__ == "__main__":
    print("=" * 60)
    print("🚀 BẮT ĐẦU MAIN PIPELINE - ETL PROCESS")
    print("=" * 60)

    # Ghi log bắt đầu pipeline
    write_pipeline_log(
        step_name="START",
        status="RUNNING",
        message="Khởi động Main Pipeline - Crawl & Load to DW"
    )

    try:
        # --- Bước 1: Crawl data ---
        run_script(
            script_path=os.path.join(os.path.dirname(__file__), "CrawlData.py"),
            step_name="CRAWL_DATA"
        )

        # --- Bước 2: Load vào Data Warehouse ---
        run_script(
            script_path=os.path.join(os.path.dirname(__file__), "LoadToDW.py"),
            step_name="LOAD_TO_DW"
        )

        # Ghi log hoàn thành toàn bộ
        print("=" * 60)
        print("🎉 HOÀN TẤT TOÀN BỘ QUÁ TRÌNH")
        print("=" * 60)

        write_pipeline_log(
            step_name="COMPLETE",
            status="SUCCESS",
            message="Main Pipeline hoàn thành thành công - Tất cả bước đều OK"
        )

    except Exception as e:
        # Xử lý lỗi không mong muốn
        error_msg = f"Lỗi không xác định: {str(e)}"
        print(f"\n❌ {error_msg}")

        write_pipeline_log(
            step_name="ERROR",
            status="FAILED",
            message=error_msg
        )

        sys.exit(1)