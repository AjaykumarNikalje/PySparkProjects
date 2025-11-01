import os
import csv
from datetime import datetime
from pathlib import Path

def log_pipeline_metadata(final_log_file_name:str,log_dir: str, month: str, run_type: str, file_path: str, status: str, message: str):
    """
    Logs pipeline metadata (success/failure) to a CSV file for audit and monitoring.
    """
    #Resolve log_dir safely
    log_dir_path = Path(log_dir)

    # Always place logs under project root
    safe_log_path = log_dir_path

    try:
        safe_log_path.mkdir(parents=True, exist_ok=True)
    except OSError:
        # If the filesystem is read-only (e.g., Spark container)
        tmp_path = Path("/tmp/logs")
        tmp_path.mkdir(parents=True, exist_ok=True)
        safe_log_path = tmp_path
        
    log_file = os.path.join(safe_log_path, "pipeline_run_metadata.csv")

    # Define metadata row
    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    meta_record = {
        "final_log_file_name":final_log_file_name,
        "timestamp": timestamp,
        "month": month,
        "run_type": run_type,
        "file_path": file_path,
        "status": status,
        "message": message
    }

    # Write header if file doesn't exist
    file_exists = os.path.exists(log_file)
    with open(log_file, "a", newline="") as csvfile:
        writer = csv.DictWriter(csvfile, fieldnames=meta_record.keys())
        if not file_exists:
            writer.writeheader()
        writer.writerow(meta_record)

    print(f" Metadata logged in {log_file}")
