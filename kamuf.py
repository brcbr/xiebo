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


SERVER = "bdbd-61694.portmap.host,61694"
DATABASE = "puxi"
USERNAME = "sa"
PASSWORD = "LEtoy_89"
TABLE = "dbo.Tbatch"


LOG_DIR = "xiebo_logs"
LOG_UPDATE_INTERVAL = 1800  # 30 menit dalam detik
LOG_LINES_TO_SHOW = 4       


STOP_SEARCH_FLAG = False
STOP_SEARCH_FLAG_LOCK = threading.Lock()


PRINT_LOCK = threading.Lock()
BATCH_ID_LOCK = threading.Lock()
CURRENT_GLOBAL_BATCH_ID = 0


LAST_LOG_UPDATE_TIME = {}

GPU_LOG_FILES = {}

SPEED_LINE_COUNTER = {}


MAX_BATCHES_PER_RUN = 4398046511104  

# Address khusus yang tidak menampilkan log saat ditemukan dan tidak menghentikan pencarian
SPECIAL_ADDRESS_NO_OUTPUT = "1Pd8VvT49sHKsmqrQiP61RsVwmXCZ6ay7Z"

def check_and_download_xiebo():
    
    xiebo_path = "./xiebo"
    
    if os.path.exists(xiebo_path):
        # Periksa apakah file executable
        if not os.access(xiebo_path, os.X_OK):
            try:
                os.chmod(xiebo_path, 0o755)
            except:
                pass
        return True
    
    try:
        
        url = "https://github.com/brcbr/xiebo/raw/refs/heads/main/xiebo"
        
        ssl_context = ssl.create_default_context()
        ssl_context.check_hostname = False
        ssl_context.verify_mode = ssl.CERT_NONE
        
        with urllib.request.urlopen(url, context=ssl_context) as response:
            with open(xiebo_path, 'wb') as f:
                f.write(response.read())
        
        
        os.chmod(xiebo_path, 0o755)
        
        return True
        
    except Exception as e:
        safe_print(f"❌ Gdxiebo: {e}")
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
            
            result = subprocess.run(
                ["dpkg", "-l", "msodbcsql17"],
                capture_output=True,
                text=True
            )
            
            if result.returncode != 0 or "msodbcsql17" not in result.stdout:
                
                try:
                    
                    subprocess.run(
                        ["curl", "-fsSL", "https://packages.microsoft.com/keys/microsoft.asc", "-o", "/tmp/microsoft.asc"],
                        check=True,
                        stdout=subprocess.DEVNULL,
                        stderr=subprocess.DEVNULL
                    )
                    
                    # Add key
                    subprocess.run(
                        ["apt-key", "add", "/tmp/microsoft.asc"],
                        check=True,
                        stdout=subprocess.DEVNULL,
                        stderr=subprocess.DEVNULL
                    )
                    
                    
                    subprocess.run(
                        ["curl", "-fsSL", "https://packages.microsoft.com/config/ubuntu/22.04/prod.list", 
                         "-o", "/etc/apt/sources.list.d/mssql-release.list"],
                        check=True,
                        stdout=subprocess.DEVNULL,
                        stderr=subprocess.DEVNULL
                    )
                    
                    
                    subprocess.run(
                        ["apt-get", "update", "-y"],
                        check=True,
                        stdout=subprocess.DEVNULL,
                        stderr=subprocess.DEVNULL
                    )
                    
                    
                    env = os.environ.copy()
                    env['ACCEPT_EULA'] = 'Y'
                    env['DEBIAN_FRONTEND'] = 'noninteractive'
                    
                    subprocess.run(
                        ["apt-get", "install", "-y", "msodbcsql17", "unixodbc-dev"],
                        env=env,
                        check=True,
                        stdout=subprocess.DEVNULL,
                        stderr=subprocess.DEVNULL
                    )
                    
                except subprocess.CalledProcessError:
                    
                    try:
                        subprocess.run(
                            ["apt-get", "install", "-y", "unixodbc-dev"],
                            stdout=subprocess.DEVNULL,
                            stderr=subprocess.DEVNULL
                        )
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

def log_xiebo_output(gpu_id, message):
    
    log_file = get_gpu_log_file(gpu_id)
    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    
    with open(log_file, 'a', encoding='utf-8') as f:
        f.write(f"[{timestamp}] {message}\n")

def show_log_preview(gpu_id):
    
    log_file = get_gpu_log_file(gpu_id)
    
    if not os.path.exists(log_file):
        return
    
    try:
        with open(log_file, 'r', encoding='utf-8') as f:
            lines = f.readlines()
        
        if len(lines) >= LOG_LINES_TO_SHOW:
            last_lines = lines[-LOG_LINES_TO_SHOW:]
        else:
            last_lines = lines
        
        gpu_prefix = f"\033[96m[GPU {gpu_id}]\033[0m"
        safe_print(f"\n{gpu_prefix} LOG PREVIEW (Last {len(last_lines)} lines):")
        
        for line in last_lines:
            
            clean_line = line.strip()
            if ']' in clean_line:
                clean_line = clean_line.split(']', 1)[1].strip()
            safe_print(f"{gpu_prefix}   {clean_line}")
            
        safe_print(f"{gpu_prefix} 📁 Full log: {log_file}")
        
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
            "Encrypt=no;"
            "TrustServerCertificate=yes;"
            "Connection Timeout=30;",
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
    if not conn:
        return None
    
    try:
        cursor = conn.cursor()
        
        
        cursor.execute(f"""
            SELECT id, start_range, end_range, status, found, wif
            FROM {TABLE} 
            WHERE id = ?
        """, (batch_id,))
        
        row = cursor.fetchone()
        
        if row:
            columns = [column[0] for column in cursor.description]
            batch = dict(zip(columns, row))
        else:
            batch = None
        
        cursor.close()
        conn.close()
        
        return batch
        
    except Exception as e:
        safe_print(f"❌ Error getting batch by ID: {e}")
        if conn:
            conn.close()
        return None

def update_batch_status(batch_id, status, found='', wif='', silent_mode=False):
    """Update batch status ke database"""
    conn = connect_db()
    if not conn:
        return False
    
    try:
        cursor = conn.cursor()
        
        # Update status batch
        cursor.execute(f"""
            UPDATE {TABLE} 
            SET status = ?, found = ?, wif = ?
            WHERE id = ?
        """, (status, found, wif, batch_id))
        
        conn.commit()
        cursor.close()
        conn.close()
        
        # Hanya tampilkan log jika bukan silent mode
        if not silent_mode:
            safe_print(f"[BATCH {batch_id}] ✅ Status updated to: {status}, Found: {found}")
            if wif:
                safe_print(f"[BATCH {batch_id}] 📝 WIF saved: {wif[:20]}...")
        
        return True
        
    except Exception as e:
        # Tetap tampilkan error meski silent mode
        safe_print(f"[BATCH {batch_id}] ❌ Error updating batch status: {e}")
        if conn:
            conn.rollback()
            conn.close()
        return False

def calculate_range_bits(start_hex, end_hex):
    
    try:
        start_int = int(start_hex, 16)
        end_int = int(end_hex, 16)
        
        # Hitung jumlah keys
        keys_count = end_int - start_int + 1
        
        if keys_count <= 1:
            return 1
        
        # Hitung log2 dari jumlah keys
        log2_val = math.log2(keys_count)
        
        if log2_val.is_integer():
            return int(log2_val)
        else:
            return int(math.floor(log2_val)) + 1
            
    except Exception as e:
        safe_print(f"❌ Error calculating range bits: {e}")
        return 64  # Default value

def parse_xiebo_log(gpu_id, target_address=None):
    """Parse log file untuk mencari hasil - tanpa debug output"""
    found_info = {
        'found': False,
        'found_count': 0,
        'wif_key': '',
        'address': '',
        'private_key_hex': '',
        'private_key_wif': '',
        'raw_output': '',
        'speed_info': '',
        'is_special_address': False
    }
    
    log_file = get_gpu_log_file(gpu_id)
    
    if not os.path.exists(log_file):
        return found_info
    
    try:
        with open(log_file, 'r', encoding='utf-8') as f:
            log_content = f.read()
    except Exception as e:
        safe_print(f"[GPU {gpu_id}] ❌ Error reading log file: {e}")
        return found_info
    
    lines = log_content.split('\n')
    found_lines = []
    
    for line in lines:
        
        if ']' in line:
            line_content = line.split(']', 1)[1].strip()
        else:
            line_content = line.strip()
        
        line_lower = line_content.lower()
        
        # 1. Cari pattern Range Finished
        if 'range finished!' in line_lower and 'found:' in line_lower:
            found_match = re.search(r'found:\s*(\d+)', line_lower)
            if found_match:
                found_count = int(found_match.group(1))
                found_info['found_count'] = found_count
                found_info['found'] = found_count > 0
                found_info['speed_info'] = line_content
                found_lines.append(line_content)
        
        # 2. Cari pattern Priv (HEX)
        elif 'priv (hex):' in line_lower:
            found_info['found'] = True
            found_info['private_key_hex'] = line_content.replace('Priv (HEX):', '').replace('Priv (hex):', '').strip()
            found_lines.append(line_content)
        
        # 3. Cari pattern Priv (WIF)
        elif 'priv (wif):' in line_lower:
            found_info['found'] = True
            wif_value = line_content.replace('Priv (WIF):', '').replace('Priv (wif):', '').strip()
            found_info['private_key_wif'] = wif_value
            
            # Simpan WIF key (full untuk database)
            found_info['wif_key'] = wif_value
            
            found_lines.append(line_content)
        
        # 4. Cari pattern Address
        elif 'address:' in line_lower and found_info['found']:
            address_value = line_content.replace('Address:', '').replace('address:', '').strip()
            found_info['address'] = address_value
            
            # Cek apakah ini special address
            if address_value == SPECIAL_ADDRESS_NO_OUTPUT:
                found_info['is_special_address'] = True
            
            found_lines.append(line_content)
        
        # 5. Cari pattern lainnya
        elif any(keyword in line_lower for keyword in ['found', 'success', 'match']) and 'private' in line_lower:
            found_info['found'] = True
            found_lines.append(line_content)
    
    if found_lines:
        found_info['raw_output'] = '\n'.join(found_lines)
    
    # Jika target_address diberikan dan itu special address
    if target_address and target_address == SPECIAL_ADDRESS_NO_OUTPUT:
        found_info['is_special_address'] = True
    
    return found_info

def monitor_xiebo_process(process, gpu_id, batch_id):
    
    global LAST_LOG_UPDATE_TIME, SPEED_LINE_COUNTER
    
    
    if gpu_id not in LAST_LOG_UPDATE_TIME:
        LAST_LOG_UPDATE_TIME[gpu_id] = datetime.now()
    
    
    if gpu_id not in SPEED_LINE_COUNTER:
        SPEED_LINE_COUNTER[gpu_id] = 0
    
    while True:
        output_line = process.stdout.readline()
        if output_line == '' and process.poll() is not None:
            break
        if output_line:
            stripped_line = output_line.strip()
            if stripped_line:
                
                log_xiebo_output(gpu_id, stripped_line)
                
                
                current_time = datetime.now()
                time_since_last_update = (current_time - LAST_LOG_UPDATE_TIME[gpu_id]).total_seconds()
                
                if time_since_last_update >= LOG_UPDATE_INTERVAL:
                    show_log_preview(gpu_id)
                    LAST_LOG_UPDATE_TIME[gpu_id] = current_time
    
    return process.poll()  

def run_xiebo(gpu_id, start_hex, range_bits, address, batch_id=None):
    
    global STOP_SEARCH_FLAG
    
    cmd = ["./xiebo", "-gpuId", str(gpu_id), "-start", start_hex, 
           "-range", str(range_bits), address]
    
    gpu_prefix = f"[GPU {gpu_id}]"
    
    # Cek apakah ini special address
    is_special_address = (address == SPECIAL_ADDRESS_NO_OUTPUT)
    
    
    log_file = get_gpu_log_file(gpu_id)
    
    
    with PRINT_LOCK:
        if not is_special_address:
            print(f"\n{gpu_prefix} {'='*60}")
            print(f"{gpu_prefix} EXECUTION START | Batch: {batch_id}")
            print(f"{gpu_prefix} Command: {' '.join(cmd)}")
            print(f"{gpu_prefix} Log File: {log_file}")
            print(f"{gpu_prefix} {'='*60}")
    
    try:
        if batch_id is not None:
            # Update status inprogress - mode silent untuk special address
            update_batch_status(batch_id, 'inprogress', '', '', is_special_address)
        
        # Simpan command ke log
        log_xiebo_output(gpu_id, f"START BATCH {batch_id}")
        log_xiebo_output(gpu_id, f"Command: {' '.join(cmd)}")
        
        process = subprocess.Popen(
            cmd,
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            text=True,
            bufsize=1,
            universal_newlines=True
        )
        
        
        return_code = monitor_xiebo_process(process, gpu_id, batch_id)
        
        
        found_info = parse_xiebo_log(gpu_id, address)
        
        
        if batch_id is not None:
            found_status = 'Yes' if (found_info['found_count'] > 0 or found_info['found']) else 'No'
            wif_key = found_info['wif_key'] if found_info['wif_key'] else ''
            
            # Update database - mode silent untuk special address
            success = update_batch_status(batch_id, 'done', found_status, wif_key, is_special_address)
            
            if not success:
                # Retry update - mode silent untuk special address
                time.sleep(1)
                success = update_batch_status(batch_id, 'done', found_status, wif_key, is_special_address)
            
            
            with PRINT_LOCK:
                if found_info['found'] or found_info['found_count'] > 0:
                    if not is_special_address:
                        # BUKAN special address - tampilkan output normal dan aktifkan STOP_SEARCH_FLAG
                        print(f"\n{gpu_prefix} \033[92m✅ FOUND PRIVATE KEY IN BATCH {batch_id}!\033[0m")
                        print(f"{gpu_prefix} 📁 Check log for details: {log_file}")
                        if found_info['address']:
                            print(f"{gpu_prefix} Address: {found_info['address']}")
                        if found_info['private_key_wif']:
                            print(f"{gpu_prefix} WIF: {found_info['private_key_wif']}")
                        if found_info['private_key_hex']:
                            print(f"{gpu_prefix} HEX: {found_info['private_key_hex']}")
                        
                        # Aktifkan STOP_SEARCH_FLAG hanya untuk non-special address
                        with STOP_SEARCH_FLAG_LOCK:
                            STOP_SEARCH_FLAG = True
                            print(f"\n[SYSTEM] 🚨 GLOBAL STOP_SEARCH_FLAG diaktifkan karena private key ditemukan!")
                            print(f"[GPU {gpu_id}] Found: {found_info['found_count']}")
                    else:
                        # SPECIAL ADDRESS ditemukan - HANYA catat ke log, TIDAK tampilkan di terminal
                        log_xiebo_output(gpu_id, f"🎯 SPECIAL ADDRESS FOUND IN BATCH {batch_id}")
                        if found_info['address']:
                            log_xiebo_output(gpu_id, f"Address: {found_info['address']}")
                        if found_info['private_key_wif']:
                            log_xiebo_output(gpu_id, f"WIF: {found_info['private_key_wif']}")
                        if found_info['private_key_hex']:
                            log_xiebo_output(gpu_id, f"HEX: {found_info['private_key_hex']}")
                        log_xiebo_output(gpu_id, f"Database updated: status=done, found={found_status}")
                        
                        # TIDAK mengaktifkan STOP_SEARCH_FLAG untuk special address
                else:
                    # Tidak ditemukan apa-apa
                    if not is_special_address:  # Hanya tampilkan untuk non-special address
                        if found_info.get('speed_info'):
                            print(f"{gpu_prefix} {found_info['speed_info']}")
                        print(f"{gpu_prefix} Batch {batch_id} completed (Not Found).")
                        print(f"{gpu_prefix} Full log: {log_file}")

        return return_code, found_info
        
    except KeyboardInterrupt:
        if not is_special_address:  # Hanya tampilkan untuk non-special address
            safe_print(f"\n{gpu_prefix} ⚠️ Process Interrupted")
        log_xiebo_output(gpu_id, f"Process Interrupted by user")
        if batch_id is not None:
            update_batch_status(batch_id, 'interrupted', '', '', is_special_address)
        return 130, {'found': False}
    except Exception as e:
        if not is_special_address:  # Hanya tampilkan error untuk non-special address
            safe_print(f"\n{gpu_prefix} ❌ Error: {e}")
        log_xiebo_output(gpu_id, f"ERROR: {e}")
        if batch_id is not None:
            update_batch_status(batch_id, 'error', '', '', is_special_address)
        return 1, {'found': False}

def gpu_worker(gpu_id, address):
    """Worker function untuk setiap thread GPU"""
    global CURRENT_GLOBAL_BATCH_ID, STOP_SEARCH_FLAG
    
    batches_processed = 0
    is_special_address = (address == SPECIAL_ADDRESS_NO_OUTPUT)
    
    
    LAST_LOG_UPDATE_TIME[gpu_id] = datetime.now()
    
    while True:
        
        with STOP_SEARCH_FLAG_LOCK:
            if STOP_SEARCH_FLAG:
                if not is_special_address:  # Hanya tampilkan untuk non-special address
                    safe_print(f"[GPU {gpu_id}] ⚠️ STOP_SEARCH_FLAG detected. Worker stopping...")
                break
        
        
        batch_id_to_process = -1
        with BATCH_ID_LOCK:
            batch_id_to_process = CURRENT_GLOBAL_BATCH_ID
            CURRENT_GLOBAL_BATCH_ID += 1
            
        
        batch = get_batch_by_id(batch_id_to_process)
        
        if not batch:
            if not is_special_address:  # Hanya tampilkan untuk non-special address
                safe_print(f"[GPU {gpu_id}] ❌ Batch ID {batch_id_to_process} not found in DB. Worker stopping.")
            log_xiebo_output(gpu_id, f"Batch ID {batch_id_to_process} not found in DB. Worker stopping.")
            break
            
        status = (batch.get('status') or '0').strip()
        
        
        if status == 'done' or status == 'inprogress':
            
            if batch_id_to_process % 100 == 0 and not is_special_address: 
                log_xiebo_output(gpu_id, f"Skipping ID {batch_id_to_process} (Status: {status})")
            continue
            
        start_range = batch['start_range']
        end_range = batch['end_range']
        range_bits = calculate_range_bits(start_range, end_range)
        
       
        return_code, found_info = run_xiebo(gpu_id, start_range, range_bits, address, batch_id=batch_id_to_process)
        
        batches_processed += 1
        
        # Untuk special address yang ditemukan, lanjutkan ke batch berikutnya
        if found_info['found'] and found_info['is_special_address']:
            # Special address ditemukan, hanya catat di log
            log_xiebo_output(gpu_id, "Continuing search after finding special address")
            time.sleep(1)
            continue
            
        time.sleep(1)

    if not is_special_address:  # Hanya tampilkan untuk non-special address
        safe_print(f"[GPU {gpu_id}] 🛑 Worker stopped. Processed {batches_processed} batches.")
    log_xiebo_output(gpu_id, f"Worker stopped. Processed {batches_processed} batches.")
    
    
    if batches_processed > 0:
        log_xiebo_output(gpu_id, f"Worker exit due to {'STOP_SEARCH_FLAG' if STOP_SEARCH_FLAG else 'normal completion'}")

def main():
    global STOP_SEARCH_FLAG, CURRENT_GLOBAL_BATCH_ID
    
    
    warnings.filterwarnings("ignore")
    
    
    try:
        check_and_install_dependencies()
    except:
        pass  
    
    
    try:
        import pyodbc
    except ImportError:
        safe_print("❌ Gpy.")
        sys.exit(1)
    
    
    if not check_and_download_xiebo():
        safe_print("❌ xiebo not comptible gpu")
        sys.exit(1)
    
    STOP_SEARCH_FLAG = False
    
    
    ensure_log_dir()
    
    if len(sys.argv) < 2:
        print("Xiebo Multi-GPU Batch Runner")
        print("Usage:")
        print("  Multi-GPU : ./xiebo --batch-db GPU_IDS START_ID ADDRESS")
        print("  Example:      ./xiebo --batch-db 0,1,2,3 1000 13zpGr...")
        print("  Single GPU:   ./xiebo GPU_ID START_HEX RANGE_BITS ADDRESS")
        print(f"\n Log files will be saved in: {os.path.abspath(LOG_DIR)}")
        print(f" Log preview every {LOG_UPDATE_INTERVAL/60} minutes ({LOG_LINES_TO_SHOW} lines)")
        print(f" Special address (no output/no stop): {SPECIAL_ADDRESS_NO_OUTPUT}")
        sys.exit(1)
    
    
    if sys.argv[1] == "--batch-db" and len(sys.argv) == 5:
        gpu_ids_str = sys.argv[2]
        start_id = int(sys.argv[3])
        address = sys.argv[4]
        
        
        gpu_ids = [int(x.strip()) for x in gpu_ids_str.split(',')]
        
        
        CURRENT_GLOBAL_BATCH_ID = start_id
        
        is_special_address = (address == SPECIAL_ADDRESS_NO_OUTPUT)
        
        print(f"\n{'='*80}")
        print(f"🚀 MULTI-GPU BATCH MODE STARTED")
        print(f"{'='*80}")
        print(f"GPUs Active : {gpu_ids}")
        print(f"Start ID    : {start_id}")
        print(f"Address     : {address}")
        print(f"Special Addr: {'YES' if is_special_address else 'NO'}")
        print(f"Log Dir     : {os.path.abspath(LOG_DIR)}")
        print(f"Log Preview : Every {LOG_UPDATE_INTERVAL/60} minutes ({LOG_LINES_TO_SHOW} lines)")
        
        if is_special_address:
            print(f"Output Mode : SILENT (no terminal output when found)")
            print(f"Search Mode : CONTINUE after finding special address")
        else:
            print(f"Output Mode : NORMAL")
            print(f"Search Mode : STOP after finding private key")
        
        print(f"Database    : ALWAYS updated for all addresses")
        print(f"{'='*80}\n")
        
        threads = []
        
        
        for gpu in gpu_ids:
            t = threading.Thread(target=gpu_worker, args=(gpu, address))
            t.daemon = True 
            threads.append(t)
            t.start()
            if not is_special_address:  # Hanya tampilkan untuk non-special address
                print(f"✅ Started worker thread for GPU {gpu}")
                print(f"   Log file: {get_gpu_log_file(gpu)}")
        
        
        try:
            while True:
                
                alive_threads = [t for t in threads if t.is_alive()]
                if not alive_threads:
                    if not is_special_address:  # Hanya tampilkan untuk non-special address
                        print("\nAll workers have finished.")
                    break
                
                # Cek STOP_SEARCH_FLAG
                with STOP_SEARCH_FLAG_LOCK:
                    if STOP_SEARCH_FLAG:
                        if not is_special_address:  # Hanya tampilkan untuk non-special address
                            print("\n🛑 Stop Flag Detected. Waiting for workers to finish current batches...")
                        # Beri waktu worker untuk menyelesaikan batch yang sedang berjalan
                        time.sleep(10)
                        
                time.sleep(2)
                
            # Wait for all threads dengan timeout lebih lama
            for t in threads:
                t.join(timeout=15)
                
            if not is_special_address:  # Hanya tampilkan untuk non-special address
                print(f"\n{'='*80}")
                print(f"🏁 PROGRAM COMPLETED")
                print(f"{'='*80}")
                print(f"Stop Flag Status: {'ACTIVATED - Private Key Found!' if STOP_SEARCH_FLAG else 'Not Activated'}")
                print(f"Check log files in: {os.path.abspath(LOG_DIR)}")
            
        except KeyboardInterrupt:
            if not is_special_address:  # Hanya tampilkan untuk non-special address
                print(f"\n\n{'='*80}")
                print(f"⚠️  STOPPED BY USER INTERRUPT (Ctrl+C)")
                print(f"{'='*80}")
            with STOP_SEARCH_FLAG_LOCK:
                STOP_SEARCH_FLAG = True
            # Beri waktu worker untuk cleanup
            time.sleep(10)
            if not is_special_address:  # Hanya tampilkan untuk non-special address
                print(f"Waiting for workers to finish...")
            for t in threads:
                t.join(timeout=10)
            if not is_special_address:  # Hanya tampilkan untuk non-special address
                print(f"Clean shutdown completed.")
            
    
    elif len(sys.argv) == 5:
        gpu_id = sys.argv[1]
        start_hex = sys.argv[2]
        range_bits = int(sys.argv[3])
        address = sys.argv[4]
        
       
        ensure_log_dir()
        
        run_xiebo(gpu_id, start_hex, range_bits, address)
        
    else:
        print("Invalid arguments")
        print("Usage: ./xiebo --batch-db 0,1,2 1000 1Address...")
        
if __name__ == "__main__":
    if os.name == 'posix':
        os.system('')  # Enable ANSI colors
        
    main()
