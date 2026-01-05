import subprocess
import sys
import os
import time
import math
import re
import pyodbc
import threading
from datetime import datetime, timedelta

# Konfigurasi database SQL Server
SERVER = "benilapo-31088.portmap.host,31088"
DATABASE = "puxi"
USERNAME = "sa"
PASSWORD = "LEtoy_89"
TABLE = "dbo.Tbatch"

# Konfigurasi logging
LOG_DIR = "xiebo_logs"
LOG_UPDATE_INTERVAL = 1800  # 30 menit dalam detik
LOG_LINES_TO_SHOW = 4       # Jumlah baris yang ditampilkan setiap interval

# Global flag untuk menghentikan pencarian
STOP_SEARCH_FLAG = False
STOP_SEARCH_FLAG_LOCK = threading.Lock()

# Global variables untuk Threading synchronization
PRINT_LOCK = threading.Lock()
BATCH_ID_LOCK = threading.Lock()
CURRENT_GLOBAL_BATCH_ID = 0

# Dictionary untuk menyimpan waktu terakhir update log per GPU
LAST_LOG_UPDATE_TIME = {}
# Dictionary untuk menyimpan path log file per GPU
GPU_LOG_FILES = {}
# Dictionary untuk menyimpan line counter untuk mengurangi frekuensi tampilan speed
SPEED_LINE_COUNTER = {}

# Konfigurasi batch
MAX_BATCHES_PER_RUN = 4000000000000  # Maksimal batch per eksekusi

def ensure_log_dir():
    """Membuat directory log jika belum ada"""
    if not os.path.exists(LOG_DIR):
        os.makedirs(LOG_DIR)

def get_gpu_log_file(gpu_id):
    """Mendapatkan path file log untuk GPU tertentu"""
    if gpu_id not in GPU_LOG_FILES:
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        log_file = os.path.join(LOG_DIR, f"gpu_{gpu_id}_{timestamp}.log")
        GPU_LOG_FILES[gpu_id] = log_file
    return GPU_LOG_FILES[gpu_id]

def log_xiebo_output(gpu_id, message):
    """Menyimpan output xiebo ke file log"""
    log_file = get_gpu_log_file(gpu_id)
    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    
    with open(log_file, 'a', encoding='utf-8') as f:
        f.write(f"[{timestamp}] {message}\n")

def show_log_preview(gpu_id):
    """Menampilkan preview log (4 baris terakhir) setiap interval"""
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
        safe_print(f"\n{gpu_prefix} 📋 LOG PREVIEW (Last {len(last_lines)} lines):")
        
        for line in last_lines:
            # Hapus timestamp dari log untuk tampilan yang lebih rapi
            clean_line = line.strip()
            if ']' in clean_line:
                clean_line = clean_line.split(']', 1)[1].strip()
            safe_print(f"{gpu_prefix}   {clean_line}")
            
        safe_print(f"{gpu_prefix} 📁 Full log: {log_file}")
        
    except Exception as e:
        safe_print(f"[GPU {gpu_id}] ❌ Error reading log: {e}")

def connect_db():
    """Membuat koneksi ke database SQL Server"""
    try:
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
        safe_print(f"❌ Database connection error: {e}")
        return None

def safe_print(message):
    """Mencetak pesan ke layar dengan thread lock agar tidak tumpang tindih"""
    with PRINT_LOCK:
        print(message)

def get_batch_by_id(batch_id):
    """Mengambil data batch berdasarkan ID"""
    conn = connect_db()
    if not conn:
        return None
    
    try:
        cursor = conn.cursor()
        
        # Ambil data batch berdasarkan ID
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

def update_batch_status(batch_id, status, found='', wif=''):
    """Update status batch di database"""
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
        
        safe_print(f"[BATCH {batch_id}] ✅ Status updated to: {status}, Found: {found}")
        if wif:
            safe_print(f"[BATCH {batch_id}] 📝 WIF saved: {wif[:20]}...")
        return True
        
    except Exception as e:
        safe_print(f"[BATCH {batch_id}] ❌ Error updating batch status: {e}")
        if conn:
            conn.rollback()
            conn.close()
        return False

def calculate_range_bits(start_hex, end_hex):
    """Menghitung range bits dari start dan end hex"""
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

def parse_xiebo_log(gpu_id):
    """Parse output dari file log xiebo untuk mencari private key yang ditemukan"""
    found_info = {
        'found': False,
        'found_count': 0,
        'wif_key': '',
        'address': '',
        'private_key_hex': '',
        'private_key_wif': '',
        'raw_output': '',
        'speed_info': ''
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
        # Hapus timestamp dari awal baris jika ada
        if ']' in line:
            line_content = line.split(']', 1)[1].strip()
        else:
            line_content = line.strip()
        
        line_lower = line_content.lower()
        
        # 1. Cari pattern "Found: X"
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
            if len(wif_value) >= 60:
                found_info['wif_key'] = wif_value[:60]
            else:
                found_info['wif_key'] = wif_value
            found_lines.append(line_content)
        
        # 4. Cari pattern Address
        elif 'address:' in line_lower and found_info['found']:
            found_info['address'] = line_content.replace('Address:', '').replace('address:', '').strip()
            found_lines.append(line_content)
        
        # 5. Cari pattern lain
        elif any(keyword in line_lower for keyword in ['found', 'success', 'match']) and 'private' in line_lower:
            found_info['found'] = True
            found_lines.append(line_content)
    
    if found_lines:
        found_info['raw_output'] = '\n'.join(found_lines)
        # Fallback logic untuk wif_key
        if found_info['private_key_wif'] and not found_info['wif_key']:
            found_info['wif_key'] = found_info['private_key_wif'][:60]
        elif found_info['private_key_hex'] and not found_info['wif_key']:
            found_info['wif_key'] = found_info['private_key_hex'][:60]
    
    return found_info

def monitor_xiebo_process(process, gpu_id, batch_id):
    """Memantau proses xiebo dan menyimpan output ke log file"""
    global LAST_LOG_UPDATE_TIME, SPEED_LINE_COUNTER
    
    # Inisialisasi waktu update log untuk GPU ini
    if gpu_id not in LAST_LOG_UPDATE_TIME:
        LAST_LOG_UPDATE_TIME[gpu_id] = datetime.now()
    
    # Inisialisasi counter untuk GPU ini
    if gpu_id not in SPEED_LINE_COUNTER:
        SPEED_LINE_COUNTER[gpu_id] = 0
    
    while True:
        output_line = process.stdout.readline()
        if output_line == '' and process.poll() is not None:
            break
        if output_line:
            stripped_line = output_line.strip()
            if stripped_line:
                # Selalu simpan ke log file
                log_xiebo_output(gpu_id, stripped_line)
                
                # Cek apakah sudah waktunya menampilkan preview log
                current_time = datetime.now()
                time_since_last_update = (current_time - LAST_LOG_UPDATE_TIME[gpu_id]).total_seconds()
                
                if time_since_last_update >= LOG_UPDATE_INTERVAL:
                    show_log_preview(gpu_id)
                    LAST_LOG_UPDATE_TIME[gpu_id] = current_time
                
                # HANYA tampilkan pesan-pesan penting ke terminal
                line_lower = stripped_line.lower()
                should_print = False
                color_code = ""

                # Hanya tampilkan jika mengandung keyword penting
                if ('found:' in line_lower and 'range finished!' in line_lower) or 'success' in line_lower:
                    color_code = "\033[92m"  # Hijau
                    should_print = True
                elif 'error' in line_lower or 'failed' in line_lower:
                    color_code = "\033[91m"  # Merah
                    should_print = True
                # Speed info TIDAK ditampilkan ke terminal
                # elif 'speed' in line_lower or 'key/s' in line_lower:
                #     # Speed info TIDAK ditampilkan ke terminal
                #     should_print = False
                elif 'range' in line_lower and ('start' in line_lower or 'finished!' in line_lower):
                    # Hanya tampilkan range start dan finish
                    color_code = "\033[94m"  # Biru
                    should_print = True
                elif 'priv (' in line_lower or 'address:' in line_lower:
                    # Private key atau address ditemukan
                    color_code = "\033[95m"  # Ungu
                    should_print = True
                
                if should_print:
                    safe_print(f"[GPU {gpu_id}] {color_code}{stripped_line}\033[0m")
    
    return process.poll()  # Return exit code

def run_xiebo(gpu_id, start_hex, range_bits, address, batch_id=None):
    """Run xiebo binary langsung"""
    global STOP_SEARCH_FLAG
    
    cmd = ["./xiebo", "-gpuId", str(gpu_id), "-start", start_hex, 
           "-range", str(range_bits), address]
    
    gpu_prefix = f"[GPU {gpu_id}]"
    
    # Inisialisasi log file untuk GPU ini
    log_file = get_gpu_log_file(gpu_id)
    
    # Gunakan lock hanya untuk print block besar ini agar rapi
    with PRINT_LOCK:
        print(f"\n{gpu_prefix} {'='*60}")
        print(f"{gpu_prefix} 🚀 EXECUTION START | Batch: {batch_id}")
        print(f"{gpu_prefix} Command: {' '.join(cmd)}")
        print(f"{gpu_prefix} Log File: {log_file}")
        print(f"{gpu_prefix} {'='*60}")
    
    try:
        if batch_id is not None:
            update_batch_status(batch_id, 'inprogress')
        
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
        
        # Monitor process dan simpan output ke log
        return_code = monitor_xiebo_process(process, gpu_id, batch_id)
        
        # Parse output dari log file
        found_info = parse_xiebo_log(gpu_id)
        
        # UPDATE DATABASE SETELAH MENDAPATKAN FOUND_INFO
        if batch_id is not None:
            found_status = 'Yes' if (found_info['found_count'] > 0 or found_info['found']) else 'No'
            wif_key = found_info['wif_key'] if found_info['wif_key'] else ''
            
            # UPDATE KE DATABASE MESKIPUN STOP_SEARCH_FLAG AKTIF
            success = update_batch_status(batch_id, 'done', found_status, wif_key)
            
            if not success:
                safe_print(f"[GPU {gpu_id}] ⚠️ Failed to update database, retrying...")
                time.sleep(1)
                success = update_batch_status(batch_id, 'done', found_status, wif_key)
            
            # Tampilkan hasil setelah update database
            with PRINT_LOCK:
                if found_info['found'] or found_info['found_count'] > 0:
                    print(f"\n{gpu_prefix} \033[92m✅ FOUND PRIVATE KEY IN BATCH {batch_id}!\033[0m")
                    print(f"{gpu_prefix} 📁 Check log for details: {log_file}")
                    if found_info['private_key_wif']:
                        print(f"{gpu_prefix} WIF: {found_info['private_key_wif']}")
                    if found_info['private_key_hex']:
                        print(f"{gpu_prefix} HEX: {found_info['private_key_hex']}")
                    print(f"{gpu_prefix} ✅ Status updated in database: Found = {found_status}")
                    
                    # AKTIFKAN STOP_SEARCH_FLAG SETELAH UPDATE DATABASE
                    with STOP_SEARCH_FLAG_LOCK:
                        STOP_SEARCH_FLAG = True
                        print(f"\n[SYSTEM] 🚨 GLOBAL STOP_SEARCH_FLAG diaktifkan karena private key ditemukan!")
                        print(f"[GPU {gpu_id}] Found: {found_info['found_count']}")
                else:
                    # Tampilkan informasi selesai tanpa preview log (kecuali penting)
                    if found_info.get('speed_info'):
                        # Tampilkan speed info terakhir
                        print(f"{gpu_prefix} {found_info['speed_info']}")
                    print(f"{gpu_prefix} Batch {batch_id} completed (Not Found).")
                    print(f"{gpu_prefix} Full log: {log_file}")

        return return_code, found_info
        
    except KeyboardInterrupt:
        safe_print(f"\n{gpu_prefix} ⚠️ Process Interrupted")
        log_xiebo_output(gpu_id, f"Process Interrupted by user")
        if batch_id is not None:
            update_batch_status(batch_id, 'interrupted')
        return 130, {'found': False}
    except Exception as e:
        safe_print(f"\n{gpu_prefix} ❌ Error: {e}")
        log_xiebo_output(gpu_id, f"ERROR: {e}")
        if batch_id is not None:
            update_batch_status(batch_id, 'error')
        return 1, {'found': False}

def gpu_worker(gpu_id, address):
    """Worker function untuk setiap thread GPU"""
    global CURRENT_GLOBAL_BATCH_ID, STOP_SEARCH_FLAG
    
    batches_processed = 0
    
    # Inisialisasi waktu update log untuk worker ini
    LAST_LOG_UPDATE_TIME[gpu_id] = datetime.now()
    
    while True:
        # CEK STOP_SEARCH_FLAG DI AWAL LOOP
        with STOP_SEARCH_FLAG_LOCK:
            if STOP_SEARCH_FLAG:
                safe_print(f"[GPU {gpu_id}] ⚠️ STOP_SEARCH_FLAG detected. Worker stopping...")
                break
        
        # 1. Ambil Batch ID berikutnya secara aman (Thread Safe)
        batch_id_to_process = -1
        with BATCH_ID_LOCK:
            batch_id_to_process = CURRENT_GLOBAL_BATCH_ID
            CURRENT_GLOBAL_BATCH_ID += 1
            
        # 2. Ambil Data Batch dari DB
        batch = get_batch_by_id(batch_id_to_process)
        
        if not batch:
            safe_print(f"[GPU {gpu_id}] ❌ Batch ID {batch_id_to_process} not found in DB. Worker stopping.")
            log_xiebo_output(gpu_id, f"Batch ID {batch_id_to_process} not found in DB. Worker stopping.")
            break
            
        status = (batch.get('status') or '0').strip()
        
        # Skip jika sudah selesai atau sedang dikerjakan
        if status == 'done' or status == 'inprogress':
            # Jangan print skip terlalu banyak agar log bersih
            if batch_id_to_process % 100 == 0: 
                log_xiebo_output(gpu_id, f"Skipping ID {batch_id_to_process} (Status: {status})")
            continue
            
        start_range = batch['start_range']
        end_range = batch['end_range']
        range_bits = calculate_range_bits(start_range, end_range)
        
        # 3. Jalankan Xiebo (FUNGSI INI AKAN SELESAIKAN SEMUA PROSES TERMASUK UPDATE DATABASE)
        return_code, found_info = run_xiebo(gpu_id, start_range, range_bits, address, batch_id=batch_id_to_process)
        
        batches_processed += 1
            
        # Delay sedikit antar batch per GPU agar tidak terlalu spam request ke DB/Screen
        time.sleep(1)

    safe_print(f"[GPU {gpu_id}] 🛑 Worker stopped. Processed {batches_processed} batches.")
    log_xiebo_output(gpu_id, f"Worker stopped. Processed {batches_processed} batches.")
    
    # LAPORKAN KE DATABASE TERAKHIR JIKA PERLU
    if batches_processed > 0:
        log_xiebo_output(gpu_id, f"Worker exit due to {'STOP_SEARCH_FLAG' if STOP_SEARCH_FLAG else 'normal completion'}")

def main():
    global STOP_SEARCH_FLAG, CURRENT_GLOBAL_BATCH_ID
    
    STOP_SEARCH_FLAG = False
    
    # Pastikan directory log ada
    ensure_log_dir()
    
    if len(sys.argv) < 2:
        print("Xiebo Multi-GPU Batch Runner")
        print("Usage:")
        print("  Multi-GPU DB: python3 bm.py --batch-db GPU_IDS START_ID ADDRESS")
        print("  Example:      python3 bm.py --batch-db 0,1,2,3 1000 13zpGr...")
        print("  Single Run:   python3 bm.py GPU_ID START_HEX RANGE_BITS ADDRESS")
        print(f"\n📝 Log files will be saved in: {os.path.abspath(LOG_DIR)}")
        print(f"📋 Log preview every {LOG_UPDATE_INTERVAL/60} minutes")
        print(f"🚫 No real-time output to terminal (only saved to log)")
        sys.exit(1)
    
    # Mode Multi-GPU Database
    if sys.argv[1] == "--batch-db" and len(sys.argv) == 5:
        gpu_ids_str = sys.argv[2]
        start_id = int(sys.argv[3])
        address = sys.argv[4]
        
        # Parse list GPU (misal "0,1,2" -> [0, 1, 2])
        gpu_ids = [int(x.strip()) for x in gpu_ids_str.split(',')]
        
        # Set Global Start ID
        CURRENT_GLOBAL_BATCH_ID = start_id
        
        print(f"\n{'='*80}")
        print(f"🚀 MULTI-GPU BATCH MODE STARTED")
        print(f"{'='*80}")
        print(f"GPUs Active : {gpu_ids}")
        print(f"Start ID    : {start_id}")
        print(f"Address     : {address}")
        print(f"Log Dir     : {os.path.abspath(LOG_DIR)}")
        print(f"Log Preview : Every {LOG_UPDATE_INTERVAL/60} minutes ({LOG_LINES_TO_SHOW} lines)")
        print(f"Terminal    : NO real-time output (quiet mode)")
        print(f"{'='*80}\n")
        
        threads = []
        
        # Buat dan jalankan thread untuk setiap GPU
        for gpu in gpu_ids:
            t = threading.Thread(target=gpu_worker, args=(gpu, address))
            t.daemon = True # Agar thread mati jika main program di kill
            threads.append(t)
            t.start()
            print(f"✅ Started worker thread for GPU {gpu}")
            print(f"   Log file: {get_gpu_log_file(gpu)}")
        
        # Main loop untuk menjaga program tetap berjalan dan handle KeyboardInterrupt
        try:
            while True:
                # Cek apakah semua thread masih hidup
                alive_threads = [t for t in threads if t.is_alive()]
                if not alive_threads:
                    print("\nAll workers have finished.")
                    break
                
                # Cek STOP_SEARCH_FLAG
                with STOP_SEARCH_FLAG_LOCK:
                    if STOP_SEARCH_FLAG:
                        print("\n🛑 Stop Flag Detected. Waiting for workers to finish current batches...")
                        # Beri waktu worker untuk menyelesaikan batch yang sedang berjalan
                        time.sleep(10)  # Beri waktu lebih lama
                        
                time.sleep(2)
                
            # Wait for all threads dengan timeout lebih lama
            for t in threads:
                t.join(timeout=15)
                
            print(f"\n{'='*80}")
            print(f"🏁 PROGRAM COMPLETED")
            print(f"{'='*80}")
            print(f"Stop Flag Status: {'ACTIVATED - Private Key Found!' if STOP_SEARCH_FLAG else 'Not Activated'}")
            print(f"Check log files in: {os.path.abspath(LOG_DIR)}")
            
            # Verifikasi status batch terakhir
            if STOP_SEARCH_FLAG:
                print(f"\n📊 Checking final batch status in database...")
                # Anda bisa menambahkan kode untuk memverifikasi status terakhir di sini
            
        except KeyboardInterrupt:
            print(f"\n\n{'='*80}")
            print(f"⚠️  STOPPED BY USER INTERRUPT (Ctrl+C)")
            print(f"{'='*80}")
            with STOP_SEARCH_FLAG_LOCK:
                STOP_SEARCH_FLAG = True
            # Beri waktu worker untuk cleanup
            time.sleep(10)
            print(f"Waiting for workers to finish...")
            for t in threads:
                t.join(timeout=10)
            print(f"Clean shutdown completed.")
            
    # Single run mode (Legacy support)
    elif len(sys.argv) == 5:
        gpu_id = sys.argv[1]
        start_hex = sys.argv[2]
        range_bits = int(sys.argv[3])
        address = sys.argv[4]
        
        # Pastikan directory log ada
        ensure_log_dir()
        
        run_xiebo(gpu_id, start_hex, range_bits, address)
        
    else:
        print("Invalid arguments")
        print("Usage: python3 bm.py --batch-db 0,1,2 1000 1Address...")
        
if __name__ == "__main__":
    if not os.path.exists("./xiebo"):
        print("❌ Error: xiebo binary not found")
        sys.exit(1)
        
    if not os.access("./xiebo", os.X_OK):
        os.chmod("./xiebo", 0o755)
        
    if os.name == 'posix':
        os.system('')
        
    main()
