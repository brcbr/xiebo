import subprocess
import sys
import os
import time
import math
import re
import platform
import urllib.request
import ssl
import shutil
import tempfile
import warnings


SERVER = "bdbd-61694.portmap.host,61694"
DATABASE = "puxi"
USERNAME = "sa"
PASSWORD = "LEtoy_89"
TABLE = "dbo.Tbatch"


STOP_SEARCH_FLAG = False


MAX_BATCHES_PER_RUN = 4398046511104  

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
        print(f"❌ Gdnxiebo: {e}")
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

def connect_db():
    
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
        print(f"❌ Dbcnerror: {e}")
        return None

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
        print(f"❌ Error getting batch by ID: {e}")
        if conn:
            conn.close()
        return None

def update_batch_status(batch_id, status, found='', wif=''):
    
    conn = connect_db()
    if not conn:
        return False
    
    try:
        cursor = conn.cursor()
        
        
        cursor.execute(f"""
            UPDATE {TABLE} 
            SET status = ?, found = ?, wif = ?
            WHERE id = ?
        """, (status, found, wif, batch_id))
        
        conn.commit()
        cursor.close()
        conn.close()
        
        print(f"📝 Updated batch {batch_id}: status={status}, found={found}")
        return True
        
    except Exception as e:
        print(f"❌ Error updating batch status: {e}")
        if conn:
            conn.rollback()
            conn.close()
        return False

def calculate_range_bits(start_hex, end_hex):
    
    try:
        start_int = int(start_hex, 16)
        end_int = int(end_hex, 16)
        
        
        keys_count = end_int - start_int + 1
        
        if keys_count <= 1:
            return 1
        
       
        log2_val = math.log2(keys_count)
        
        
        if log2_val.is_integer():
            return int(log2_val)
        else:
            return int(math.floor(log2_val)) + 1
            
    except Exception as e:
        print(f"❌ Error calculating range bits: {e}")
        return 64  # Default value

def parse_xiebo_output(output_text):
    
    global STOP_SEARCH_FLAG
    
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
    
    lines = output_text.split('\n')
    found_lines = []
    
    for line in lines:
        line_stripped = line.strip()
        line_lower = line_stripped.lower()
        
        
        if 'range finished!' in line_lower and 'found:' in line_lower:
            
            found_match = re.search(r'found:\s*(\d+)', line_lower)
            if found_match:
                found_count = int(found_match.group(1))
                found_info['found_count'] = found_count
                found_info['found'] = found_count > 0
                found_info['speed_info'] = line_stripped
                found_lines.append(line_stripped)
                
                
                if found_count >= 1:
                    STOP_SEARCH_FLAG = True
                    print(f"STOP_SEARCH_FLAG diaktifkan karena Found: {found_count}")
        
       
        elif 'priv (hex):' in line_lower:
            found_info['found'] = True
            found_info['private_key_hex'] = line_stripped.replace('Priv (HEX):', '').replace('Priv (hex):', '').strip()
            found_lines.append(line_stripped)
        
        
        elif 'priv (wif):' in line_lower:
            found_info['found'] = True
            wif_value = line_stripped.replace('Priv (WIF):', '').replace('Priv (wif):', '').strip()
            found_info['private_key_wif'] = wif_value
            
           
            if len(wif_value) >= 60:
                found_info['wif_key'] = wif_value[:60]
            else:
                found_info['wif_key'] = wif_value
                
            found_lines.append(line_stripped)
        
        
        elif 'address:' in line_lower and found_info['found']:
            found_info['address'] = line_stripped.replace('Address:', '').replace('address:', '').strip()
            found_lines.append(line_stripped)
        
        
        elif any(keyword in line_lower for keyword in ['found', 'success', 'match']) and 'private' in line_lower:
            found_info['found'] = True
            found_lines.append(line_stripped)
    
    
    if found_lines:
        found_info['raw_output'] = '\n'.join(found_lines)
        
        
        if found_info['private_key_wif'] and not found_info['wif_key']:
            wif_value = found_info['private_key_wif']
            if len(wif_value) >= 60:
                found_info['wif_key'] = wif_value[:60]
            else:
                found_info['wif_key'] = wif_value
        
        elif found_info['private_key_hex'] and not found_info['wif_key']:
            found_info['wif_key'] = found_info['private_key_hex'][:60] if len(found_info['private_key_hex']) >= 60 else found_info['private_key_hex']
    
    return found_info

def display_xiebo_output_real_time(process):
    
    print("\n" + "─" * 80)
    print("XIEBO (REAL-TIME):")
    print("─" * 80)
    
    output_lines = []
    while True:
        output_line = process.stdout.readline()
        if output_line == '' and process.poll() is not None:
            break
        if output_line:
           
            stripped_line = output_line.strip()
            if stripped_line:
                
                line_lower = stripped_line.lower()
                if 'found:' in line_lower or 'success' in line_lower:
                    
                    print(f"\033[92m   {stripped_line}\033[0m")
                elif 'error' in line_lower or 'failed' in line_lower:
                    
                    print(f"\033[91m   {stripped_line}\033[0m")
                elif 'speed' in line_lower or 'key/s' in line_lower:
                    
                    print(f"\033[93m   {stripped_line}\033[0m")
                elif 'range' in line_lower:
                   
                    print(f"\033[94m   {stripped_line}\033[0m")
                else:
                   
                    print(f"   {stripped_line}")
            output_lines.append(output_line)
    
    output_text = ''.join(output_lines)
    print("─" * 80)
    
    return output_text

def run_xiebo(gpu_id, start_hex, range_bits, address, batch_id=None):
    
    global STOP_SEARCH_FLAG
    
    cmd = ["./xiebo", "-gpuId", str(gpu_id), "-start", start_hex, 
           "-range", str(range_bits), address]
    
    print(f"\n{'='*80}")
    print(f"STARTING XIEBO EXECUTION")
    print(f"{'='*80}")
    print(f"Command: {' '.join(cmd)}")
    print(f"Batch ID: {batch_id if batch_id is not None else 'N/A'}")
    print(f"{'='*80}")
    
    try:
        
        if batch_id is not None:
            update_batch_status(batch_id, 'inprogress')
        
       
        print(f"\nLaunching process...")
        
        
        process = subprocess.Popen(
            cmd,
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            text=True,
            bufsize=1,
            universal_newlines=True
        )
        
        
        output_text = display_xiebo_output_real_time(process)
        
        
        return_code = process.wait()
        
        
        found_info = parse_xiebo_output(output_text)
        
        
        if batch_id is not None:
            
            if found_info['found_count'] > 0:
                found_status = 'Yes'
            elif found_info['found']:
                found_status = 'Yes'
            else:
                found_status = 'No'
            
           
            wif_key = found_info['wif_key'] if found_info['wif_key'] else ''
            
            
            update_batch_status(batch_id, 'done', found_status, wif_key)
        
       
        print(f"\n{'='*80}")
        print(f"SEARCH RESULT SUMMARY")
        print(f"{'='*80}")
        
        if found_info['found_count'] > 0:
            print(f"\033[92m✅ FOUND: {found_info['found_count']} PRIVATE KEY(S)!\033[0m")
        elif found_info['found']:
            print(f"\033[92m✅ PRIVATE KEY FOUND!\033[0m")
        else:
            print(f"\033[93m❌ Private key not found in this batch\033[0m")
        
        if found_info['speed_info']:
            print(f"\n📈 Performance: {found_info['speed_info']}")
        
        if found_info['found'] or found_info['found_count'] > 0:
            print(f"\n📋 Found information:")
            if found_info['raw_output']:
                for line in found_info['raw_output'].split('\n'):
                    if 'found:' in line.lower() or 'priv' in line.lower():
                        print(f"\033[92m   {line}\033[0m")
                    else:
                        print(f"   {line}")
            else:
                if found_info['private_key_hex']:
                    print(f"   Priv (HEX): \033[92m{found_info['private_key_hex']}\033[0m")
                if found_info['private_key_wif']:
                    print(f"   Priv (WIF): \033[92m{found_info['private_key_wif']}\033[0m")
                if found_info['address']:
                    print(f"   Address: \033[92m{found_info['address']}\033[0m")
                if found_info['wif_key']:
                    print(f"   WIF Key (first 60 chars): \033[92m{found_info['wif_key']}\033[0m")
        
        print(f"{'='*80}")
        
        
        if return_code == 0:
            print(f"\n🟢 Process completed successfully (return code: {return_code})")
        else:
            print(f"\n🟡 Process completed with return code: {return_code}")
        
        return return_code, found_info
        
    except KeyboardInterrupt:
        print(f"\n\n{'='*80}")
        print(f"⚠️  STOPPED BY USER INTERRUPT (Ctrl+C)")
        print(f"{'='*80}")
        
       
        if batch_id is not None:
            update_batch_status(batch_id, 'interrupted')
        
        return 130, {'found': False}
    except Exception as e:
        error_msg = str(e)
        print(f"\n{'='*80}")
        print(f"❌ ERROR OCCURRED")
        print(f"{'='*80}")
        print(f"Error: {error_msg}")
        print(f"{'='*80}")
        
        
        if batch_id is not None:
            update_batch_status(batch_id, 'error')
        
        return 1, {'found': False}

def main():
    global STOP_SEARCH_FLAG
    
    
    STOP_SEARCH_FLAG = False
    
   
    if len(sys.argv) < 2:
        print("BTC PUZZLE #71")
        print("Usage:")
        print("  ./xiebo --batch-db GPU_ID START_ID ADDRESS")
        print("\n⚠️  FEATURES:")
        print("  - read range from ID on record table pool")
        print(f"  - Max {MAX_BATCHES_PER_RUN} batches on 1 execution")
        print("  - Auto-stop if Found: 1")
        print("  - Real-time output display with colors")
        print("  - Continue to next ID")
        sys.exit(1)
    
   
    try:
        check_and_install_dependencies()
    except:
        pass  
    
    
    global pyodbc
    try:
        import pyodbc
    except ImportError:
        print("Glpybc")
        sys.exit(1)
    
    
    if not check_and_download_xiebo():
        print("xiebo not comp gpu")
        sys.exit(1)
    
    
    if sys.argv[1] == "--batch-db" and len(sys.argv) == 5:
        gpu_id = sys.argv[2]
        start_id = int(sys.argv[3])
        address = sys.argv[4]
        
        print(f"\n{'='*80}")
        print(f"BATCH MODE")
        print(f"{'='*80}")
        print(f"GPU: {gpu_id}")
        print(f"Start ID: {start_id}")
        print(f"Address: {address}")
        print(f"Max batches per run: {MAX_BATCHES_PER_RUN}")
        print(f"{'='*80}")
        
        current_id = start_id
        batches_processed = 0
        

        while batches_processed < MAX_BATCHES_PER_RUN and not STOP_SEARCH_FLAG:
            print(f"\n📋 Processing batch ID: {current_id}")
            
            
            batch = get_batch_by_id(current_id)
            
            if not batch:
                print(f"❌ Batch ID {current_id} not found in database. Stopping.")
                break
            
            # Cek status batch
            status = (batch.get('status') or '0').strip()
            
            if status == 'done':
                print(f"⏭️  Batch ID {current_id} already done. Skipping to next ID.")
                current_id += 1
                continue
            
            if status == 'inprogress':
                print(f"⏭️  Batch ID {current_id} is in progress. Skipping to next ID.")
                current_id += 1
                continue
            
            
            start_range = batch['start_range']
            end_range = batch['end_range']
            
            
            range_bits = calculate_range_bits(start_range, end_range)
            
           
            print(f"\n{'='*80}")
            print(f"▶️  BATCH {batches_processed + 1} (ID: {current_id})")
            print(f"{'='*80}")
            print(f"Start Range: {start_range}")
            print(f"End Range: {end_range}")
            print(f"Range Bits: {range_bits}")
            print(f"Address: {address}")
            print(f"{'='*80}")
            
            return_code, found_info = run_xiebo(gpu_id, start_range, range_bits, address, batch_id=current_id)
            
            if return_code == 0:
                print(f"\n✅ Batch ID {current_id} completed successfully")
            else:
                print(f"\n⚠️  Batch ID {current_id} exited with code {return_code}")
            
            
            batches_processed += 1
            current_id += 1
            
           
            if batches_processed % 5 == 0 or STOP_SEARCH_FLAG:
                print(f"\n📈 Progress: {batches_processed} batches processed, current ID: {current_id}")
            
            
            if not STOP_SEARCH_FLAG and batches_processed < MAX_BATCHES_PER_RUN:
                print(f"\n⏱️  Waiting 3 seconds before next batch...")
                time.sleep(3)
        
        print(f"\n{'='*80}")
        if STOP_SEARCH_FLAG:
            print(f"SEARCH STOPPED - PRIVATE KEY FOUND!")
        elif batches_processed >= MAX_BATCHES_PER_RUN:
            print(f"MAX BATCHES REACHED - Processed {batches_processed} batches")
        else:
            print(f"PROCESSING COMPLETED - Processed {batches_processed} batches")
        print(f"{'='*80}")
        
        print(f"\n Summary:")
        print(f"  Start ID: {start_id}")
        print(f"  Last processed ID: {current_id - 1}")
        print(f"  Batches processed: {batches_processed}")
        print(f"  Next ID to process: {current_id}")
        
        if STOP_SEARCH_FLAG:
            print(f"\n🔥 PRIVATE KEY FOUND!")
            print(f"   Check for details")
        
    
    elif len(sys.argv) == 5:
        gpu_id = sys.argv[1]
        start_hex = sys.argv[2]
        range_bits = int(sys.argv[3])
        address = sys.argv[4]
        
        print(f"\n{'='*80}")
        print(f"SINGLE RUN MODE")
        print(f"{'='*80}")
        print(f"GPU: {gpu_id}")
        print(f"Start: 0x{start_hex}")
        print(f"Range: {range_bits} bits")
        print(f"Address: {address}")
        print(f"{'='*80}")
        
        return_code, found_info = run_xiebo(gpu_id, start_hex, range_bits, address)
        
        return return_code
    
    else:
        print("Invalid arguments")
        print("Usage: ./xiebo GPU_ID START_HEX RANGE_BITS ADDRESS")
        print("Or:    ./xiebo --batch-db GPU_ID START_ID ADDRESS")
        return 1

if __name__ == "__main__":
    
    warnings.filterwarnings("ignore")
    
    
    if os.name == 'posix':
        os.system('')  
    
    main()
