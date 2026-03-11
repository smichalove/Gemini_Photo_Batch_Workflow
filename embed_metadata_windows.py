import json
import subprocess
import os
import concurrent.futures
import threading
from typing import Set, Dict, List, Any, Optional

"""
Module for running Exiftool concurrently to embed the AI generated descriptions
back into the local image's IPTC / EXIF metadata visually.
"""

PROJECT_DIR: str = os.path.dirname(os.path.abspath(__file__))
json_file: str = os.path.join(PROJECT_DIR, 'photo_descriptions.json')
embedded_cache_file: str = os.path.join(PROJECT_DIR, 'embedded_photos_cache.txt')

cache_lock: threading.Lock = threading.Lock()

def load_embedded_cache() -> Set[str]:
    """
    Reads the list of files that have already had metadata embedded physically.
    
    Args:
        None
    
    Returns:
        Set[str]: A set of relative paths that have already been embedded.
    """
    if not os.path.exists(embedded_cache_file):
        return set()
        
    cache_set: Set[str] = set()
    with open(embedded_cache_file, 'r', encoding='utf-8') as f:
        for line in f:
            if line.strip():
                full_path: str = line.strip()
                rel_path: str = full_path
                if "Pictures/" in full_path:
                    rel_path = full_path.split("Pictures/", 1)[1]
                elif "Pictures\\" in full_path:
                    rel_path = full_path.split("Pictures\\", 1)[1]
                elif "\\" in full_path: 
                    rel_path = os.path.basename(full_path)
                elif "/" in full_path:
                    rel_path = os.path.basename(full_path)
                
                rel_path = rel_path.replace("\\", "/").lower()
                cache_set.add(rel_path)
    return cache_set

def save_to_cache(file_path: str) -> None:
    """
    Thread-safe write of a newly processed file path into the embedded log.
    
    Args:
        file_path (str): The local path to append to cache.
        
    Returns:
        None
    """
    with cache_lock:
        with open(embedded_cache_file, 'a', encoding='utf-8') as f:
            f.write(file_path + '\n')

def update_metadata(entry: Dict[str, Any]) -> None:
    """
    Spawns an ExifTool subprocess to physically embed IPTC description data into the file.
    
    Args:
        entry (Dict[str, Any]): The JSON dictionary containing the file "full_path" and "description"
        
    Returns:
        None (Mutates file in-place on disk)
    """
    file_path: Optional[str] = entry.get('full_path')
    if not file_path:
        return
        
    description: str = entry.get('description', '')

    if os.path.exists(file_path):
        exiftool_exe: str = os.path.join(PROJECT_DIR, "exiftool", "exiftool.exe")
        
        # Fallback to system path if local directory isn't setup
        if not os.path.exists(exiftool_exe):
             exiftool_exe = "exiftool" 
             
        cmd: List[str] = [
            exiftool_exe,
            '-m', 
            '-charset', 'iptc=UTF8',
            '-charset', 'UTF8',      
            '-overwrite_original',
            f'-Caption-Abstract={description}',
            f'-Description={description}',
            f'-ImageDescription={description}',
            file_path
        ]
        
        try:
            subprocess.run(cmd, check=True, stdout=subprocess.DEVNULL, stderr=subprocess.PIPE)
            print(f"✅ Updated: {file_path}")
            save_to_cache(file_path)
        except subprocess.CalledProcessError as e:
            err_msg: str = e.stderr.decode().strip() if e.stderr else str(e)
            
            # Common exiftool workaround fallbacks
            if "Temporary file already exists" in err_msg:
                tmp_file: str = file_path + "_exiftool_tmp"
                if os.path.exists(tmp_file):
                    try:
                        os.remove(tmp_file)
                        subprocess.run(cmd, check=True, stdout=subprocess.DEVNULL, stderr=subprocess.PIPE)
                        print(f"✅ Updated (Cleared TMP): {file_path}")
                        save_to_cache(file_path)
                    except Exception as retry_e:
                        print(f"❌ Error updating {file_path} after clearing tmp: {retry_e}")
            elif "Bad Photoshop IRB resource" in err_msg:
                try:
                    fallback_cmd: List[str] = cmd.copy()
                    fallback_cmd.insert(4, "-Photoshop:All=")
                    subprocess.run(fallback_cmd, check=True, stdout=subprocess.DEVNULL, stderr=subprocess.PIPE)
                    print(f"✅ Updated (w/ IRB Fix): {file_path}")
                    save_to_cache(file_path)
                except subprocess.CalledProcessError as e2:
                    err_msg2: str = e2.stderr.decode().strip() if e2.stderr else str(e2)
                    print(f"❌ Error updating {file_path} (even with IRB fix): {err_msg2}")
            elif "Not a valid HEIC" in err_msg or "looks more like a JPEG" in err_msg:
                print(f"⚠️ Skipped {file_path}: Extension mismatch (.HEIC but actually JPEG).")
                save_to_cache(file_path)
            elif "Format error in file" in err_msg:
                print(f"⚠️ Skipped {file_path}: File is corrupted or unreadable.")
                save_to_cache(file_path)
            else:
                print(f"❌ Error updating {file_path}: {err_msg}")
        except FileNotFoundError:
            print("❌ Error: exiftool not found. Please ensure it is in the inner 'exiftool' folder or system PATH.")
    else:
        # Silence the warning for mac paths since we know they don't exist here
        if not file_path.startswith('/Volumes/'):
            print(f"⚠️ File not found: {file_path}")

def main() -> None:
    """
    Coordinates loading the description JSON database, filtering out already embedded files,
    and launching a concurrent ThreadPool to execute the ExifTool embedding.
    
    Args:
        None
        
    Returns:
        None (Outputs physical metadata changes and writes to cache txt)
    """
    if not os.path.exists(json_file):
        print(f"Error: {json_file} does not exist.")
        return

    photo_data: List[Dict[str, Any]] = []
    with open(json_file, 'r', encoding='utf-8') as f:
        photo_data = json.load(f)

    embedded_paths_lower: Set[str] = load_embedded_cache()
    
    unprocessed_photos: List[Dict[str, Any]] = []
    skipped_count: int = 0
    for entry in photo_data:
        fp: Optional[str] = entry.get('full_path')
        if not fp:
            continue
            
        rel_path: str = fp
        if "Pictures\\" in fp:
            rel_path = fp.split("Pictures\\", 1)[1]
        elif "Pictures/" in fp:
            rel_path = fp.split("Pictures/", 1)[1]
        elif "\\" in fp: 
            rel_path = os.path.basename(fp)
        elif "/" in fp:
            rel_path = os.path.basename(fp)
            
        rel_path = rel_path.replace("\\", "/").lower()
        
        if rel_path in embedded_paths_lower:
            skipped_count = skipped_count + 1
        else:
            unprocessed_photos.append(entry)

    if skipped_count > 0:
        print(f"Skipping {skipped_count} photos that already have embedded metadata (found in cache).")

    if not unprocessed_photos:
        print("✅ All described photos have already been embedded! Nothing left to do.")
        return

    workers: int = min(32, (os.cpu_count() or 1) * 4)
    print(f"🚀 Starting metadata embedding for {len(unprocessed_photos)} photos with {workers} concurrent threads...")
    
    with concurrent.futures.ThreadPoolExecutor(max_workers=workers) as executor:
        executor.map(update_metadata, unprocessed_photos)

    print("🎉 Metadata embedding complete.")

if __name__ == "__main__":
    main()
