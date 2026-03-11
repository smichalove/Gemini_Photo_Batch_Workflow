import os
import json
import subprocess
import concurrent.futures
from datetime import datetime
from typing import List, Dict, Optional, Tuple, Any

"""
Utility to locate files modified today, read any manual descriptions placed by the user
via ACDSee or other editors, and push those descriptions back into the tracking JSON database.
"""

# Hardcoded user paths for scanning
PHOTO_DIR: str = r"D:\Users"
OUTPUT_JSON: str = os.path.dirname(os.path.abspath(__file__)) + "\\photo_descriptions.json"

def extract_exif(path: str) -> Tuple[Optional[str], Optional[str]]:
    """
    Attempts to extract commonly used description tags from an image using ExifTool.
    
    Args:
        path (str): The absolute local system path to the image file.
        
    Returns:
        Tuple[Optional[str], Optional[str]]: The lowercase path string and the extracted description string.
                                             Returns (None, None) on failure or missing executable.
    """
    try:
        # Try a few common description tags used by editors like ACDSee/Lightroom
        exiftool_exe: str = os.path.join(os.path.dirname(os.path.abspath(__file__)), "exiftool", "exiftool.exe")
        if not os.path.exists(exiftool_exe):
            exiftool_exe = 'exiftool' # fallback to global path
            
        result = subprocess.run(
            [exiftool_exe, '-s', '-s', '-s', '-Description', '-Title', '-UserComment', '-Caption-Abstract', path], 
            capture_output=True, 
            text=True
        )
        out: str = result.stdout.strip()
        if out:
            # Just take the first non-empty line returned
            desc: str = out.split('\n')[0].strip()
            if desc:
                return path.lower(), desc
    except Exception as e:
        print(f"Error reading {os.path.basename(path)}: {e}")
        
    return None, None

def main() -> None:
    """
    Scans the PHOTO_DIR for files modified today, extracts their metadata,
    and updates the main tracking database if descriptions differ.
    
    Args:
        None
    
    Returns:
        None (Mutates OUTPUT_JSON directly)
    """
    print("===================================================")
    print("Manual Update Metadata Sync To Database")
    print("===================================================")

    # 1. Locate all the user's photos from D:\Users modified today
    today = datetime.now().date()
    print(f"Scanning '{PHOTO_DIR}' for files modified today ({today})...")

    updated_files: List[str] = []
    
    # os.walk to find files modified today
    for root, dirs, files in os.walk(PHOTO_DIR):
        for f in files:
            if f.lower().endswith(('.jpg', '.jpeg', '.png', '.tif', '.tiff')):
                full_path: str = os.path.join(root, f)
                try:
                    mtime: float = os.path.getmtime(full_path)
                    mdate = datetime.fromtimestamp(mtime).date()
                    if mdate == today:
                        updated_files.append(full_path)
                except Exception:
                    pass

    print(f"Found {len(updated_files)} images modified today.")

    # 2. Extract metadata using ExifTool
    print("Extracting updated descriptions via ExifTool (using 32 threads)...")
    descriptions: Dict[str, str] = {}

    with concurrent.futures.ThreadPoolExecutor(max_workers=32) as executor:
        results = executor.map(extract_exif, updated_files)
        for p_lower, desc in results:
            if p_lower and desc:
                descriptions[p_lower] = desc

    print(f"Successfully extracted {len(descriptions)} descriptions.")

    # 3. Update the tracking database
    print("Updating database...")
    if not os.path.exists(OUTPUT_JSON):
        print("Database not found!")
        exit(1)

    db: List[Dict[str, Any]] = []
    with open(OUTPUT_JSON, 'r', encoding='utf-8') as f:
        db = json.load(f)

    # Need to hash everything down to lowercase since Windows paths can drift depending on root parsing
    updated_count: int = 0
    for entry in db:
        path: str = entry.get('full_path', '').lower()
        
        if path in descriptions:
            old_desc: Optional[str] = entry.get('description')
            new_desc: str = descriptions[path]
            
            # Don't overwrite if it already has the exact same description
            if old_desc != new_desc:
                entry['description'] = new_desc
                updated_count += 1
                print(f"✅ Updated: {entry.get('full_path')} -> '{str(new_desc)[:60]}...'")

    with open(OUTPUT_JSON, 'w', encoding='utf-8') as f:
        json.dump(db, f, indent=4)

    print(f"Workflow Complete! Hand-updated {updated_count} legacy records in the database.")

if __name__ == "__main__":
    main()
