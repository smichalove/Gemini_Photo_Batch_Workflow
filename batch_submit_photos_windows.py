import os
import json
import mimetypes
import uuid
import concurrent.futures
from typing import List, Tuple, Optional, Set, Dict, Any
from google.cloud import storage # type: ignore
import google.genai as genai # type: ignore

"""
Module for scanning local directories, uploading missing images to Google Cloud Storage,
and submitting them to Vertex AI Batch for processing.
"""

PROJECT_ID: str = "mutua-477100"
LOCATION: str = "global"
MODEL_NAME: str = "gemini-2.5-flash"
BUCKET_NAME: str = "mutua-477100-batch-images"
MAX_TEST_PHOTOS: int = 150000  # Next batch limit

# Define the local project directory dynamically based on script location
PROJECT_DIR: str = os.path.dirname(os.path.abspath(__file__))

def get_mime_type(file_path: str) -> str:
    """
    Guesses the exact mime type of a file to pass to Google Cloud Storage.
    
    Args:
        file_path (str): The absolute local path to the image file.
        
    Returns:
        str: The guessed mime type as a string, e.g., 'image/jpeg' or 'image/heif'.
    """
    # Guess mime type using built-in library
    mime, _ = mimetypes.guess_type(file_path)
    if mime:
        return mime
        
    # Fallbacks for specific extensions
    ext: str = os.path.splitext(file_path)[1].lower()
    if ext in [".heic", ".heif"]:
        return "image/heif"
        
    # Default fallback
    return "image/jpeg"

def upload_to_gcs(local_path: str, bucket_name: str, gcs_path: str) -> Tuple[bool, Optional[str]]:
    """
    Uploads a single local file to a Google Cloud Storage bucket.
    
    Args:
        local_path (str): The local fully qualified path to the file.
        bucket_name (str): The name of the target GCS bucket.
        gcs_path (str): The destination object name/path within the GCS bucket.
        
    Returns:
        Tuple[bool, Optional[str]]: A tuple containing a boolean indicating success or failure,
                                    and the GCS URI string (gs://...) if successful, or None.
    """
    try:
        # Initialize storage client and bucket instance
        client = storage.Client(project=PROJECT_ID)
        bucket = client.bucket(bucket_name)
        blob = bucket.blob(gcs_path)
        
        # Perform the upload
        blob.upload_from_filename(local_path)
        return True, f"gs://{bucket_name}/{gcs_path}"
    except Exception as e:
        print(f"    ❌ Failed to upload {local_path}: {e}")
        return False, None

def _upload_worker(args: Tuple[str, str, str]) -> Tuple[str, bool, Optional[str]]:
    """
    A worker function wrapper for concurrent uploads.
    
    Args:
        args (Tuple[str, str, str]): A tuple containing args (local_path, bucket_name, gcs_path).
        
    Returns:
        Tuple[str, bool, Optional[str]]: Local path processed, success boolean, and resulting GCS URI.
    """
    local_path, bucket_name, gcs_path = args
    success, gcs_uri = upload_to_gcs(local_path, bucket_name, gcs_path)
    return local_path, success, gcs_uri

def main() -> None:
    """
    Main entry point for scanning directories, deduplicating with caches, uploading
    to GCS, and submitting the batch JSONL request to Vertex AI.
    
    Args:
        None
    
    Returns:
        None (The output is generation of side-effect tracking files and GCS uploads).
    """
    print(f"Initializing Gemini client (Project: {PROJECT_ID}, Location: {LOCATION}, Model: {MODEL_NAME})...")
    # Initialize the Gemini GenAI high level library object
    client = genai.Client(vertexai=True, project=PROJECT_ID, location=LOCATION)
    
    # Define Local Image Search Paths
    PICTURE_DIRS: List[str] = [r"D:\Users\steven\Pictures", r"H:\\"]
    
    # State tracking file locations
    OUTPUT_JSON: str = os.path.join(PROJECT_DIR, "photo_descriptions.json")
    SUBMITTED_CACHE: str = os.path.join(PROJECT_DIR, "submitted_photos_cache.txt")
    
    # 1. Deduplication Check using Relative Paths
    processed_relative_paths: Set[str] = set()
    
    # Load recently submitted files to prevent double-submitting while jobs are in flight
    if os.path.exists(SUBMITTED_CACHE):
        with open(SUBMITTED_CACHE, "r", encoding="utf-8") as f:
            for line in f:
                if line.strip():
                    # Parse into a uniform format
                    processed_relative_paths.add(line.strip().replace("\\", "/").lower())

    # Load previously successful jobs from descriptions file
    if os.path.exists(OUTPUT_JSON):
        with open(OUTPUT_JSON, "r", encoding="utf-8") as f:
            content: str = f.read().strip()
            if content:
                for item in json.loads(content):
                    full_path: str = item.get("full_path", "")
                    
                    # Try to extract the relative path portion
                    rel_path: str = full_path
                    if "Pictures/" in full_path:
                        rel_path = full_path.split("Pictures/", 1)[1]
                    elif "Pictures\\" in full_path:
                        rel_path = full_path.split("Pictures\\", 1)[1]
                    elif "\\" in full_path: # Fallback to filename if no Pictures dir
                        rel_path = os.path.basename(full_path)
                    elif "/" in full_path:
                        rel_path = os.path.basename(full_path)
                        
                    # Normalize slashes for comparison
                    rel_path = rel_path.replace("\\", "/").lower()
                    processed_relative_paths.add(rel_path)

    print(f"Loaded {len(processed_relative_paths)} existing processed relative paths.")

    photos_to_process: List[str] = []
    image_extensions: Set[str] = {".jpg", ".jpeg", ".png", ".webp", ".heic", ".heif"}
    
    print(f"Scanning directories for unprocessed photos...")
    for pic_dir in PICTURE_DIRS:
        if not os.path.exists(pic_dir): 
            continue
            
        print(f" -> Scanning {pic_dir}")
        for root, _, files in os.walk(pic_dir):
            if "venv" in root or ".git" in root or "$RECYCLE.BIN" in root or "System Volume Information" in root: 
                continue
                
            for file in files:
                if file.startswith("._") or file.startswith(".DS_Store"):
                    continue
                    
                ext: str = os.path.splitext(file)[1].lower()
                if ext in image_extensions:
                    full_path: str = os.path.join(root, file)
                    
                    # Get relative path for this file to compare against cache
                    rel_path: str = ""
                    if "Pictures\\" in full_path:
                        rel_path = full_path.split("Pictures\\", 1)[1]
                    elif "Pictures/" in full_path:
                        rel_path = full_path.split("Pictures/", 1)[1]
                    elif "H:\\" in full_path:
                        rel_path = full_path.replace("H:\\", "")
                    elif "H:/" in full_path:
                        rel_path = full_path.replace("H:/", "")
                    else:
                        rel_path = os.path.basename(full_path)
                    
                    rel_path = rel_path.replace("\\", "/").lower()
                    
                    # Check if the photo is in the exclusion pool
                    if rel_path not in processed_relative_paths:
                        photos_to_process.append(full_path)
                        
                        if MAX_TEST_PHOTOS is not None and len(photos_to_process) >= MAX_TEST_PHOTOS:
                            break
            # Multiple breaks to fast-exit walks limits                
            if MAX_TEST_PHOTOS is not None and len(photos_to_process) >= MAX_TEST_PHOTOS:
                break
        if MAX_TEST_PHOTOS is not None and len(photos_to_process) >= MAX_TEST_PHOTOS:
            break

    # Guard clause if nothing needs processing
    if not photos_to_process:
        print("No new photos found to process!")
        return

    print(f"Found {len(photos_to_process)} unprocessed photos. Starting batch preparation.")

    # 2. Setup the text prompts
    SYSTEM_PROMPT: str = "You are a detailed image describer. Provide a complete description of the photo. Include information about the subjects, setting, lighting, mood, actions, and any text visible. Return only the description."
    PROMPT_TEXT: str = "Describe this photo completely."

    # 2. Upload images to GCS Concurrently
    jsonl_file_path: str = os.path.join(PROJECT_DIR, "batch_requests.jsonl")
    job_uuid: str = str(uuid.uuid4())[:8]
    
    # Calculate appropriate concurrent workers
    workers: int = min(64, (os.cpu_count() or 1) * 4)
    print(f"Uploading {len(photos_to_process)} images to GCS using up to {workers} threads...")
    
    successful_uploads: List[Tuple[str, str]] = []
    failed_uploads: List[str] = []
    
    upload_tasks: List[Tuple[str, str, str]] = []
    for local_img_path in photos_to_process:
        file_name: str = os.path.basename(local_img_path)
        gcs_img_path: str = f"batch_{job_uuid}/{file_name}"
        upload_tasks.append((local_img_path, BUCKET_NAME, gcs_img_path))

    # Perform Concurrent Uploads
    with concurrent.futures.ThreadPoolExecutor(max_workers=workers) as executor:
        completed: int = 0
        total: int = len(upload_tasks)
        for local_path, success, gcs_uri in executor.map(_upload_worker, upload_tasks):
            completed += 1
            if success and gcs_uri:
                successful_uploads.append((local_path, gcs_uri))
                if completed % 50 == 0 or completed == total:
                    print(f"  Progress: {completed}/{total} uploaded...")
            else:
                failed_uploads.append(local_path)
                print(f"  ❌ Failed: {local_path}")

    # 3. Generate the JSONL requests formatting for Vertex
    print("Generating JSONL manifest...")
    with open(jsonl_file_path, "w", encoding="utf-8") as f:
        for local_img_path, gcs_uri in successful_uploads:
            # We enforce request_id is the local path so it natively maps back later
            request_line: Dict[str, Any] = {
                "request_id": local_img_path, 
                "request": {
                    "contents": [{
                        "role": "user",
                        "parts": [
                            {"fileData": {"fileUri": gcs_uri, "mimeType": get_mime_type(local_img_path)}},
                            {"text": PROMPT_TEXT}
                        ]
                    }],
                    "systemInstruction": {
                        "parts": [{"text": SYSTEM_PROMPT}]
                    },
                    "safetySettings": [
                        {"category": "HARM_CATEGORY_HATE_SPEECH", "threshold": "BLOCK_NONE"},
                        {"category": "HARM_CATEGORY_HARASSMENT", "threshold": "BLOCK_NONE"},
                        {"category": "HARM_CATEGORY_SEXUALLY_EXPLICIT", "threshold": "BLOCK_NONE"},
                        {"category": "HARM_CATEGORY_DANGEROUS_CONTENT", "threshold": "BLOCK_NONE"}
                    ]
                }
            }
            f.write(json.dumps(request_line) + "\n")

    # Halt if everything failed to upload
    if not successful_uploads:
        print("❌ All image uploads failed. Aborting batch submission.")
        return
        
    print(f"✅ Successfully uploaded {len(successful_uploads)} images to GCS.")
    
    # Save a log of failed paths for posterity
    if failed_uploads:
        print(f"⚠️ Warning: {len(failed_uploads)} images failed to upload to GCS!")
        failed_log: str = os.path.join(PROJECT_DIR, f"failed_uploads_{job_uuid}.txt")
        with open(failed_log, "w", encoding="utf-8") as f:
            for fail_path in failed_uploads:
                f.write(fail_path + "\n")
        print(f"   Saved list of failed uploads to: {failed_log}")

    # Upload JSONL file to Cloud Storage as well
    print("Uploading JSONL manifest...")
    gcs_jsonl_path: str = f"manifests/batch_requests_{job_uuid}.jsonl"
    success, gcs_jsonl_uri = upload_to_gcs(jsonl_file_path, BUCKET_NAME, gcs_jsonl_path) # type: ignore
    if not success or not gcs_jsonl_uri:
        print("❌ CRITICAL ERROR: Failed to upload JSONL manifest to GCS. Aborting batch job.")
        return
        
    print(f"Manifest uploaded to: {gcs_jsonl_uri}")

    # 4. Trigger the Vertex AI Batch Job remotely
    print(f"Triggering Vertex AI Batch Job (gemini-2.5-flash) in {LOCATION}...")
    try:
        dest_uri: str = f"gs://{BUCKET_NAME}/batch_output_{job_uuid}/"
        batch_job = client.batches.create(
            model=MODEL_NAME,
            src=gcs_jsonl_uri,
            config={'dest': dest_uri}
        )
        print(f"✅ Batch Job Successfully Created!")
        print(f"   Job Resource Name: {batch_job.name}")
        print(f"   GCS Output Destination: {dest_uri}")
        print(f"   Current Status: {batch_job.state}")
        
        tracking_info: Dict[str, str] = {
            "job_name": str(batch_job.name),
            "output_uri": dest_uri,
            "status": "PENDING"
        }
        
        # Save tracking data so retrieve script can pick it up
        tracking_file: str = os.path.join(PROJECT_DIR, f"batch_job_{job_uuid}.json")
        with open(tracking_file, "w", encoding="utf-8") as f:
            json.dump(tracking_info, f, indent=4)
            
        print(f"\nSaved tracking info to {tracking_file}")
        
        # 5. Log to the submitted cache to prevent resubmitting in future identical runs before finishing
        print(f"Logging {len(successful_uploads)} photos to submitted cache...")
        with open(SUBMITTED_CACHE, "a", encoding="utf-8") as f:
            for local_img_path, _ in successful_uploads:
                rel_path = ""
                if "Pictures\\" in local_img_path:
                    rel_path = local_img_path.split("Pictures\\", 1)[1]
                elif "Pictures/" in local_img_path:
                    rel_path = local_img_path.split("Pictures/", 1)[1]
                else:
                    rel_path = os.path.basename(local_img_path)
                f.write(rel_path.replace("\\", "/").lower() + "\n")
                
        print("You can check the status in the Google Cloud Console.")
        
    except Exception as e:
        print(f"❌ Error during batch execution: {e}")

if __name__ == "__main__":
    main()
