import os
import json
import mimetypes
import uuid
import concurrent.futures
from google.cloud import storage
import google.genai as genai

PROJECT_ID = "mutua-477100"
LOCATION = "global"
MODEL_NAME = "gemini-2.5-flash"
BUCKET_NAME = "mutua-477100-batch-images"
MAX_TEST_PHOTOS = 150000 # Next batch limit

def get_mime_type(file_path):
    mime, _ = mimetypes.guess_type(file_path)
    if mime:
        return mime
    ext = os.path.splitext(file_path)[1].lower()
    if ext in [".heic", ".heif"]:
        return "image/heif"
    return "image/jpeg"

def upload_to_gcs(local_path, bucket_name, gcs_path):
    """Uploads a file to Google Cloud Storage. Returns True on success, False on failure."""
    try:
        client = storage.Client(project=PROJECT_ID)
        bucket = client.bucket(bucket_name)
        blob = bucket.blob(gcs_path)
        blob.upload_from_filename(local_path)
        return True, f"gs://{bucket_name}/{gcs_path}"
    except Exception as e:
        print(f"    ❌ Failed to upload {local_path}: {e}")
        return False, None

def _upload_worker(args):
    """Worker function for concurrent uploads."""
    local_path, bucket_name, gcs_path = args
    success, gcs_uri = upload_to_gcs(local_path, bucket_name, gcs_path)
    return local_path, success, gcs_uri

def load_prompt_template(filename: str, default_text: str) -> str:
    """Reads a prompt template file from the workspace.

    Args:
        filename: The base name of the text file (e.g., 'system_prompt.txt').
        default_text: Fallback string if the file is missing.

    Returns:
        The loaded prompt string stripped of whitespace.
    """
    script_dir = os.path.dirname(os.path.abspath(__file__))
    path = os.path.join(script_dir, "prompts", filename)
    if os.path.exists(path):
        try:
            with open(path, "r", encoding="utf-8") as f:
                return f.read().strip()
        except Exception as e:
            print(f"⚠️ Warning: Failed to read prompt file {filename}: {e}")
    return default_text

def main():
    print(f"Initializing Gemini client (Project: {PROJECT_ID}, Location: {LOCATION}, Model: {MODEL_NAME})...")
    client = genai.Client(vertexai=True, project=PROJECT_ID, location=LOCATION)
    
    # Windows Paths
    PICTURE_DIRS = [r"D:\Users\steven\Pictures"]
    PROJECT_DIR = r"H:\Wan_project"
    OUTPUT_JSON = os.path.join(PROJECT_DIR, "photo_descriptions.json")
    SUBMITTED_CACHE = os.path.join(PROJECT_DIR, "submitted_photos_cache.txt")
    
    # 1. Deduplication Check using Relative Paths
    processed_relative_paths = set()
    
    # Load recently submitted files to prevent double-submitting while jobs are in flight
    if os.path.exists(SUBMITTED_CACHE):
        with open(SUBMITTED_CACHE, "r", encoding="utf-8") as f:
            for line in f:
                if line.strip():
                    processed_relative_paths.add(line.strip().replace("\\", "/").lower())

    if os.path.exists(OUTPUT_JSON):
        with open(OUTPUT_JSON, "r", encoding="utf-8") as f:
            content = f.read().strip()
            if content:
                for item in json.loads(content):
                    full_path = item.get("full_path", "")
                    
                    # Try to extract the relative path portion
                    rel_path = full_path
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

    photos_to_process = []
    image_extensions = {".jpg", ".jpeg", ".png", ".webp", ".heic", ".heif"}
    
    print(f"Scanning directories for unprocessed photos...")
    for pic_dir in PICTURE_DIRS:
        if not os.path.exists(pic_dir): continue
        print(f" -> Scanning {pic_dir}")
        for root, _, files in os.walk(pic_dir):
            if "venv" in root or ".git" in root or "$RECYCLE.BIN" in root or "System Volume Information" in root: continue
            for file in files:
                if file.startswith("._") or file.startswith(".DS_Store"):
                    continue
                ext = os.path.splitext(file)[1].lower()
                if ext in image_extensions:
                    full_path = os.path.join(root, file)
                    
                    # Get relative path for this file to compare against cache
                    rel_path = ""
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
                    
                    if rel_path not in processed_relative_paths:
                        photos_to_process.append(full_path)
                        
                        if MAX_TEST_PHOTOS is not None and len(photos_to_process) >= MAX_TEST_PHOTOS:
                            break
            if MAX_TEST_PHOTOS is not None and len(photos_to_process) >= MAX_TEST_PHOTOS:
                break
        if MAX_TEST_PHOTOS is not None and len(photos_to_process) >= MAX_TEST_PHOTOS:
            break

    if not photos_to_process:
        print("No new photos found to process!")
        return

    print(f"Found {len(photos_to_process)} unprocessed photos. Starting batch preparation.")

    SYSTEM_PROMPT = load_prompt_template(
        "system_prompt.txt",
        "You are a detailed image describer. Provide a complete description of the photo. Include information about the subjects, setting, lighting, mood, actions, and any text visible. Return only the description."
    )
    PROMPT_TEXT = load_prompt_template(
        "user_prompt.txt",
        "Describe this photo completely."
    )

    # 2. Upload images to GCS Concurrently
    jsonl_file_path = os.path.join(PROJECT_DIR, "batch_requests.jsonl")
    job_uuid = str(uuid.uuid4())[:8]
    
    workers = min(64, (os.cpu_count() or 1) * 4)
    print(f"Uploading {len(photos_to_process)} images to GCS using up to {workers} threads...")
    
    successful_uploads = []
    failed_uploads = []
    
    upload_tasks = []
    for local_img_path in photos_to_process:
        file_name = os.path.basename(local_img_path)
        gcs_img_path = f"batch_{job_uuid}/{file_name}"
        upload_tasks.append((local_img_path, BUCKET_NAME, gcs_img_path))

    # ThreadPoolExecutor for parallel uploads
    with concurrent.futures.ThreadPoolExecutor(max_workers=workers) as executor:
        # TQDM-like manual tracking since we might not have it installed
        completed = 0
        total = len(upload_tasks)
        for local_path, success, gcs_uri in executor.map(_upload_worker, upload_tasks):
            completed += 1
            if success:
                successful_uploads.append((local_path, gcs_uri))
                if completed % 50 == 0 or completed == total:
                    print(f"  Progress: {completed}/{total} uploaded...")
            else:
                failed_uploads.append(local_path)
                print(f"  ❌ Failed: {local_path}")

    # 3. Generate the JSONL requests
    print("Generating JSONL manifest...")
    with open(jsonl_file_path, "w", encoding="utf-8") as f:
        for local_img_path, gcs_uri in successful_uploads:
            request_line = {
                "request_id": local_img_path, # Still use full local path here so retrieve script knows exact file location
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

    if not successful_uploads:
        print("❌ All image uploads failed. Aborting batch submission.")
        return
        
    print(f"✅ Successfully uploaded {len(successful_uploads)} images to GCS.")
    
    if failed_uploads:
        print(f"⚠️ Warning: {len(failed_uploads)} images failed to upload to GCS!")
        failed_log = os.path.join(PROJECT_DIR, f"failed_uploads_{job_uuid}.txt")
        with open(failed_log, "w", encoding="utf-8") as f:
            for fail_path in failed_uploads:
                f.write(fail_path + "\n")
        print(f"   Saved list of failed uploads to: {failed_log}")

    print("Uploading JSONL manifest...")
    gcs_jsonl_path = f"manifests/batch_requests_{job_uuid}.jsonl"
    success, gcs_jsonl_uri = upload_to_gcs(jsonl_file_path, BUCKET_NAME, gcs_jsonl_path)
    if not success:
        print("❌ CRITICAL ERROR: Failed to upload JSONL manifest to GCS. Aborting batch job.")
        return
        
    print(f"Manifest uploaded to: {gcs_jsonl_uri}")

    # 4. Trigger the Vertex AI Batch Job
    print(f"Triggering Vertex AI Batch Job (gemini-2.5-flash) in {LOCATION}...")
    try:
        dest_uri = f"gs://{BUCKET_NAME}/batch_output_{job_uuid}/"
        batch_job = client.batches.create(
            model=MODEL_NAME,
            src=gcs_jsonl_uri,
            config={'dest': dest_uri}
        )
        print(f"✅ Batch Job Successfully Created!")
        print(f"   Job Resource Name: {batch_job.name}")
        print(f"   GCS Output Destination: {dest_uri}")
        print(f"   Current Status: {batch_job.state}")
        
        tracking_info = {
            "job_name": batch_job.name,
            "output_uri": dest_uri,
            "status": "PENDING"
        }
        
        tracking_file = os.path.join(PROJECT_DIR, f"batch_job_{job_uuid}.json")
        with open(tracking_file, "w", encoding="utf-8") as f:
            json.dump(tracking_info, f, indent=4)
            
        print(f"\nSaved tracking info to {tracking_file}")
        
        # Log to the submitted cache to prevent resubmitting
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
