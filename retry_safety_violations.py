import os
import json
import uuid
import mimetypes
import concurrent.futures
from typing import List, Tuple, Dict, Any, Optional, Set
from google.cloud import storage # type: ignore
import google.genai as genai # type: ignore

"""
Module for scanning the completed photo database for 'safety violation' blocks,
and automatically resubmitting them with a safer, purely descriptive prompt to try to bypass filters.
"""

PROJECT_ID: str = "mutua-477100"
LOCATION: str = "global"
MODEL_NAME: str = "gemini-2.5-flash"
BUCKET_NAME: str = "mutua-477100-batch-images"
PROJECT_DIR: str = os.path.dirname(os.path.abspath(__file__))
JSON_FILE: str = os.path.join(PROJECT_DIR, 'photo_descriptions.json')

def get_mime_type(file_path: str) -> str:
    """
    Determines the MIME type of a file based on its extension.
    
    Args:
        file_path (str): The local path to the file.
        
    Returns:
        str: The determined MIME type (e.g., 'image/jpeg').
    """
    mime, _ = mimetypes.guess_type(file_path)
    return mime or 'image/jpeg'

def upload_gcs_worker(args: Tuple[str, str, str]) -> Tuple[bool, str, str]:
    """
    Worker function to upload a local file to Google Cloud Storage.
    Designed for concurrent execution by creating a new storage client per thread
    to prevent connection pool exhaustion.
    
    Args:
        args (Tuple[str, str, str]): A tuple containing:
            - local_path: The local file path to upload.
            - bucket_name: The GCS bucket name.
            - gcs_path: The target GCS path.
            
    Returns:
        Tuple[bool, str, str]: A tuple containing:
            - success flag (True/False)
            - local file path
            - target GCS path (if successful) or error message (if failed)
    """
    local_path, bucket_name, gcs_path = args
    try:
        # Create a local client instance specifically for this thread
        storage_client: storage.Client = storage.Client(project=PROJECT_ID)
        bucket: storage.Bucket = storage_client.bucket(bucket_name)
        blob: storage.Blob = bucket.blob(gcs_path)
        
        # Bypassing exists() check for speed and permission safety
        blob.upload_from_filename(local_path, content_type=get_mime_type(local_path))
        return True, local_path, gcs_path
    except Exception as e:
        print(f"Error uploading {local_path}: {e}")
        return False, local_path, str(e)

def main() -> None:
    """
    Main function to identify safety violations in the photo database, re-upload them to
    Google Cloud Storage using aggressive concurrency tailored for an AMD Ryzen 9 5950X,
    and submit a new Vertex AI batch job for modified analysis.
    
    Args:
        None
    
    Returns:
        None (Outputs tracking files and submits jobs to Vertex API)
    """
    if not os.path.exists(JSON_FILE):
        print(f"Error: {JSON_FILE} does not exist.")
        return

    print("Scanning photo database for safety violations...")
    
    # Load entire photo database into 128GB of RAM to prevent repeated disk I/O
    photo_data: List[Dict[str, Any]] = []
    with open(JSON_FILE, 'r', encoding='utf-8') as f:
        photo_data = json.load(f)

    photos_to_process: List[str] = []
    
    # 1. Pre-compute a set of successful paths O(1) lookups to leverage the 128GB of RAM.
    # The previous O(N^2) loop was causing the 5950X CPU to bottleneck on array traversal.
    print("Pre-computing successful paths into memory...")
    successful_paths_cache: Set[str] = set()
    for entry in photo_data:
        path: str = entry.get('full_path', '')
        desc: Optional[str] = entry.get('description')
        if path and desc not in ['safety violation', '', None]:
            successful_paths_cache.add(path.lower())
    
    print("Evaluating safety violations...")
    for entry in photo_data:
        if entry.get('description') == 'safety violation':
            full_path: str = entry.get('full_path', '')
            
            # Translate old Mac paths to Windows paths
            if full_path.startswith('/Volumes/'):
                full_path = full_path.replace('/Volumes/', 'D:\\').replace('/', '\\')
                
            if os.path.exists(full_path):
                # O(1) hash lookup instead of O(N^2) loop traversal
                if full_path.lower() in successful_paths_cache:
                    print(f"⏭️ Skipping {full_path}")
                    print(f"   -> Reason: This exact file had a 'safety violation' listed, but it was already successfully retried on another run.")
                elif full_path not in photos_to_process:
                    photos_to_process.append(full_path)
            else:
                print(f"⚠️ Skipping missing file: {full_path}")

    if not photos_to_process:
        print("✅ No safety violations found to retry.")
        return

    print(f"Found {len(photos_to_process)} safety violations to retry.")
    print("Keeping original safety violations in database; new successful descriptions will simply be appended.")

    job_uuid: str = str(uuid.uuid4())[:8]
    
    # Leverage the Ryzen 5950X 16-Core CPU with an aggressively high worker pool to blast through file uploads
    workers: int = 100 
    print(f"Uploading {len(photos_to_process)} images to GCS using up to {workers} concurrent threads...")
    
    successful_uploads: List[Tuple[str, str]] = []
    failed_uploads: List[Tuple[str, str]] = []
    upload_tasks: List[Tuple[str, str, str]] = []

    for local_img_path in photos_to_process:
        rel_path: str = ""
        if "Pictures\\" in local_img_path:
            rel_path = local_img_path.split("Pictures\\", 1)[1]
        elif "Pictures/" in local_img_path:
            rel_path = local_img_path.split("Pictures/", 1)[1]
        else:
            rel_path = os.path.basename(local_img_path)
            
        gcs_img_path: str = f"batch_{job_uuid}/{rel_path.replace('\\', '/')}"
        upload_tasks.append((local_img_path, BUCKET_NAME, gcs_img_path))

    # Multi-threading using ThreadPoolExecutor for concurrent uploads
    with concurrent.futures.ThreadPoolExecutor(max_workers=workers) as executor:
        completed: int = 0
        total: int = len(upload_tasks)
        results = executor.map(upload_gcs_worker, upload_tasks)
        
        for success, local_path, gcs_path_or_err in results:
            completed += 1
            if success:
                successful_uploads.append((local_path, gcs_path_or_err))
            else:
                failed_uploads.append((local_path, gcs_path_or_err))
            
            # Print periodic progress
            if total <= 250 or completed % 50 == 0 or completed == total:
                print(f"Uploaded: {local_path} ({completed}/{total})")

    if not successful_uploads:
        print("❌ No images were successfully uploaded. Exiting.")
        return

    jsonl_file_path: str = os.path.join(PROJECT_DIR, f"retry_requests_{job_uuid}.jsonl")
    print(f"Generating batch manifest: {jsonl_file_path}")
    
    with open(jsonl_file_path, "w", encoding='utf-8') as f:
        for local_path, gcs_path in successful_uploads:
            # Reformat the request with a safer, purely factual prompt
            request_data: Dict[str, Any] = {
                "request_id": local_path,
                "request": {
                    "contents": [
                        {
                            "role": "user",
                            "parts": [
                                {"fileData": {"fileUri": f"gs://{BUCKET_NAME}/{gcs_path}", "mimeType": get_mime_type(local_path)}},
                                {"text": "Provide a neutral, factual description of the visual elements in this image. Focus on objects, colors, and layout."}
                            ]
                        }
                    ],
                    "safetySettings": [
                        {"category": "HARM_CATEGORY_HATE_SPEECH", "threshold": "BLOCK_NONE"},
                        {"category": "HARM_CATEGORY_HARASSMENT", "threshold": "BLOCK_NONE"},
                        {"category": "HARM_CATEGORY_SEXUALLY_EXPLICIT", "threshold": "BLOCK_NONE"},
                        {"category": "HARM_CATEGORY_DANGEROUS_CONTENT", "threshold": "BLOCK_NONE"}
                    ]
                }
            }
            f.write(json.dumps(request_data) + "\n")

    print(f"Uploading manifest to gs://{BUCKET_NAME}/manifests/retry_requests_{job_uuid}.jsonl...")
    storage_client: storage.Client = storage.Client(project=PROJECT_ID)
    bucket: storage.Bucket = storage_client.bucket(BUCKET_NAME)
    manifest_blob: storage.Blob = bucket.blob(f"manifests/retry_requests_{job_uuid}.jsonl")
    manifest_blob.upload_from_filename(jsonl_file_path)

    print(f"Submitting Batch Prediction Job...")
    client = genai.Client(vertexai=True, project=PROJECT_ID, location=LOCATION)
    
    # Submitting batch job via SDK
    job = client.batches.create(
        model=MODEL_NAME,
        src=f"gs://{BUCKET_NAME}/manifests/retry_requests_{job_uuid}.jsonl",
        config={'dest': f"gs://{BUCKET_NAME}/batch_{job_uuid}/output/"}
    )

    print(f"\n✅ Vertex AI Retry Job Created!")
    print(f"Job Name (ID): {job.name}")
    print(f"Job State: {job.state}")
    
    # Store tracking json file prefixing with retry so cleanup script knows
    out_file: str = os.path.join(PROJECT_DIR, f"batch_job_retry_{job_uuid}.json")
    with open(out_file, "w") as f:
        json.dump({
            "job_name": str(job.name),
            "job_uuid": job_uuid,
            "output_uri": f"gs://{BUCKET_NAME}/batch_{job_uuid}/output/"
        }, f)
        
    print(f"Saved tracking info to {out_file}.")
    print("The primary scheduled task will automatically monitor and retrieve these retried photos.")

if __name__ == "__main__":
    main()
