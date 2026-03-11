import os
import json
import glob
import concurrent.futures
from typing import List, Dict, Any, Optional
from google.cloud import storage # type: ignore
import google.genai as genai # type: ignore

"""
Utility Script for monitoring Google Cloud Storage for failed or dissolved Vertex AI
Batch jobs, and proactively clearing their artifacts to avoid paying storage costs.
"""

PROJECT_ID: str = "mutua-477100"
LOCATION: str = "global"
BUCKET_NAME: str = "mutua-477100-batch-images"
PROJECT_DIR: str = os.path.dirname(os.path.abspath(__file__))

def _delete_worker(blob: Any) -> bool:
    """
    Internal wrapper method to call delete on a GCS blob and catch exceptions.
    
    Args:
        blob (Any): A google.cloud.storage.blob.Blob object
        
    Returns:
        bool: True if deletion succeeds, False otherwise.
    """
    try:
        blob.delete()
        return True
    except Exception:
        return False

def delete_blobs(bucket: Any, prefix: str) -> int:
    """
    Finds and deletes all blobs in a bucket starting with a specific prefix.
    Uses concurrent workers to aggressively delete blobs quickly.
    
    Args:
        bucket (Any): The instantiated Google Cloud Storage Bucket object.
        prefix (str): The path/folder string to hunt for inside the bucket.
        
    Returns:
        int: The number of files deleted.
    """
    blobs = list(bucket.list_blobs(prefix=prefix))
    if not blobs:
        return 0
        
    count: int = 0
    with concurrent.futures.ThreadPoolExecutor(max_workers=32) as executor:
        results = executor.map(_delete_worker, blobs)
        for success in results:
            if success is True:
                count += 1
    return count

def main() -> None:
    """
    Iterates through local job tracking files, validates them against Vertex API,
    and purges any GCS artifacts associated with failed or missing jobs.
    
    Args:
        None
        
    Returns:
        None (Outputs tracking file destructions and GCS deletions)
    """
    print(f"Connecting to Google Cloud Storage (Bucket: {BUCKET_NAME})...")
    storage_client: storage.Client = storage.Client(project=PROJECT_ID)
    bucket: storage.Bucket = storage_client.bucket(BUCKET_NAME)
    
    print(f"Initializing Gemini client (Project: {PROJECT_ID}, Location: {LOCATION})...")
    client = genai.Client(vertexai=True, project=PROJECT_ID, location=LOCATION)
    
    deleted_jobs: List[str] = []
    
    # 1. Find all local tracking jobs (both regular and retry schemas)
    search_pattern: str = os.path.join(PROJECT_DIR, 'batch_job_*.json')
    job_files: List[str] = glob.glob(search_pattern)
    
    print(f"Found {len(job_files)} local job tracking files.")
    
    for job_file in job_files:
        try:
            data: Dict[str, Any] = {}
            with open(job_file, 'r', encoding='utf-8') as f:
                content: str = f.read().strip()
                if not content: 
                    continue
                data = json.load(f)
                
            job_res_name: str = data.get('job_name', '')
            job_uuid: str = os.path.basename(job_file).split("_")[-1].split(".")[0]
            
            # Extract standard UUID by parsing away potential prefixes
            extracted_uuid: str = job_uuid
            if "retry" in os.path.basename(job_file):
                extracted_uuid = os.path.basename(job_file).replace("batch_job_", "").replace(".json", "")
                if extracted_uuid.startswith("retry_"):
                     extracted_uuid = extracted_uuid.replace("retry_", "")

            try:
                job = client.batches.get(name=job_res_name)
                state: str = str(job.state)
                
                # We only want to delete inputs/outputs/tracker if the job was cancelled or failed
                if "CANCELLED" in state or "FAILED" in state:
                    print(f"\n[Status: {state}] Cleaning up storage for {job_uuid}...")
                    
                    # Target input manifests folder
                    input_prefix: str = f"batch_{extracted_uuid}/"
                    in_count: int = delete_blobs(bucket, input_prefix)
                    print(f"  -> Deleted {in_count} input files")
                    
                    # Target generated request schemas folder
                    manifest_prefix: str = f"manifests/batch_requests_{extracted_uuid}"
                    man_count: int = delete_blobs(bucket, manifest_prefix)
                    # Support retry fallback pattern
                    if man_count == 0 and "retry" in os.path.basename(job_file):
                         man_count = delete_blobs(bucket, f"manifests/retry_requests_{extracted_uuid}")
                    print(f"  -> Deleted {man_count} manifest files")
                    
                    # Target the final JSONL output locations
                    out_count: int = 0
                    output_uri: str = data.get("output_uri", "")
                    if output_uri:
                         custom_out_prefix: str = output_uri.replace(f"gs://{BUCKET_NAME}/", "")
                         out_count += delete_blobs(bucket, custom_out_prefix)
                    else:
                         out_count += delete_blobs(bucket, f"batch_output_{extracted_uuid}/")

                    print(f"  -> Deleted {out_count} output files")
                    
                    # Print success statistics and delete the tracking artifact.
                    total_deleted: int = in_count + man_count + out_count
                    if total_deleted > 0:
                        print(f"✅ Successfully deleted {total_deleted} files for {job_uuid}")
                    else:
                        print(f"⚠️ No files found for {job_uuid}")
                        
                    print(f"🗑️ Deleting local tracking file {os.path.basename(job_file)}...")
                    os.remove(job_file)
                    deleted_jobs.append(job_file)
                else:
                    print(f"\n[Status: {state}] Skipping {job_uuid} (Still Active)")
                    
            except Exception as get_err:
                # If Job dissolves entirely from Google Cloud's end usually takes 30 days max.
                if "404" in str(get_err) or "NOT_FOUND" in str(get_err):
                    print(f"\n[NOT_FOUND] Job {job_res_name} dissolved. Removing tracker {os.path.basename(job_file)}...")
                    os.remove(job_file)
                else:
                    print(f"\nError getting job {job_res_name}: {get_err}")
                
        except Exception as e:
             print(f"Error handling file {job_file}: {e}")
            
    print(f"\nCleanup operation complete. Cleared {len(deleted_jobs)} cancelled/failed jobs.")

if __name__ == "__main__":
    main()
