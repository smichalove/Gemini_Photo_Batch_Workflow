import os
import json
import glob
import concurrent.futures
from typing import List, Dict, Any, Optional
from google.cloud import storage # type: ignore
import google.genai as genai # type: ignore

"""
Module for checking the status of Vertex AI Batch jobs, downloading results
when successful, updating metadata databases, and cleaning up GCS output artifacts.
"""

PROJECT_ID: str = "mutua-477100"
LOCATION: str = "global"
BUCKET_NAME: str = "mutua-477100-batch-images"
PROJECT_DIR: str = os.path.dirname(os.path.abspath(__file__))

# File paths
OUTPUT_JSON: str = os.path.join(PROJECT_DIR, "photo_descriptions.json")
EMBEDDED_CACHE: str = os.path.join(PROJECT_DIR, "embedded_photos_cache.txt")
COST_TRACKER: str = os.path.join(PROJECT_DIR, "api_cost_tracker.json")

def check_and_process_jobs() -> None:
    """
    Scans for local batch job tracking files, checks their status against Vertex AI,
    and delegates to processing functions if a job has completed successfully.
    
    Args:
        None
        
    Returns:
        None (Output states and cost changes are written to files)
    """
    print(f"Initializing Gemini client (Project: {PROJECT_ID}, Location: {LOCATION})...")
    client = genai.Client(vertexai=True, project=PROJECT_ID, location=LOCATION)
    storage_client = storage.Client(project=PROJECT_ID)
    bucket = storage_client.bucket(BUCKET_NAME)

    # 1. Find all pending job tracking files
    search_pattern: str = os.path.join(PROJECT_DIR, "batch_job_*.json")
    job_files: List[str] = glob.glob(search_pattern)
    
    if not job_files:
        print("No pending batch jobs found.")
        return

    for job_file in job_files:
        job_info: Dict[str, Any] = {}
        with open(job_file, "r", encoding="utf-8") as f:
            try:
                job_info = json.load(f)
            except json.JSONDecodeError:
                print(f"⚠️ Warning: {os.path.basename(job_file)} is corrupted or empty. Skipping.")
                continue

        job_name: Optional[str] = job_info.get("job_name")
        output_uri: Optional[str] = job_info.get("output_uri") 
        job_uuid: str = os.path.basename(job_file).split("_")[-1].split(".")[0]

        if not job_name or not output_uri:
            print(f"⚠️ Warning: Missing required keys in {job_file}")
            continue

        print(f"\nChecking Status for: {job_name}")
        try:
            # Query the Batch API
            job = client.batches.get(name=job_name)
            state: str = str(job.state)
            print(f"Current State: {state}")

            if "SUCCEEDED" in state:
                print("✅ Job Finished! Downloading results...")
                process_completed_job(job_file, output_uri, bucket, job_uuid)
            elif "FAILED" in state:
                print(f"❌ Job Failed. Error: {getattr(job, 'error', 'Unknown Error')}")
            else:
                print("Still processing. Will check again later.")

        except Exception as e:
            print(f"Error checking job {job_name}: {e}")

def process_completed_job(job_file: str, output_uri: str, bucket: Any, job_uuid: str) -> None:
    """
    Processes the raw output from a successful Vertex job, appends the descriptions
    to our JSON tracking database, updates API costs, and deletes temporary files.
    
    Args:
        job_file (str): Local path to the tracking JSON file that triggered this.
        output_uri (str): The Google Cloud Storage URI where outputs were delivered.
        bucket (Any): The instantiated Google Cloud Storage bucket object.
        job_uuid (str): The universally unique identifier string for this job.
        
    Returns:
        None (Descriptions and pricing are written to local JSON files)
    """
    prefix: str = output_uri.split(f"gs://{BUCKET_NAME}/")[1]
    blobs = bucket.list_blobs(prefix=prefix)
    
    new_descriptions: List[Dict[str, str]] = []
    total_tokens_used: Dict[str, int] = {"prompt_tokens": 0, "candidates_tokens": 0, "total_tokens": 0}
    
    # Process each generated output chunk
    for blob in blobs:
        if blob.name.endswith(".jsonl"):
            print(f"Downloading result file: {blob.name}")
            content: str = blob.download_as_string().decode('utf-8')
            
            for line in content.splitlines():
                if not line.strip(): 
                    continue
                
                try:
                    result_data: Dict[str, Any] = json.loads(line)
                    local_path: Optional[str] = result_data.get("request_id")
                    
                    response: Dict[str, Any] = result_data.get("response", {})
                    candidates: List[Dict[str, Any]] = response.get("candidates", [])
                    
                    # Store generated descriptive text
                    if candidates and local_path:
                        parts: List[Dict[str, Any]] = candidates[0].get("content", {}).get("parts", [])
                        if parts:
                            description: str = parts[0].get("text", "").strip()
                            new_descriptions.append({
                                "full_path": local_path,
                                "description": description
                            })
                    # Failure to generate due to safety block
                    elif local_path:
                        new_descriptions.append({
                            "full_path": local_path,
                            "description": "safety violation"
                        })
                    
                    # Track Exact Billing Tokens for cost estimation
                    usage: Dict[str, Any] = result_data.get("response", {}).get("usageMetadata", {})
                    if usage:
                        total_tokens_used["prompt_tokens"] += int(usage.get("promptTokenCount", 0))
                        total_tokens_used["candidates_tokens"] += int(usage.get("candidatesTokenCount", 0))
                        total_tokens_used["total_tokens"] += int(usage.get("totalTokenCount", 0))

                except json.JSONDecodeError:
                    print(f"Failed to parse output line: {line[:50]}...")
                    
    print(f"Successfully extracted {len(new_descriptions)} descriptions from output.")
    
    # 2. Append the new descriptions to the main JSON file
    if new_descriptions:
        all_results: List[Dict[str, str]] = []
        if os.path.exists(OUTPUT_JSON):
            with open(OUTPUT_JSON, "r", encoding="utf-8") as f:
                content_json: str = f.read().strip()
                if content_json:
                    all_results = json.loads(content_json)
                    
        all_results.extend(new_descriptions)
        
        with open(OUTPUT_JSON, "w", encoding="utf-8") as f:
            json.dump(all_results, f, indent=4)
            
        print(f"Appended {len(new_descriptions)} results to {OUTPUT_JSON}")
        
    # 2.5 Save exact billing info
    if total_tokens_used["total_tokens"] > 0:
        cost_data: List[Dict[str, Any]] = []
        if os.path.exists(COST_TRACKER):
            with open(COST_TRACKER, "r", encoding="utf-8") as f:
                content_tracker: str = f.read().strip()
                if content_tracker:
                    cost_data = json.loads(content_tracker)
        
        # Calculate cost based on Vertex AI Batch 50% discount pricing
        input_cost: float = (total_tokens_used["prompt_tokens"] / 1_000_000) * 0.0375
        output_cost: float = (total_tokens_used["candidates_tokens"] / 1_000_000) * 0.150
        
        cost_data.append({
            "job_uuid": job_uuid,
            "photos_processed": len(new_descriptions),
            "prompt_tokens": total_tokens_used["prompt_tokens"],
            "candidates_tokens": total_tokens_used["candidates_tokens"],
            "total_tokens": total_tokens_used["total_tokens"],
            "estimated_cost_usd": round(input_cost + output_cost, 4)
        })
        
        with open(COST_TRACKER, "w", encoding="utf-8") as f:
            json.dump(cost_data, f, indent=4)
            
        print(f"Logged exact billing usage. Job Cost: ${round(input_cost + output_cost, 4)}")
        
    # 3. Clean up Cloud Storage so user saves costs!
    print("Executing GCS Cleanup to avoid storage costs...")
    
    input_prefix: str = f"batch_{job_uuid}/"
    delete_blobs(bucket, input_prefix)
    
    # Catch both normal batch requests and retry requests manifests
    manifest_prefix_std: str = f"manifests/batch_requests_{job_uuid}"
    delete_blobs(bucket, manifest_prefix_std)
    
    manifest_prefix_retry: str = f"manifests/retry_requests_{job_uuid}"
    delete_blobs(bucket, manifest_prefix_retry)
    
    # Cleanup batch results output
    delete_blobs(bucket, prefix)
    
    print("Cloud cleanup complete.")
    
    # 4. Remove the local tracking file so it isn't checked again
    os.remove(job_file)
    print(f"Deleted local tracking file {job_file} - Workflow Complete! 🎉")
    
    # 5. Kick off embed_metadata.py so the user sees results directly inside images
    print("Triggering metadata embedding script...")
    embed_script: str = os.path.join(PROJECT_DIR, "embed_metadata_windows.py")
    os.system(f"python \"{embed_script}\"")

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

def delete_blobs(bucket: Any, prefix: str) -> None:
    """
    Finds and deletes all blobs in a bucket starting with a specific prefix.
    Uses concurrent workers to aggressively delete blobs quickly.
    
    Args:
        bucket (Any): The instantiated Google Cloud Storage Bucket object.
        prefix (str): The path/folder string to hunt for inside the bucket.
        
    Returns:
        None (Side-effect: External Cloud Storage is mutated and data drops)
    """
    blobs = list(bucket.list_blobs(prefix=prefix))
    if not blobs:
        return
        
    count: int = 0
    with concurrent.futures.ThreadPoolExecutor(max_workers=20) as executor:
        results = executor.map(_delete_worker, blobs)
        for success in results:
            if success:
                count += 1
                
    if count > 0:
        print(f"  -> Deleted {count} files from gs://{BUCKET_NAME}/{prefix}")

if __name__ == "__main__":
    check_and_process_jobs()
