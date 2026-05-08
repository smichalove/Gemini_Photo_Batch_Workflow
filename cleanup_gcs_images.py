import sys
import concurrent.futures
from typing import Tuple, Optional, List
from google.cloud import storage

def delete_blob_concurrent(blob_name: str, project_id: str, bucket_name: str) -> Tuple[bool, Optional[str]]:
    """
    Deletes a specific Google Cloud Storage blob.
    Designed for concurrent execution by isolating the client per thread to prevent connection pool exhaustion.
    
    Args:
        blob_name (str): The name of the Google Cloud Storage blob to delete.
        project_id (str): The ID of the GCP project.
        bucket_name (str): The name of the GCS bucket.
        
    Returns:
        Tuple[bool, Optional[str]]: A tuple where the first element is a boolean indicating 
                                    success (True) or failure (False), and the second element 
                                    is an optional error message string if a failure occurred.
    """
    try:
        # Create a new local client for thread safety and fresh connections
        t_client: storage.Client = storage.Client(project=project_id)
        t_bucket: storage.Bucket = t_client.bucket(bucket_name)
        t_blob: storage.Blob = t_bucket.blob(blob_name)
        t_blob.delete()
        return True, None
    except Exception as e:
        return False, str(e)

def main() -> None:
    """
    Main execution function to clean up batch image blobs from the GCS bucket.
    
    This function utilizes the AMD Ryzen 9 5950X 16-Core Processor and 128GB of available system memory
    to fetch and process large lists of blobs simultaneously. The 16 cores and massive memory footprint allow
    for aggressively spinning up multiple threads without system bottlenecking, dramatically speeding up the deletion
    of leftover batch request images while safely isolating and protecting the batch output manifests.
    
    Args:
        None
        
    Returns:
        None
    """
    PROJECT_ID: str = "mutua-477100"
    BUCKET_NAME: str = "mutua-477100-batch-images"
    
    print(f"Connecting to Cloud Storage (project {PROJECT_ID})...")
    
    # Initialize the Cloud Storage client
    try:
        client: storage.Client = storage.Client(project=PROJECT_ID)
        bucket: storage.Bucket = client.bucket(BUCKET_NAME)
    except Exception as e:
        print(f"Failed to initialize storage client: {e}")
        return
    
    print(f"Listing blobs in gs://{BUCKET_NAME}...")
    
    # Fetch all relevant blobs into memory. The system has 128GB of RAM, allowing for caching 
    # of very large lists of blobs simultaneously without memory pressure.
    # OPTIMIZATION: Passing the `prefix` argument offloads the filtering to Google's backend,
    # drastically reducing network latency and memory overhead before our Ryzen CPU even 
    # needs to touch the data.
    try:
        blobs: List[storage.Blob] = list(bucket.list_blobs(prefix="batch_"))
    except Exception as e:
        print(f"Failed to list blobs: {e}")
        return
        
    to_delete: List[str] = []
    count_other: int = 0
    
    # Filter blobs: only batch request images (batch_*/...) should be deleted, not inputs/outputs
    # We already filtered for "batch_" via the prefix above, so we only need to ignore "batch_output_"
    for blob in blobs:
        if not blob.name.startswith("batch_output_"):
            to_delete.append(blob.name) # Only cache names for concurrent threads
        else:
            count_other += 1
            
    print(f"Found {len(to_delete)} image blobs to delete. (Ignored {count_other} other objects)")
    if not to_delete:
        print("No image blobs found to delete.")
        return
        
    # We leverage the Ryzen 5950X 16-Core CPU to run a high number of parallel threads
    print("Deleting images using 100 concurrent threads (Optimized for Ryzen 5950X)...")
    deleted_count: int = 0
    failed_count: int = 0
    
    with concurrent.futures.ThreadPoolExecutor(max_workers=100) as executor:
        # Submit all deletion tasks, mapping arguments correctly
        futures = {executor.submit(delete_blob_concurrent, name, PROJECT_ID, BUCKET_NAME): name for name in to_delete}
        
        # Iterate over results as they complete
        for i, future in enumerate(concurrent.futures.as_completed(futures), 1):
            success, err = future.result()
            if success:
                deleted_count += 1
            else:
                failed_count += 1
                name = futures[future]
                print(f"  Failed deleting {name}: {err}")
            
            # Log progress periodically
            if i % 100 == 0:
                print(f"  Processed {i}/{len(to_delete)} (Deleted {deleted_count}, Failed {failed_count})...")

    print(f"Finished. Successfully deleted {deleted_count} image blobs. ({failed_count} failed)")

if __name__ == "__main__":
    main()
