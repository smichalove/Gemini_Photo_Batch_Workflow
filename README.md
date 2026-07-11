# Gemini Photo Batch Workflow

Automated workflow scripts for checking local photos, uploading to Google Cloud Storage, describing them using Vertex AI Gemini Batch API, and embedding the results back into local files.

## Initial Setup

1. Install Python 3.
2. Initialize the repo using `init_git_repo.bat`.
3. Set up authentication: Create an `auth/` directory and place your `service_account.json` key inside. See `auth/README.md` for details. 
4. External tool: Create a folder `exiftool/` and place `exiftool.exe` inside it.
5. Setup your python environment and packages with `pip install -r requirements.txt`. (Optionally run `venv-photos\Scripts\activate`).
6. Prompt templates: Configure your prompts by editing `prompts/system_prompt.txt`, `prompts/user_prompt.txt`, and `prompts/retry_prompt.txt` to align with your cataloging goals.

## Prompt Customization

To separate system behavior from core code, all model prompts are loaded dynamically from files in the `prompts/` directory:
* **`prompts/system_prompt.txt`**: System instructions steering the Gemini describer.
* **`prompts/user_prompt.txt`**: User query passed alongside each image to be described.
* **`prompts/retry_prompt.txt`**: A factual, layout-focused prompt used to bypass false-positive safety violations.

To steer the model for specific workloads (e.g. portraits, documents, landscapes, products), modify these files. The submit and retry scripts load them dynamically at execution time.

## Key Workflows

### 1. Main Batch Processing

The core data pipeline has two primary steps:

- **Submit Jobs (`run_batch_submit.bat`)**: Scans `D:\Users\steven\Pictures` and `H:\` for new, unprocessed images. Bypasses already submitted files. Uploads these images to a temporary Vertex Storage Bucket and submits a Vertex AI Batch Prediction job. 
- **Retrieve Results (`run_batch_retrieve.bat`)**: Polls the GCP system for job completion tracking files. Once a job succeeds, it downloads the descriptions, stores them in `photo_descriptions.json`, tracks the cost in `api_cost_tracker.json`, and triggers ExifTool processing automatically.

### 2. Handling Safety Content Retries

Occasionally, Vertex AI filters might flag standard photography (like artistic nude or context-heavy images) as a "safety violation". 

- **Retry Safety Violations (`run_retry_safety.bat`)**: This script reads `photo_descriptions.json` and finds any image marked as a `safety violation`. It then completely skips the normal submission queue and re-submits these specific photos to Vertex AI using a neutral, highly factual layout-focused prompt. This neutral prompt is specifically engineered to bypass false-positive safety flags while still extracting valuable object data. The normal polling script will retrieve them automatically.

### 3. Manual Updates & Metadata Syncing

If you view the AI generated descriptions and decide to *hand-edit* them using a tool like ACDSee or Lightroom, the database will fall out of sync.

- **Sync Manual Updates (`python sync_manual_updates.py`)**: This script automatically scans your hard drive for *any* photo modified today. It checks its EXIF/IPTC metadata (via `exiftool`) for manual descriptions, and surgically patches `photo_descriptions.json` to match user overrides.

### 4. Extra Utility Scripts

- `cleanup_and_log_gcs.bat`: Run this batch file to execute `cleanup_gcs_images.py`. It deletes old batch images from your Google Cloud Storage bucket and logs the total storage used before and after to track space savings.
- `cleanup_canceled_jobs.py`: Run this manually to delete bloated Google Cloud Storage files (Manifest JSONLs, Inputs, Outputs) for jobs that were either accidentally cancelled or failed on Google's end. Saves massive storage costs.
- `estimate_costs.py`: Run this to calculate approximate lifetime API spending of the AI Generation batch processes based on Vertex Flash token cost.
- `embed_metadata_windows.py`: Automatically triggered by the retrieval script to embed the textual descriptions visually into your `.jpg`/`.heic` IPTC metadata using `exiftool.exe`.

## Use Cases

Why use this automated workflow instead of just opening an LLM and asking it to describe a photo?

*   **Cloud Searchability (Google Drive):** Because the AI descriptions are physically embedded into the IPTC metadata of your images, upsyncing your catalog to Google Drive makes the files natively searchable on the cloud. You can instantly search Google Drive for complex queries like "A red car parked near a snowy cabin" and retrieve the exact image file!
*   **Searchable Local Archives:** Desktop software (like ACDSee, Lightroom, or Windows Search) can also instantly read these exact same embedded descriptions across entirely unorganized local file dumps.
*   **Mass Digitization:** Safely labeling tens of thousands, or even millions, of scanned family photos, historical archives, or professional portfolios in a fraction of the time it would take a human. 
*   **High-Volume Cost Efficiency:** Utilizing the Vertex AI Batch Prediction API provides a massive 50% discount per-token over standard synchronous requests, which is crucial when describing an entire multi-terabyte photo directory at once.
*   **Safety Filter Bypass:** Automatically catching images that trigger aggressive AI safety filters (e.g., artistic photography) and re-routing them through a sterilized prompt pipeline to ensure no files are left blank or skipped in the final archive.

---

### Local Performance Tuning

These scripts have specifically been designed to execute locally on high-end consumer hardware by dynamically scaling based on available logical and physical cores.
- To prevent slow I/O bottlenecks when dealing with 150,000+ photo manifests, the scripts utilize aggressive multithreading (scaling `max_workers` based on `os.cpu_count() * 4`, up to 64-100 threads) to saturate available physical and logical CPU cores during GCS Uploads and ExifTool embeddings. 
- O(N) Array loops have been aggressively refactored into O(1) Dictionary Cache hits. This shifts the computation bottleneck from disk traversal into memory, safely utilizing large amounts of system RAM to load the entire database state simultaneously. For example, instead of searching the JSON list repeatedly:

```python
# O(1) Hash Optimization Pattern Used
successful_paths_cache: set[str] = set()
for entry in photo_data:
    path: str = entry.get('full_path', '')
    desc: Optional[str] = entry.get('description')
    if path and desc not in ['safety violation', '', None]:
        successful_paths_cache.add(path.lower())

# Later lookups map instantly to memory instead of iterating the 150,000+ array
if full_path.lower() in successful_paths_cache:
    print(f"⏭️ Skipping {full_path}")
```
