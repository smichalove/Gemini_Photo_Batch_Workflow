# Gemini Photo Batch Workflow

Automated workflow scripts for checking local photos, uploading to Google Cloud Storage, describing them using Vertex AI Gemini Batch API, and embedding the results back into local files.

## Initial Setup

1. Install Python 3.
2. Initialize the repo using `init_git_repo.bat`.
3. Set up authentication: Create an `auth/` directory and place your `service_account.json` key inside. See `auth/README.md` for details. 
4. External tool: Create a folder `exiftool/` and place `exiftool.exe` inside it.
5. Setup your python environment and packages with `pip install -r requirements.txt`. (Optionally run `venv-photos\Scripts\activate`).

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

- `cleanup_canceled_jobs.py`: Run this manually to delete bloated Google Cloud Storage files (Manifest JSONLs, Inputs, Outputs) for jobs that were either accidentally cancelled or failed on Google's end. Saves massive storage costs.
- `estimate_costs.py`: Run this to calculate approximate lifetime API spending of the AI Generation batch processes based on Vertex Flash token cost.
- `embed_metadata_windows.py`: Automatically triggered by the retrieval script to embed the textual descriptions visually into your `.jpg`/`.heic` IPTC metadata using `exiftool.exe`.

## Use Cases

Why use this automated workflow instead of just opening an LLM and asking it to describe a photo?

*   **Mass Digitization:** Safely labeling tens of thousands, or even millions, of scanned family photos, historical archives, or professional portfolios in a fraction of the time it would take a human. 
*   **Searchable Archives:** Embedding rich, factual AI-generated descriptions directly into the `IPTC` metadata of image files so that desktop software (like ACDSee, Lightroom, or Windows Search) can instantly find "A red car parked near a snowy cabin" across entirely unorganized file dumps.
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
