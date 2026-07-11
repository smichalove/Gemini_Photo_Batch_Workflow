# Gemini Photo Batch Workflow - Agent Rules & Guidelines

> [!IMPORTANT]
> This document serves as the official instruction manual for any AI agent or developer contributing to this repository. Always adhere to these guidelines before writing, modifying, or executing code.

---

## 1. Project Configuration & Parameters

Ensure that all project configuration variables are properly set in the script constants or environment variables. Do not hardcode private credentials.

* **GCP Project ID**: Must point to the active Google Cloud Project.
* **GCP Location**: Defaults to `global` or target regional endpoint.
* **Gemini Model**: Configured to the desired Gemini version (e.g., `gemini-2.5-flash`).
* **GCS Storage Bucket**: The GCS bucket designated to hold temporary batch images.
* **Image Directories**: Local paths to scan for unprocessed images (e.g. standard user folders or dedicated media drives).

---

## 2. Batch Job Lifecycle & File Mappings

* **`photo_descriptions.json`**: The master database storing path-to-description mapping records.
* **`submitted_photos_cache.txt`**: Logs relative paths of images that have been submitted to Vertex AI but are still in flight, preventing double-submission in subsequent runs.
* **`embedded_photos_cache.txt`**: Cache of image paths that have had metadata physically written to them on disk by ExifTool, preventing redundant writes.
* **`api_cost_tracker.json`**: Tracks token metrics and exact billing data compiled from completed jobs.
* **`batch_job_[UUID].json` / `batch_job_retry_[UUID].json`**: Local tracking files used by the retrieval and cleanup scripts to poll Vertex AI for state updates. Deleted automatically upon job completion, failure, or cancellation.
* **`batch_requests_[UUID].jsonl` / `retry_requests_[UUID].jsonl`**: Local JSONL manifests containing image GCS links and prompts, which are uploaded to the `manifests/` directory in the storage bucket before triggering the API.

---

## 3. Coding, Readability, & Dependency Standards

All Python and shell code must strictly follow the standards below to ensure build determinism, security compliance, and readability:

### Strict Type Hinting
* Every function, method, and class attribute must be fully type-annotated.
* Avoid using `Any`. Be as specific as possible (e.g., use `List[str]`, `Dict[str, int]`, `Optional[float]`, etc.).

### Mandatory Docstrings
* **Module/File Headers**: Every `.py` file must contain a fully descriptive, architectural top-level module docstring at the very beginning of the file. This docstring must detail the file's role in the overall architecture, its primary responsibilities, interfaces, dependencies, and how it fits into the project pipeline, ensuring other agents and developers can easily understand its context.
* **Classes**: Every class must include a docstring explaining its purpose, state, and key responsibilities.
* **Functions and Methods**: Every function/method must contain a comprehensive docstring that details:
  * The behavior and purpose of the function.
  * **Args**: Clearly listed arguments with their expected types and descriptions.
  * **Returns**: The return type and description of the output.
  * **Raises**: Any exceptions that could be raised by the function.
  
  Example:
  ```python
  def read_serial_data(port: str, timeout: float) -> str:
      """Reads raw XML telemetry from a serial port.

      Args:
          port: The filesystem path to the serial device (e.g., '/dev/ttyUSB0').
          timeout: The read timeout in seconds.

      Returns:
          A string containing the raw payload received.

      Raises:
          SerialException: If the serial interface cannot be accessed.
      """
  ```

### Descriptive Inline Comments
* Write descriptive, inline comments for non-trivial logic blocks.
* Comments should explain the *why* behind the implementation decisions, not just *what* the code does, to enable other developers and users to quickly understand and debug the codebase.

### Prompt Management Standards
* **External Prompt Files**: LLM prompts shall always be drafted in separate external `.txt` files rather than hardcoded in the codebase. The application must load and refresh the prompt template from the file on disk dynamically at runtime (e.g., right before invoking the model).
* **Show Prompt Returns**: Whenever the agent or application runs a prompt (either through emulation, test scripts, or local executions), the agent must explicitly print and show the raw prompt return/response text to the user.

### Command Execution & Shell Portability
* **Command Explanations**: For every command executed or proposed on the terminal, the agent must provide a quick, one-line explanation of what the command does and why it is being executed.
* **Script Path Portability (Absolute / Resolved Paths)**: When writing or modifying shell scripts (`.sh` or `.bat`), never use un-anchored relative paths (like `./`) for file interactions or script executions. Always dynamically resolve the script's directory (using `SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"`) or use home directory references (like `~/`) to ensure the script operates correctly regardless of the caller's working directory.

### Dependency Pinned Requirements
* **Strict Dependency Pinning**: All Python dependencies listed in `requirements.txt` must be strictly pinned to exact versions (using `==` instead of loose range operators like `>=` or unpinned packages) to ensure build determinism and prevent dependency drift.

---

## 4. Workload-Specific Prompt Customization

To ensure flexibility when moving between different photo cataloging runs, prompts must be dynamically loaded from external files and customized for the target media assets.

### Dynamic Prompt Loading Code Pattern
Rather than hardcoding string literals, implement a self-healing template reader:
```python
def load_prompt_template(filename: str, default_text: str) -> str:
    """Reads a prompt template file from the workspace.
    
    Args:
        filename: The base name of the text file (e.g., 'system_prompt.txt').
        default_text: Fallback string if the file is missing.
        
    Returns:
        The loaded prompt string stripped of leading/trailing whitespace.
    """
    path: str = os.path.join(PROJECT_DIR, "prompts", filename)
    if os.path.exists(path):
        with open(path, "r", encoding="utf-8") as f:
            return f.read().strip()
    return default_text
```

### Workload-Specific Customization Guidelines
When setting up a batch run, configure the external prompt files (`system_prompt.txt` / `user_prompt.txt`) to focus on the key variables needed for the workload:

1. **Standard Scenic / Landscape Curation**:
   * *Focus*: Geological features, weather, lighting conditions, time of day (e.g., golden hour, twilight), foliage, water clarity, and overall atmospheric mood.
2. **Portrait / Family Album Archiving**:
   * *Focus*: Number of subjects, demographics, attire descriptions, physical poses, emotional expressions, primary interactions, framing details (e.g., close-up, wide shot), and background details.
3. **Document Scan / Archival Transcription**:
   * *Focus*: Perform OCR on text headers, transcribe visible dates or stamps, note paper/medium degradation features (e.g., sepia, tears, binding details), and catalog structural document layouts.
4. **Product / Inventory Cataloging**:
   * *Focus*: Visible branding, packaging details, colors, texture descriptions, barcodes, product model IDs, camera angles, and scale reference objects.

---

## 5. Threading & Concurrency Worker Constraints

To maximize throughput and saturate system bandwidth and CPU cores without hitting network or API rate limits, enforce the following thread pool limits:
* **GCS Image Uploads**:
  * Set worker limits (typically up to **100 threads** or scaled to `min(64, os.cpu_count() * 4)`) to accelerate concurrent networking.
* **GCS Deletion Cleanup**:
  * Clamped between **20 and 32 threads** to quickly clear temporary directories and GCS output chunks.
* **ExifTool Metadata Extraction**:
  * Clamped around **32 threads** for directory traversal scans.
* **ExifTool Metadata Writing (Embedding)**:
  * Clamped to `min(32, os.cpu_count() * 4)` threads to optimize write performance on high-speed drives.

---

## 6. ExifTool Metadata Embedding Rules

When invoking the `exiftool` executable via `subprocess.run`, always use parameter list argument parsing and respect these configurations:
* **IPTC/EXIF Targets**:
  * Descriptions must be written to three distinct metadata tags: `-Caption-Abstract`, `-Description`, and `-ImageDescription`.
* **Character Encoding**:
  * Command must include `-charset iptc=UTF8` and `-charset UTF8` to ensure international characters are written cleanly.
* **Backup Prevention**:
  * Use `-overwrite_original` to update the image files in-place on disk and prevent creating backup duplicates.
* **Error Workaround & Recovery Procedures**:
  * **Temporary file already exists**: If ExifTool throws this error, locate the `_exiftool_tmp` file, delete it using `os.remove()`, and immediately retry the command.
  * **Bad Photoshop IRB resource**: If ExifTool fails with this error, insert `-Photoshop:All=` into the command arguments list to clear corrupt IRB resource blocks and retry.
  * **Format errors / Extension Mismatches**: If ExifTool reports corrupt files or extension mismatches (e.g. `.HEIC` files that are actually JPEGs), skip processing the file and add it to the cache to prevent infinite retries.

---

## 7. Safety Violation Filter Routing

Artistic photography or context-heavy images might be falsely flagged as `safety violation` by cloud safety filters.
* **Neutral Prompt Fallback**:
  * Images marked as `safety violation` in the database must be re-routed with the safety settings configured to `BLOCK_NONE`.
  * The normal creative prompt must be swapped out for a sanitized, purely objective prompt:
    `"Provide a neutral, factual description of the visual elements in this image. Focus on objects, colors, and layout."`
* **Deduplication Check**:
  * Verify if the image was already successfully retried on another run by checking the path cache. If it was, skip submitting to GCS.

---

## 8. O(1) Memory Performance Optimization Pattern

When processing databases containing large volumes of files, do not perform nested loops or O(N) array traverses. Load the database into memory and build lookup indexes using Python's hashed `set` structure:

```python
# PRE-COMPUTE: Build successful paths set in memory (O(1) matching time)
successful_paths: Set[str] = set()
for entry in data_store:
    path: str = entry.get('full_path', '')
    desc: Optional[str] = entry.get('description')
    if path and desc not in ['safety violation', '', None]:
        successful_paths.add(path.lower())

# LOOKUP: Query instantly inside loop traverses
if full_path.lower() in successful_paths:
    # Skip already completed file
    continue
```

---

## 9. Vertex AI Batch Cost Math Reference

API costs are calculated based on Vertex AI Batch Prediction pricing (which includes a flat **50% discount** compared to synchronous calls):
* **Input Token Math**:
  * Fixed image cost: **258 tokens** per image.
  * Prompt text overhead: **~40 tokens** per request.
* **Pricing Rates**:
  * Input pricing is based on the discounted rate (50% off standard rates).
  * Output pricing is based on the discounted rate (50% off standard rates).
* **Cost Estimation Formula**:
  $$\text{Cost (USD)} = \left(\frac{\text{Input Tokens}}{1,000,000} \times \text{Discounted Input Rate}\right) + \left(\frac{\text{Output Tokens}}{1,000,000} \times \text{Discounted Output Rate}\right)$$

---

## 10. Post-Mortem & Incident Logs

*Document all code regressions, logic bugs, database schema changes, and network connection issues here.*

### (Log format: Date - Failure, Root Cause, Resolution details)
*No logs have been recorded in this repository yet.*
