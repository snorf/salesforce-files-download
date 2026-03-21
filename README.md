# salesforce-files-download

Python tool to download Salesforce Files (ContentDocument/ContentVersion). Originally a simple download script, now extended with deployment capabilities for org-to-org file migration.

## Features

- **Download files** from Salesforce via ContentDocumentLink or ContentDocument queries
- **Deploy/upload files** to a target Salesforce org with relationship reconstruction
- **Checksum verification** — automatically skips files already on disk with matching MD5, saving bandwidth on re-runs
- **Progress bar** — Rich-based live progress with file count, percentage, elapsed and estimated time
- **Run statistics** — summary after each run showing succeeded, failed, skipped counts, duration and throughput
- **Batch mode** — process multiple objects from a CSV mapping file
- **CLI mode** — run a single SOQL query with custom filters
- **Threaded downloads** — uses ThreadPoolExecutor for parallel file downloads
- **Filename collision handling** — deterministic `_1`, `_2` suffixes for duplicate titles within a run
- **Cross-platform filenames** — sanitizes titles for both Windows and Unix
- **Configurable filename patterns** — placeholders for output dir, document ID, title, extension, linked entity name, version number
- **File extension filtering** — download only specific file types (e.g. PDF, Excel)
- **CSV mapping output** — detailed mapping file for each run with ContentVersion IDs, linked entity info, owner, and Case metadata
- **Special .snote handling** — converts Salesforce Notes to HTML during deploy
- **Owner mapping** — maps file owners between orgs via User.AboutMe field
- **Pre-flight checks** — validates config files and output directories before starting

## Quick Start

```bash
# Clone and set up
git clone <repo-url>
cd salesforce-files-download
python3 -m venv .venv
source .venv/bin/activate  # Windows: .venv\Scripts\activate
pip install -r requirements.txt

# Configure
cp config.ini.sample config.ini
# Edit config.ini with your Salesforce credentials
```

## Configuration

### config.ini

```ini
[salesforce]
source_username = user@example.com
source_password = yourpassword
source_security_token = yourtoken
source_domain = mydomain          # custom domain (without .my.salesforce.com)
source_connect_to_sandbox = False

# Target org (only needed for deploy)
target_username = user@target.com
target_password = yourpassword
target_security_token = yourtoken
target_domain = targetdomain
target_connect_to_sandbox = False

output_dir = exported_files/
output_dir_auto_delete = false
deployment_dir = deployment_files/

batch_size = 200
loglevel = INFO

# Default filename pattern (can be overridden via -f flag)
default_filename_pattern = {0}{1}-{2}.{3}

# Default file extension filter (can be overridden via -fe flag)
default_file_extension_filter =
```

### object_mapping.csv (batch mode)

```csv
Source Org Object,Target Org Object
Account,Account
Case,Case
```

## Usage

### Download — Batch Mode

Processes all objects from `object_mapping.csv`:

```bash
python download.py
```

### Download — CLI Mode

Run a specific SOQL query:

```bash
# Basic download
python download.py --mode cli -q "SELECT Id FROM Account WHERE Industry='Tech'" -so Account

# Filter by file type (PDF only)
python download.py --mode cli -q "SELECT Id FROM Case" -so Case -fe "'PDF'"

# Custom filename pattern (title only, no document ID)
python download.py --mode cli -q "SELECT Id FROM Account" -so Account -f "{0}{2}.{3}"

# Download and immediately deploy
python download.py --mode cli -q "SELECT Id FROM Case LIMIT 10" -so Case --deploy
```

### Deploy

Upload downloaded files to the target org:

```bash
python deploy.py
```

### Filename Pattern Placeholders

| Placeholder | Value               |
| ----------- | ------------------- |
| `{0}`       | Output directory    |
| `{1}`       | ContentDocument ID  |
| `{2}`       | Title (sanitized)   |
| `{3}`       | File extension      |
| `{4}`       | Linked entity name  |
| `{5}`       | Version number      |

Default: `{0}{1}-{2}.{3}` (e.g. `output/069xx-MyFile.pdf`)

## How It Works

### Download Phase

1. Queries ContentDocumentLinks for the specified object
2. Fetches ContentVersion records (with checksum) in batches
3. Skips files already on disk with matching MD5 checksum
4. Downloads new/changed files via ThreadPoolExecutor
5. Writes a `files.csv` mapping file with all records (including skipped)

### Deploy Phase

1. Reads the CSV mapping files from download
2. Checks for duplicates in target org via `SI_Old_Id__c`
3. Uploads files as ContentVersion records (base64 encoded)
4. Reconstructs ContentDocumentLinks to parent records
5. Reports success/failure summary

### Re-running

The tool is safe to re-run:
- Files with matching checksums are skipped (no re-download)
- The CSV mapping is regenerated fresh each time
- Deploy checks `SI_Old_Id__c` to avoid duplicate uploads

## File Structure

```
salesforce-files-download/
├── download.py              # Orchestrator: batch mode + CLI mode
├── download_functions.py    # Core download logic
├── deploy.py                # Orchestrator: deploy phase
├── deploy_functions.py      # Core deploy logic
├── filename_utils.py        # Cross-platform filename sanitization
├── reporting.py             # Deploy summary tracking
├── config.ini.sample        # Configuration template
├── object_mapping.csv       # Source-to-target object mapping
└── requirements.txt         # Python dependencies
```

## Log Files

- `download.log` — download orchestrator log
- `download_functions.log` — detailed download/checksum log
- `deploy.log` — deploy orchestrator log
- `deploy_summary.json` — structured deploy results

## Prerequisites

- Python 3.7+
- Salesforce user with permissions to read/write ContentDocument, ContentVersion, ContentDocumentLink
- For deploy: parent records must exist in target org with `SI_Old_Id__c` populated
