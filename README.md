# LinkedIn Contract Extractor Automation Bot

A robust, enterprise-grade automation tool designed to extract contract-related posts (W2, C2C, 1099) from LinkedIn. It features a two-phase extraction process, advanced session recovery, and stealth mechanisms to navigate modern obfuscated UIs.

## 🚀 Key Features

- **Workflow Orchestration**: Integrated with `run_workflow.py` for API-driven execution and scheduling.
- **Session Recovery**: Automatically detects and recovers from browser crashes or "Invalid Session ID" errors by restarting the driver and re-authenticating.
- **Two-Phase Extraction Engine**:
  - **Phase 1 (Collection)**: High-speed scraping and caching of relevant posts to local storage.
  - **Phase 2 (Processing & Sync)**: Parallel processing of cached posts for contact extraction (Email/Phone) and automated syncing to backend databases.
- **Obfuscated UI Handling**: Advanced selector logic in `selectors.json` and content-hashing fallbacks to identify posts even when standard class names are randomized.
- **Stealth & Evasion**: Utilizes `undetected-chromedriver` and `selenium-stealth` with human-like interactions and profile-based persistence.
- **Reporting**: Generates consolidated HTML reports summarizing Runs, Contacts Found, Contacts Synced, and Jobs Identified.

## 🏗 System Architecture

```
project-root/
├── modules/
│   ├── browser_manager.py   # Chrome lifecycle, session recovery, and stealth.
│   ├── scraper.py           # DOM interaction and raw post collection.
│   ├── processor.py         # AI-based relevance scoring and data cleaning.
│   ├── data_extractor.py    # Post-processing (Email/Phone extraction) & Sync.
│   ├── storage_manager.py   # Raw post caching and structured metadata storage.
│   └── bot_reporter.py      # HTML report generation and email distribution.
├── src/extractor/
│   ├── orchestration/       # Service layer for workflow execution.
│   ├── persistence/         # Database and candidate source adapters.
│   └── workflow/            # API client and manager for job tracking.
├── main.py                  # CLI Orchestrator for direct runs.
├── run_workflow.py          # Entry point for Scheduled/API runs.
├── config.py                # Configuration loader & constants.
├── selectors.json           # CENTRAL UI selector definitions.
└── keywords.json            # Target search queries.
```

## 🛠️ Setup & Installation

### Prerequisites
- Python 3.10+
- Google Chrome (Automatic version detection enabled).

### 1. Install Dependencies
```bash
pip install -r requirements.txt
```

### 2. Configure Environment
Copy the example file and fill in your details:
```bash
cp .env.example .env
```
Key Variables:
- `CHROME_PROFILE_PATH`: Path to your local Chrome profile storage.
- `CHROME_PROFILE_NAME`: Specific profile name for session persistence.
- `WBL_API_TOKEN`: Required for syncing data to the backend.

## 🏃 Usage

### Option A: Workflow Execution (Recommended)
Triggers the full extraction cycle for a specific candidate or all candidates via the orchestration layer.
```bash
python run_workflow.py --candidate-id <ID>
```

### Option B: Manual Orchestration
Runs the Phase 1 collection and Phase 2 extraction for credentials defined in `.env`.
```bash
python main.py
```

### Option C: Dry Run (Testing)
Set `DRY_RUN=True` in `.env` to simulate collection without making API syncs or database writes.

## 📊 Output & Metrics

Data is organized in the `data/` directory:
- `data/saved_posts_raw/`: Cached HTML and raw metadata for every post seen.
- `data/output/YYYY-MM-DD/`: 
  - `contacts_extracted.csv`: Valid contact details identified.
  - `jobs.json`: Classified job metadata for backend ingestion.
- `data/processed_posts/`: Log of processed URNs to prevent duplicate scraping.

## 📊 Summary Logic
The bot reports metrics at three levels:
1. **Scraper Level**: Posts seen vs. Relevant posts identified.
2. **Extraction Level**: Valid contacts parsed vs. Jobs classified.
3. **Sync Level**: New records inserted vs. Duplicates skipped.

## 🔒 Security & Stealth
- **Profile Persistence**: Uses real Chrome profiles to maintain LinkedIn sessions and avoid repeating multi-factor authentication (MFA).
- **Session Watchdog**: The `BrowserManager` monitors driver stability and self-heals without losing progress.
- **Hash-based ID**: When LinkedIn hides URNs, the bot uses cryptographic hashing of post content to ensure stable, unique tracking.

## 📝 Maintenance
Update `selectors.json` if LinkedIn undergoes a major structural change. The bot is designed to favor structural selectors (roles/data-attributes) over cosmetic class names.
