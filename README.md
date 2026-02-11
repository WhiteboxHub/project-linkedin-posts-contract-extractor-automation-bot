# LinkedIn Contract Extractor Automation Bot

A robust, modular, and stealthy automation tool designed to extract contract-related posts (W2, C2C, 1099) from LinkedIn, specifically targeting AI/Tech roles. It filters posts for relevance, extracts author contact details (Email/Phone), and automates data entry into a local database and CSV reports.

## 🚀 Key Features

- **Job Scheduler**: Integrated polling scheduler that fetches jobs from the backend and executes them automatically.
- **Dynamic Keyword Assignment**: 
  - **Centralized List**: All keywords are managed in `keywords.json`.
  - **Round-Robin**: Keywords are assigned one-by-one to candidates in a rotating fashion, ensuring equal coverage.
- **Advanced Search & Filtering**: Supports complex Boolean strings (e.g., `"AI AND Engineer AND W2"`) with strict date filtering (`past-24h`, `week`) and sorting (`date_posted`).
- **Stealth Mode**: Integrates `undetected-chromedriver` and `selenium-stealth` with human-like scrolling and mouse movements to evade detection.
- **Modular Architecture**: Clean separation of concerns (`Scraper`, `Processor`, `Storage`, `Browser`) for maintainability.
- **Smart Extraction**: 
  - Extracts Name, Email, Phone, Company, Location, and Post URL.
  - Dedupes contacts (caches profiles during run).
  - Validates post relevance (AI/Tech keywords).
- **Data Persistence**:
  - **CSV**: Daily structured files (`output/YYYY-MM-DD/posts.csv`, `emails.csv`).
  - **Database**: DuckDB integration (`linkedin_data.db`).
- **Metrics & Reporting**: Detailed end-of-run summary with success rates, skip reasons, and retry statistics.

## 🏗 System Architecture

The project follows a modular design pattern:

```
project-root/
├── Modules
│   ├── browser_manager.py   # Handles Chrome lifecycle, profiles, proxy, and stealth.
│   ├── scraper_module.py    # DOM interaction, navigation, and raw data extraction.
│   ├── processor_module.py  # Data cleaning, regex extraction (Email/Phone), and relevance scoring.
│   ├── storage_manager.py   # Manages CSV/DB I/O, file structure, and deduplication.
│   ├── metrics_manager.py   # Centralized tracking of counters, skips, and failures.
│   ├── scheduler.py         # Polling worker for backend job execution.
│   └── logger.py            # Structured JSON logging for audit trails.
├── main.py                  # Main Orchestrator. Coordinates the flow between modules.
├── scheduler_main.py        # Entry point for the Job Scheduler.
├── config.py                # Configuration loader (Env vars, Selectors, Constants).
├── keywords.json            # CENTRAL List of target search queries.
├── candidates.json          # List of candidate credentials (without keywords).
├── job_activity_logger.py   # Integration with external backend (WBL).
└── setup_production.py      # Onboarding script for new users.
```

## 🛠️ Setup & Installation

### Prerequisites
- Python 3.10+
- Google Chrome installed.

### 1. Install Dependencies
```bash
pip install -r requirements.txt
```

### 2. Configure Environment
Copy the example file and fill in your details:
```bash
cp .env.example .env
```
Edit `.env` with your credentials:
```ini
LINKEDIN_EMAIL=your_email@example.com
LINKEDIN_PASSWORD=your_password
# Optional Proxy
PROXY_URL=http://user:pass@host:port
# Chrome Profile (Recommended for session persistence)
CHROME_PROFILE_PATH=/Users/your_user/Library/Application Support/Google/Chrome
CHROME_PROFILE_NAME=Default
```

### 3. Setup Candidates
Use the setup script to add valid LinkedIn credentials. 
**Note:** You will NO LONGER be asked for keywords here.
```bash
python setup_production.py
```
Select Option 2 to manage `candidates.json`.

### 4. Configure Keywords
Edit `keywords.json` in the root directory. This is the **single source of truth** for all searches.
```json
[
    "AI AND W2",
    "GenAI AND Contract",
    "Machine Learning AND C2C"
]
```

## 🏃 Usage

### Option A: Run Scheduler (Recommended)
Starts the polling worker. It will automatically pick up jobs and assign keywords round-robin to your candidates.
```bash
python scheduler_main.py
```

### Option B: Manual Run
Runs the bot once for all configured candidates in `candidates.json`, assigning keywords sequentially.
```bash
python main.py
```

### Option C: Dry Run (Testing)
Set `DRY_RUN=True` in `.env` or `config.py` to simulate actions without saving data:
```bash
python main.py
```

## 📊 Output & Metrics

### Files
Data is organized by date in the `output/` directory:
- `output/202X-XX-XX/posts.csv`: Full post metadata.
- `output/202X-XX-XX/emails.csv`: Extracted valid contacts.

### Execution Summary
At the end of every run, you will see a report:
```text
==================================================
           EXECUTION SUMMARY REPORT           
==================================================
Duration:        0:12:45
Total Seen:      150
Successfully Extracted: 42
Skipped:         108
...
FAILURE BREAKDOWN:
  - Stale Element: 2
==================================================
```

## 🔒 Security
- **Credentials**: Never stored in code. utilize `.env` or `candidates.json` (local only).
- **Stealth**: 
  - `selenium-stealth` masks WebDriver properties.
  - `BrowserManager` implements random mouse movements and variable scrolling.
  - Proxies supported via `undetected-chromedriver`.

## 📝 Developer Notes
- **Selectors**: Defined in `config.py` and `selectors.json`. Update `selectors.json` if LinkedIn UI changes.
- **Logging**: Logs are structured JSON format for easy ingestion into monitoring tools (Splunk/ELK).
