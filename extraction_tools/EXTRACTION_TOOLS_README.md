# LinkedIn Post Extraction Tools

This directory contains two powerful extraction scripts that parse your LinkedIn posts JSON files.

##  Available Scripts

### 1. **Position Extractor** (`extract_positions.py`)
Extracts job titles and positions from LinkedIn posts.

**Usage (from project root):**
```bash
# Process all JSON files in saved_posts directory
python extraction_tools/extract_positions.py

# Process a specific JSON file
python extraction_tools/extract_positions.py --input saved_posts\ML_and_w2_posts.json

# Specify custom output file
python extraction_tools/extract_positions.py --output my_positions.csv
```

**Output CSV Columns:**
- `post_id` - Unique post identifier
- `author_name` - Name of person who posted
- `linkedin_id` - LinkedIn profile URL
- `extracted_positions` - Job titles found (separated by |)
- `post_snippet` - First 100 characters of post
- `extraction_date` - When post was originally extracted
- `search_keyword` - Search keyword used
- `source_file` - Source JSON filename

**Examples of Extracted Positions:**
- "MLOps + AI Engineer"
- "Senior Java Developer"
- "Data Scientist"
- "Sr. Dynamics 365 Lead / Architect"
- "Full Stack Developer"

---

### 2. **Email Extractor** (`extract_emails.py`)
Extracts email addresses from LinkedIn posts with context.

**Usage (from project root):**
```bash
# Process all JSON files in saved_posts directory
python extraction_tools/extract_emails.py

# Process with deduplication (removes duplicate emails)
python extraction_tools/extract_emails.py --deduplicate

# Process a specific JSON file
python extraction_tools/extract_emails.py --input saved_posts\ML_and_w2_posts.json --output my_emails.csv
```

**Output CSV Columns:**
- `email` - Extracted email address
- `post_id` - Unique post identifier
- `author_name` - Name of person who posted
- `linkedin_id` - LinkedIn profile URL
- `context` - Surrounding text where email was found
- `extraction_date` - When post was originally extracted
- `search_keyword` - Search keyword used
- `source_file` - Source JSON filename

**Email Validation:**
- Automatically filters out invalid emails (image files, placeholders)
- Excludes generic email providers (gmail, yahoo, hotmail, etc.)
- Uses the same validation logic as the main bot
- **Author Name Recovery**: If a post author is missing or "Feed post", it recovers the name from the email prefix (e.g., `harsh.kushwah@...` → `Harsh Kushwah`).

---

##  Output Location

By default, extracted data is saved to `extraction_tools/extracted_data/`. Files are named by date:
```
extracted_data/
├── positions_2026-02-06.csv
└── emails_2026-02-06.csv
```
Files automatically combine data from all processed JSONs for that day.

---

##  Command-Line Options

### Common Options (Both Scripts)

| Option | Description | Default |
|--------|-------------|---------|
| `--input` | Input JSON file or directory | `saved_posts` |
| `--output` | Output CSV filename | Auto-generated with timestamp |

### Email Extractor Only

| Option | Description |
|--------|-------------|
| `--deduplicate` | Remove duplicate emails (keeps first occurrence) |

---

##  Examples

### Extract positions from all posts
```bash
python extraction_tools/extract_positions.py
```

### Extract emails with deduplication
```bash
python extraction_tools/extract_emails.py --deduplicate
```

### Process specific file with custom output
```bash
python extraction_tools/extract_positions.py --input saved_posts\ML_and_w2_posts.json --output tech_positions.csv
python extraction_tools/extract_emails.py --input saved_posts\ML_and_w2_posts.json --output tech_emails.csv
```

---

##  Sample Results

From the test run on `ML_and_w2_posts.json`:

- **Positions Extracted:** 58 posts with job titles
- **Emails Extracted:** 43 unique email addresses
- **Processing Time:** ~2-3 seconds per file

---

##  Technical Details

### Position Extraction Logic
- Pattern matching for common job posting formats
- Keyword detection (engineer, developer, architect, etc.)
- Seniority level recognition (senior, lead, principal, etc.)
- Automatic cleaning and normalization
- False positive filtering

### Email Extraction Logic
- Reuses existing `ProcessorModule.extract_email()` method
- Context capture (shows surrounding text)
- Multi-email support (finds all emails in a post)
- Automatic validation and filtering

---

##  Notes

- Scripts are standalone and don't modify the original JSON files
- **Smart Path Resolution**: Scripts automatically find the `saved_posts` folder regardless of if you run them from the root or from within the `extraction_tools/` directory.
- Can be run multiple times safely (outputs are dated)
- Works with existing JSON files retroactively
- Uses core bot validation logic for consistency

---

##  Quick Start

```bash
# 1. Extract positions
python extraction_tools/extract_positions.py

# 2. Extract emails (with deduplication)
python extraction_tools/extract_emails.py --deduplicate

# 3. Check the results
ls extraction_tools/extracted_data/
```

Your extracted data is now ready for analysis! 
