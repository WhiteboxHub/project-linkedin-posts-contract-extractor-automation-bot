import json
import csv
import os
import re
from datetime import datetime
from pathlib import Path
import argparse
import sys
import os
# Ensure we can find the modules folder in the parent directory
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from modules.processor import ProcessorModule
def get_email_context(text_lines, email):
    """Get surrounding text context where email was found."""
    full_text = '\n'.join(text_lines)
    
    # Find the line containing the email
    for line in text_lines:
        if email.lower() in line.lower():
            # Return the line, trimmed to reasonable length
            context = line.strip()
            if len(context) > 150:
                # Find email position and get context around it
                email_pos = context.lower().find(email.lower())
                start = max(0, email_pos - 50)
                end = min(len(context), email_pos + len(email) + 50)
                context = '...' + context[start:end] + '...'
            return context
    
    # If not found in individual lines, search full text
    email_pos = full_text.lower().find(email.lower())
    if email_pos >= 0:
        start = max(0, email_pos - 50)
        end = min(len(full_text), email_pos + len(email) + 50)
        context = full_text[start:end].replace('\n', ' ')
        return '...' + context + '...'
    
    return ''

def extract_emails_from_text(text_lines):
    if not text_lines:
        return []
    
    full_text = '\n'.join(text_lines)
    processor = ProcessorModule()
    primary_email = processor.extract_email(full_text)
    emails = []
    if primary_email:
        emails.append(primary_email)
    email_pattern = r'\b[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Z|a-z]{2,}\b'
    all_matches = re.findall(email_pattern, full_text, re.IGNORECASE)
    
    # Filter and add unique emails
    for email in all_matches:
        email_lower = email.lower()
        
        # Skip if already in list
        if email_lower in [e.lower() for e in emails]:
            continue
        if processor.extract_email(email):
            emails.append(email)
    
    return emails

def process_json_file(json_path, output_csv):
    """Process a single JSON file and extract emails."""
    print(f"\n Processing: {json_path}")
    
    try:
        with open(json_path, 'r', encoding='utf-8') as f:
            posts = json.load(f)
    except Exception as e:
        print(f" Error reading JSON: {e}")
        return 0
    
    if not isinstance(posts, list):
        print(f" Invalid JSON format (expected array)")
        return 0
    
    print(f"   Found {len(posts)} posts")
    
    # Prepare output
    output_dir = Path(output_csv).parent
    output_dir.mkdir(parents=True, exist_ok=True)
    
    file_exists = os.path.exists(output_csv)
    
    extracted_count = 0
    email_count = 0
    
    with open(output_csv, 'a', newline='', encoding='utf-8') as f:
        fieldnames = [
            'email', 'post_id', 'author_name', 'linkedin_id',
            'context', 'extraction_date', 'search_keyword', 'source_file'
        ]
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        
        if not file_exists:
            writer.writeheader()
        
        for post in posts:
            post_id = post.get('post_id', 'N/A')
            author_name = post.get('author_name', '')
            linkedin_id = post.get('linkedin_id', '')
            post_text = post.get('post_text', [])
            extraction_date = post.get('extraction_date', '')
            search_keyword = post.get('search_keyword', '')
            
            # Extract emails
            emails = extract_emails_from_text(post_text)
            
            if emails:
                extracted_count += 1
                
                for email in emails:
                    context = get_email_context(post_text, email)
                    
                    writer.writerow({
                        'email': email,
                        'post_id': post_id,
                        'author_name': author_name,
                        'linkedin_id': linkedin_id,
                        'context': context,
                        'extraction_date': extraction_date,
                        'search_keyword': search_keyword,
                        'source_file': Path(json_path).name
                    })
                    email_count += 1
    
    print(f" Extracted {email_count} email(s) from {extracted_count} posts")
    return email_count

def main():
    # Calculate default paths relative to this script
    script_dir = Path(__file__).parent.absolute()
    default_input = script_dir.parent / 'saved_posts'
    
    parser = argparse.ArgumentParser(description='Extract emails from LinkedIn posts JSON')
    parser.add_argument('--input', default=str(default_input), help='Input JSON file or directory')
    parser.add_argument('--output', default=None, help='Output CSV file')
    parser.add_argument('--deduplicate', action='store_true', help='Remove duplicate emails')
    args = parser.parse_args()
    
    # Determine output file
    if args.output:
        output_csv = args.output
    else:
        timestamp = datetime.now().strftime('%Y-%m-%d')
        output_csv = f'extracted_data/emails_{timestamp}.csv'
    
    print("=" * 60)
    print(" LinkedIn Email Extractor")
    print("=" * 60)
    
    total_extracted = 0
    
    # Process input
    input_path = Path(args.input)
    
    if input_path.is_file():
        # Single file
        total_extracted = process_json_file(str(input_path), output_csv)
    elif input_path.is_dir():
        # Directory - process all JSON files
        json_files = list(input_path.glob('*.json'))
        
        if not json_files:
            print(f" No JSON files found in {input_path}")
            return
        
        print(f"\n Found {len(json_files)} JSON file(s)")
        
        for json_file in json_files:
            count = process_json_file(str(json_file), output_csv)
            total_extracted += count
    else:
        print(f" Invalid input path: {input_path}")
        return
    
    # Optional deduplication
    if args.deduplicate and total_extracted > 0:
        print(f"\n Deduplicating emails...")
        deduplicate_csv(output_csv)
    
    print("\n" + "=" * 60)
    print(f" COMPLETE: Extracted {total_extracted} email(s)")
    print(f" Output saved to: {output_csv}")
    print("=" * 60)

def deduplicate_csv(csv_path):
    """Remove duplicate emails from CSV, keeping first occurrence."""
    try:
        # Read all rows
        with open(csv_path, 'r', encoding='utf-8') as f:
            reader = csv.DictReader(f)
            rows = list(reader)
            fieldnames = reader.fieldnames
        
        # Track seen emails
        seen_emails = set()
        unique_rows = []
        
        for row in rows:
            email_lower = row['email'].lower()
            if email_lower not in seen_emails:
                seen_emails.add(email_lower)
                unique_rows.append(row)
        
        # Write back
        with open(csv_path, 'w', newline='', encoding='utf-8') as f:
            writer = csv.DictWriter(f, fieldnames=fieldnames)
            writer.writeheader()
            writer.writerows(unique_rows)
        
        duplicates_removed = len(rows) - len(unique_rows)
        if duplicates_removed > 0:
            print(f"   Removed {duplicates_removed} duplicate(s)")
            print(f"   {len(unique_rows)} unique email(s) remaining")
    
    except Exception as e:
        print(f" Error during deduplication: {e}")

if __name__ == '__main__':
    main()
