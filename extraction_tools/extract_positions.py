"""
Position Extractor - Extract job positions/titles from LinkedIn posts JSON files
"""
import json
import csv
import os
import re
from datetime import datetime
from pathlib import Path

# Common job title keywords
JOB_TITLE_KEYWORDS = [
    'engineer', 'developer', 'architect', 'manager', 'analyst', 'scientist',
    'consultant', 'lead', 'specialist', 'administrator', 'designer','AI Engineer','AI Developer','AI Architect','AI Manager','AI Analyst','AI Scientist','AI Consultant','AI Lead','AI Specialist','AI Administrator','AI Designer','AI Coordinator','AI Director','AI Officer','AI Technician','AI Programmer'
    'coordinator', 'director', 'officer', 'technician', 'programmer''ML Engineer','ML Developer','ML Architect','ML Manager','ML Analyst','ML Scientist','ML Consultant','ML Lead','ML Specialist','ML Administrator','ML Designer','ML Coordinator','ML Director','ML Officer','ML Technician','ML Programmer'
    'Data Engineer','Data Developer','Data Architect','Data Manager','Data Analyst','Data Scientist','Data Consultant','Data Lead','Data Specialist','Data Administrator','Data Designer','Data Coordinator','Data Director','Data Officer','Data Technician','Data Programmer'
    'MLops Engineer','MLops Developer','MLops Architect','MLops Manager','MLops Analyst','MLops Scientist','MLops Consultant','MLops Lead','MLops Specialist','MLops Administrator','MLops Designer','MLops Coordinator','MLops Director','MLops Officer','MLops Technician','MLops Programmer'
]

# Seniority levels
SENIORITY_LEVELS = [
    'senior', 'sr', 'junior', 'jr', 'lead', 'principal', 'staff',
    'chief', 'head', 'associate', 'entry level', 'mid level', 'mid-level'
]

# Position extraction patterns
POSITION_PATTERNS = [
    r'Position\s*[:\-]\s*([^\n\r]+?)(?:\n|\r|$)',
    r'Role\s*[:\-]\s*([^\n\r]+?)(?:\n|\r|$)',
    r'Hiring\s*[:\-]?\s*([^\n\r]+?)(?:\n|\r|$)',
    r'Job\s+Title\s*[:\-]\s*([^\n\r]+?)(?:\n|\r|$)',
    r'Title\s*[:\-]\s*([^\n\r]+?)(?:\n|\r|$)',
    r'(?:We are|We\'re|Looking for|Seeking)\s+(?:a|an)?\s*([A-Z][^\n\r]+?(?:Engineer|Developer|Architect|Manager|Analyst|Scientist|Consultant|Lead|Specialist))(?:\s|,|\.|$)',
]

def extract_positions_from_text(text_lines):
    """Extract job positions from post text."""
    if not text_lines:
        return []
    
    # Join lines into full text
    full_text = '\n'.join(text_lines)
    positions = []
    
    # Try pattern-based extraction first
    for pattern in POSITION_PATTERNS:
        matches = re.findall(pattern, full_text, re.IGNORECASE)
        for match in matches:
            cleaned = clean_position_text(match)
            if cleaned and is_valid_position(cleaned):
                positions.append(cleaned)
    
    # If no patterns matched, try to find positions in first few lines
    if not positions:
        for line in text_lines[:5]:  # Check first 5 lines
            line = line.strip()
            if line and is_valid_position(line):
                # Check if line contains job title keywords
                line_lower = line.lower()
                if any(keyword in line_lower for keyword in JOB_TITLE_KEYWORDS):
                    cleaned = clean_position_text(line)
                    if cleaned and len(cleaned) < 100:  # Reasonable length
                        positions.append(cleaned)
    
    # Remove duplicates while preserving order
    seen = set()
    unique_positions = []
    for pos in positions:
        pos_lower = pos.lower()
        if pos_lower not in seen:
            seen.add(pos_lower)
            unique_positions.append(pos)
    
    return unique_positions[:5]  # Return max 5 positions per post

def clean_position_text(text):
    """Clean and normalize position text."""
    if not text:
        return ""
    
    # Remove hashtags
    text = re.sub(r'#\w+', '', text)
    
    # Remove extra whitespace
    text = ' '.join(text.split())
    
    # Remove leading/trailing punctuation
    text = text.strip('.,;:!?-–—()[]{}')
    
    # Remove location patterns (City, State)
    text = re.sub(r'\s*[@\-–—]\s*[A-Z][a-z]+,?\s+[A-Z]{2}(?:\s|$)', '', text)
    
    # Remove common suffixes
    text = re.sub(r'\s*\(.*?\)\s*$', '', text)
    
    return text.strip()

def is_valid_position(text):
    """Check if text looks like a valid job position."""
    if not text or len(text) < 5:
        return False
    
    # Too long to be a position title
    if len(text) > 150:
        return False
    
    # Must contain at least one job keyword
    text_lower = text.lower()
    if not any(keyword in text_lower for keyword in JOB_TITLE_KEYWORDS):
        return False
    
    # Exclude common false positives
    exclude_patterns = [
        r'^\s*#',  # Starts with hashtag
        r'http[s]?://',  # Contains URL
        r'@\w+\.\w+',  # Contains email
        r'^\s*\d+\s*$',  # Just numbers
        r'^\s*[•\-\*]\s*$',  # Just bullet points
    ]
    
    for pattern in exclude_patterns:
        if re.search(pattern, text, re.IGNORECASE):
            return False
    
    return True

def process_json_file(json_path, output_csv):
    """Process a single JSON file and extract positions."""
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
    
    with open(output_csv, 'a', newline='', encoding='utf-8') as f:
        fieldnames = [
            'post_id', 'author_name', 'linkedin_id', 'extracted_positions',
            'post_snippet', 'extraction_date', 'search_keyword', 'source_file'
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
            
            # Extract positions
            positions = extract_positions_from_text(post_text)
            
            if positions:
                # Create snippet (first 100 chars of first line)
                snippet = post_text[0][:100] if post_text else ''
                
                writer.writerow({
                    'post_id': post_id,
                    'author_name': author_name,
                    'linkedin_id': linkedin_id,
                    'extracted_positions': ' | '.join(positions),
                    'post_snippet': snippet,
                    'extraction_date': extraction_date,
                    'search_keyword': search_keyword,
                    'source_file': Path(json_path).name
                })
                extracted_count += 1
    
    print(f" Extracted positions from {extracted_count} posts")
    return extracted_count

def main():
    """Main function to process all JSON files."""
    # Calculate default paths relative to this script
    script_dir = Path(__file__).parent.absolute()
    default_input = script_dir.parent / 'saved_posts'
    
    import argparse
    
    parser = argparse.ArgumentParser(description='Extract job positions from LinkedIn posts JSON')
    parser.add_argument('--input', default=str(default_input), help='Input JSON file or directory')
    parser.add_argument('--output', default=None, help='Output CSV file')
    args = parser.parse_args()
    
    # Determine output file
    if args.output:
        output_csv = args.output
    else:
        timestamp = datetime.now().strftime('%Y-%m-%d')
        output_csv = f'extracted_data/positions_{timestamp}.csv'
    
    print("=" * 60)
    print(" LinkedIn Position Extractor")
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
    
    print("\n" + "=" * 60)
    print(f" COMPLETE: Extracted positions from {total_extracted} posts")
    print(f" Output saved to: {output_csv}")
    print("=" * 60)

if __name__ == '__main__':
    main()
