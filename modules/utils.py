import re

def clean_html(raw_html):
    """
    Remove HTML tags and clean up whitespace from a string.
    
    Args:
        raw_html (str): The input string containing HTML or dirty text.
        
    Returns:
        str: Cleaned plain text.
    """
    if not raw_html:
        return ""
    
    # 1. Remove HTML tags
    cleanr = re.compile('<.*?>')
    cleantext = re.sub(cleanr, ' ', raw_html)
    
    # 2. Replace common HTML entities (basic ones)
    cleantext = cleantext.replace('&nbsp;', ' ').replace('&amp;', '&').replace('&gt;', '>').replace('&lt;', '<')
    
    # 3. Collapse multiple spaces and newlines
    cleantext = re.sub(r'[ \t]+', ' ', cleantext) # Collapse spaces
    cleantext = re.sub(r'\n\s*\n', '\n\n', cleantext) # Max 2 newlines
    
    return cleantext.strip()

def clean_post_content(text):
    """
    Specific cleaner for LinkedIn post content.
    Removes "…more", hashtags, and common UI noise.
    """
    if not text: 
        return ""
        
    # Strip HTML first just in case
    text = clean_html(text)
    
    # Remove "…more" or "...more" often found at the end
    text = re.sub(r'\.\.\.?more$', '', text, flags=re.IGNORECASE).strip()
    
    # Filter out common UI artifacts lines
    lines = text.split('\n')
    cleaned_lines = []
    for line in lines:
        stripped = line.strip()
        # Common buttons/labels in the text stream
        if stripped in ["Like", "Comment", "Share", "Send", "Like Comment Share"]:
            continue
        if stripped.isdigit() and len(stripped) < 4: # low stray numbers usually counters
            continue
        cleaned_lines.append(line)
        
    text = "\n".join(cleaned_lines)
    
    # Normalize excessive newlines again
    text = re.sub(r'\n{3,}', '\n\n', text)
    
    return text.strip()
