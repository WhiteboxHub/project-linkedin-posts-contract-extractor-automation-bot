import re
import config

class ProcessorModule:
    @staticmethod
    def extract_email(text):
        """
        Extract ALL valid-looking emails using a broad regex.
        No more invalid_pattern filtering based on domain names (as requested).
        """
        if not text:
            return None
            
        # Broad pattern to capture almost any email
        # We still exclude image extensions to avoid false positives like 'image.png'
        email_pattern = r'[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}'
        
        image_extensions = {'.png', '.jpg', '.jpeg', '.gif', '.svg', '.webp'}
        
        emails = re.findall(email_pattern, text)
        valid_emails = []
        
        for email in emails:
            # Basic sanity check: length and structure
            if len(email) < 5 or len(email) > 100:
                continue
            
            # Filter out image filenames that look like emails
            is_image = False
            for ext in image_extensions:
                if email.lower().endswith(ext):
                    is_image = True
                    break
            
            if is_image:
                continue
                
            # Filter out common personal email domains
            personal_domains = {'gmail.com', 'yahoo.com', 'hotmail.com', 'outlook.com', 'icloud.com', 'aol.com', 'protonmail.com'}
            is_personal = False
            for domain in personal_domains:
                if email.lower().endswith(domain):
                    is_personal = True
                    break
            
            if is_personal:
                continue
                
            # Filter out own email if defined
            if config.LINKEDIN_EMAIL and email.lower().strip() == config.LINKEDIN_EMAIL.lower().strip():
                continue
                
            valid_emails.append(email)
            
        # Return unique list
        return list(set(valid_emails)) if valid_emails else None
    
    @staticmethod
    def extract_phone(text):
        if not text:
            return None
        patterns = [
            r'\b\+?\d{1,3}[-.\s]\(?\d{3}\)?[-.\s]\d{3}[-.\s]\d{4}\b', 
            r'\b\(\d{3}\)\s?\d{3}[-.\s]?\d{4}\b',
            r'\b\d{10}\b', 
        ]
        matches = []
        for pattern in patterns:
            found = re.findall(pattern, text)
            matches.extend(found)
        return list(set(matches)) if matches else None # Return list of all found phones

    @staticmethod
    def extract_name_from_email(email):
        """
        Rule: s.smith@... -> S Smith
        john.doe@... -> John Doe
        """
        if not email: return None
        try:
            local_part = email.split('@')[0]
            # Replace dots, underscores, numbers with spaces
            clean_name = re.sub(r'[._0-9]+', ' ', local_part).strip()
            # Title case
            return clean_name.title()
        except: return None

    @staticmethod
    def extract_company_from_email(email):
        """
        Rule: ...@google.com -> Google
        """
        if not email: return None
        try:
            domain = email.split('@')[1]
            if not domain: return None
            
            # Remove TLD
            company = domain.rsplit('.', 1)[0]
            
            # Common public domains to ignore for company name
            public_domains = {'gmail', 'yahoo', 'hotmail', 'outlook', 'icloud', 'aol', 'protonmail'}
            if company.lower() in public_domains:
                return None
                
            return company.title()
        except: return None
    


    # Job-related keywords (broad indicators)
    JOB_KEYWORDS = [
        'hiring', 'job', 'position', 'opportunity', 'opening',
        'w2', 'c2c', 'corp-to-corp', 'corp to corp', '1099', 'bench', 'full time', 'full-time', 
        'contract', 'immediate', 'looking for', 'seeking', 'recruiting', 
        'join our team', 'apply', 'careers', 'employment', 'remote', 'hybrid', 'on-site',
        'hourly rate', 'salary', 'stipend', 'freelance', 'temporary', 'consultant',
        'staffing', 'agency', 'vendor', 'implementation partner', 'direct client',
        'visa sponsorship', 'h1b', 'opt', 'gc', 'citizen', 'green card', 'ead',
        'send resume', 'share profile', 'email me', 'reaching out', 'dm me',
        'urgent role', 'immediate requirement', 'looking to hire', 'multiple positions',
        'worked on a role', 'open for', 'resumes to', 'drop your email',
        'interested candidates', 'comment below', 'hiring for'
    ]

    @staticmethod
    def has_job_keywords(text):
        if not text:
            return False
        text_lower = text.lower()
        return any(kw in text_lower for kw in ProcessorModule.JOB_KEYWORDS)
    


    @staticmethod
    def extract_zip(text):
        """Extract a US zip code (5 digits) from text."""
        if not text: return ""
        # Look for 5-digit zip codes
        match = re.search(r'\b\d{5}(?:-\d{4})?\b', text)
        if match:
            return match.group(0)
        return ""

    @staticmethod
    def extract_job_title(text):
        """
        Attempt to extract a job title from the post text.
        Looks at the first few lines and matches against common tech roles.
        """
        if not text: return "Unknown Role"
        
        # 1. Look for explicit labels: "Role: ...", "Position: ..."
        labels = [r'role[:\s]+', r'position[:\s]+', r'title[:\s]+', r'hiring\s+for\s+', r'looking\s+for\s+(?:a\s+)?']
        for label in labels:
            match = re.search(label + r'([^\n,.]+)', text, re.IGNORECASE)
            if match:
                title = match.group(1).strip()
                if len(title) > 3 and len(title) < 100:
                    return title.title()

        return "Hiring Post"

    @staticmethod
    def extract_contract_type(text):
        """Extract W2, C2C, 1099, etc. from text."""
        if not text: return "N/A"
        text_lower = text.lower()
        results = []
        if 'w2' in text_lower: results.append('W2')
        if 'c2c' in text_lower or 'corp-to-corp' in text_lower or 'corp to corp' in text_lower: 
            results.append('C2C')
        if '1099' in text_lower: results.append('1099')
        if 'full-time' in text_lower or 'full time' in text_lower: results.append('Full-Time')
        if 'contract' in text_lower and 'c2c' not in text_lower and 'w2' not in text_lower:
             results.append('Contract')
        
        return ", ".join(results) if results else "N/A"

    @staticmethod
    def classify_job_post(text):
        """
        Rule-based classifier to determine if a post is a job listing.
        Returns (is_job, details_dict) where details include score and matched rules.
        """
        if not text: return False, {"score": 0, "reason": "No text"}
        
        text_lower = text.lower()
        score = 0
        matches = []
        
        # 1. Structural Headers (+20 each)
        headers = [
            'responsibilit', 'requirement', 'qualification', 'skills',
            'what we are looking for', 'nice to have', 'must have', 'experience',
            'ideal candidate', 'job description', 'essential', 'positions',
            'openings available', 'roles:'
        ]
        for h in headers:
            if h in text_lower:
                score += 20
                matches.append(f"Header: {h}")
        
        # 2. Hiring Intent (+15 each)
        intent_phrases = [
            'hiring', 'looking for', 'join our team', 'we are expanding', 
            'open role', 'job opening', 'new role', 'we are looking for',
            'positions available', 'seeking talent', 'immediate start',
            'interviewing', 'hiring for', 'we have an opening'
        ]
        for phrase in intent_phrases:
            if phrase in text_lower:
                score += 15
                matches.append(f"Intent: {phrase}")
                
        # 3. Call to Action (+15)
        cta_patterns = [
            r'send\s+(?:your\s+)?(?:resume|cv)', r'apply\s+at', r'link\s+in\s+bio',
            r'dm\s+me', r'apply\s+here', r'email\s+me', r'share\s+profile', r'share\s+resume',
            r'contact\s+at'
        ]
        for pattern in cta_patterns:
            if re.search(pattern, text_lower):
                score += 15
                matches.append(f"CTA: {pattern}")
                
        # 4. Job Keywords (+5) - Scoring using the internal broad list
        for kw in ProcessorModule.JOB_KEYWORDS:
            if kw in text_lower:
                score += 5
                matches.append(f"Keyword: {kw}")
                
        # 5. Negative Rules (Penalties)
        # Avoid candidates looking for work
        negative_phrases = [
            'open to work', 'looking for a new role', 'looking for my next adventure', 
            'looking for a job', 'i am looking for', 'seeking new opportunities',
            'i am seeking', 'unemployed'
        ]
        for phrase in negative_phrases:
            if phrase in text_lower:
                score -= 100
                matches.append(f"NEGATIVE: {phrase}")
                
        is_job = score >= 40 # Lowered for maximum contract extraction
        
        return is_job, {
            "score": score,
            "is_job": is_job,
            "matched_rules": list(set(matches)) # Dedupe matches
        }
