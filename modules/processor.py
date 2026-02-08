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
    
    @staticmethod
    def has_job_keywords(text):
        if not text:
            return False
        text_lower = text.lower()
        return any(kw in text_lower for kw in config.JOB_KEYWORDS)
    
    @staticmethod
    def is_ai_tech_related(text):
        if not text:
            return False
        text_lower = text.lower()
        return any(kw in text_lower for kw in config.AI_KEYWORDS)

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
            'responsibilit', 'requirement', 'qualification', 
            'what we are looking for', 'nice to have', 'must have'
        ]
        for h in headers:
            if h in text_lower:
                score += 20
                matches.append(f"Header: {h}")
        
        # 2. Hiring Intent (+15 each)
        intent_phrases = [
            'hiring', 'looking for', 'join our team', 'we are expanding', 
            'open role', 'job opening', 'new role', 'we are looking for'
        ]
        for phrase in intent_phrases:
            if phrase in text_lower:
                score += 15
                matches.append(f"Intent: {phrase}")
                
        # 3. Call to Action (+15)
        cta_phrases = [
            'send resume', 'send cv', 'apply at', 'link in bio', 
            'dm me', 'apply here', 'email me'
        ]
        for cta in cta_phrases:
            if cta in text_lower:
                score += 15
                matches.append(f"CTA: {cta}")
                
        # 4. Job Keywords (+5) - Low confidence but helpful
        job_keywords = [
            'remote', 'hybrid', 'on-site', 'c2c', 'w2', '1099', 
            'contract', 'full-time', 'part-time', 'hourly rate', 'salary'
        ]
        for kw in job_keywords:
            if kw in text_lower:
                score += 5
                matches.append(f"Keyword: {kw}")
                
        # 5. Negative Rules (Penalties)
        # Avoid candidates looking for work
        negative_phrases = [
            'open to work', 'looking for a new role', 'looking for my next adventure', 
            'looking for a job', 'i am looking for'
        ]
        for phrase in negative_phrases:
            if phrase in text_lower:
                score -= 100
                matches.append(f"NEGATIVE: {phrase}")
                
        is_job = score >= 50
        
        return is_job, {
            "score": score,
            "is_job": is_job,
            "matched_rules": list(set(matches)) # Dedupe matches
        }
