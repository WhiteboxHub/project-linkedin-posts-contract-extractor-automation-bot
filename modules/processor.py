import re
import config

class ProcessorModule:
    @staticmethod
    def extract_email(text):
        if not text:
            return None
            
        patterns = [
            r'\b[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Z|a-z]{2,}\b',
            r'[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}',
        ]
        
        # Invalid email patterns to exclude (mostly media extensions or placeholders)
        invalid_patterns = [
            r'\.png', r'\.jpg', r'\.jpeg', r'\.gif', r'\.svg',
            r'@2x\.', r'entity-circle', r'placeholder',
            r'example\.com', r'test\.com', r'gmail\.com', r'yahoo\.com', r'hotmail\.com', r'outlook\.com', r'aol\.com', r'icloud\.com', r'protonmail\.com', r'mail\.com', r'yopmail\.com',
        ]
        
        for pattern in patterns:
            emails = re.findall(pattern, text, re.IGNORECASE)
            for email in emails:
                if '@' in email and '.' in email.split('@')[1]:
                    is_invalid = any(re.search(inv, email, re.IGNORECASE) for inv in invalid_patterns)
                    
                    if config.LINKEDIN_EMAIL and email.lower().strip() == config.LINKEDIN_EMAIL.lower().strip():
                        is_invalid = True
                        
                    if not is_invalid:
                        return email
        
        return None
    
    @staticmethod
    def extract_phone(text):
        if not text:
            return None
        patterns = [
            r'\+?\d{1,3}[-.\s]?\(?\d{3}\)?[-.\s]?\d{3}[-.\s]?\d{4}',
            r'\(\d{3}\)\s?\d{3}[-.\s]?\d{4}',
            r'\d{3}[-.\s]?\d{3}[-.\s]?\d{4}',
        ]
        for pattern in patterns:
            phones = re.findall(pattern, text)
            if phones:
                return phones[0]
        return None
    
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
