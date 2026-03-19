import logging
from typing import Dict, List
import os

logger = logging.getLogger(__name__)

# Suppress HuggingFace/transformers warnings before importing gliner
os.environ["TOKENIZERS_PARALLELISM"] = "false"

class GLiNERExtractor:
    """
    Extract entities using GLiNER - zero-shot NER model
    """
    
    def __init__(self, model_name='urchade/gliner_base', threshold=0.5):
        self.logger = logger
        self.threshold = threshold
        self.entity_labels = [
            'Job Title',
            'Company Name',
            'Job Location',
            'IT Skill or Technology',
            'Hourly Rate or Salary',
            'Contract Duration',
            'Visa Requirement',
            'Recruiter Name'
        ]
        
        try:
            from gliner import GLiNER
            # Load the model
            self.model = GLiNER.from_pretrained(model_name)
            self.logger.info(f"GLiNER model loaded: {model_name}")
        except Exception as e:
            self.logger.error(f"Failed to load GLiNER: {str(e)}")
            self.model = None
            
    def extract_entities(self, text: str) -> Dict[str, any]:
        try:
            if not text or not self.model:
                return {}
            
            # 1. Primary extraction for titles and roles (usually in first 1000 chars)
            header_entities = self.model.predict_entities(
                text[:1000],
                ['Job Title'],
                threshold=self.threshold,
                flat_ner=True
            )
            
            # 2. General extraction for all other entities
            all_entities = self.model.predict_entities(
                text[:3000], 
                self.entity_labels,
                threshold=self.threshold,
                flat_ner=True
            )
            
            return self._parse_entities(all_entities, header_entities=header_entities)
            
        except Exception as e:
            self.logger.error(f"GLiNER extraction error: {str(e)}")
            return {}
            
    def _parse_entities(self, entities_raw: List[Dict], header_entities: List[Dict] = None) -> Dict[str, any]:
        """Parse GLiNER output into a more usable format"""
        parsed = {
            'job_title': None,
            'company': None,
            'location': None,
            'recruiter_name': None,
            'skills': [],
            'pay_rate': None,
            'duration': None,
            'visa_status': None
        }

        # Keep candidates and sort by score
        candidates = {
            'job_title': [],
            'company': [],
            'location': [],
            'recruiter_name': [],
            'pay_rate': [],
            'duration': [],
            'visa_status': []
        }

        # Use header_entities primarily for titles if provided
        source_entities = entities_raw
        if header_entities:
            # We add header entities specifically to the pool
            source_entities = entities_raw + header_entities

        for ent in source_entities:
            label = ent['label'].lower()
            text = ent['text'].strip()
            score = ent.get('score', 0)
            
            if len(text) < 2:
                continue
                
            if 'job title' in label:
                text_lower = text.lower()
                # Exclude obvious non-job-titles
                junk_phrases = [
                    'hiring post', 'could be a great fit', 'years of', 'applied ', 'on #c2c', 
                    'c2c role', 'w2 role', '1099 role', 'looking for', 'we have', 'open role',
                    'usc only', 'gc only', 'usc/gc', 'h4ead', 'h4 ead', 'visa sponsorship',
                    'responsibilities:', 'requirements:', 'qualifications:', 'about the role',
                    'greetings', 'note:', 'fake profile', 'reach out', 'drop your email'
                ]
                if any(phrase in text_lower for phrase in junk_phrases):
                    continue
                # Exclude very long strings that are likely sentences
                if len(text) > 60 or len(text.split()) > 6:
                    continue
                
                # Trim punctuation and junk from standard titles
                text = text.strip(' .,;:!()[]{}#*')
                if len(text) < 2:
                    continue
                    
                candidates['job_title'].append((text, score))
            elif 'company' in label:
                candidates['company'].append((text, score))
            elif 'location' in label:
                candidates['location'].append((text, score))
            elif 'recruiter name' in label:
                candidates['recruiter_name'].append((text, score))
            elif 'hourly rate' in label or 'salary' in label or 'pay' in label:
                candidates['pay_rate'].append((text, score))
            elif 'contract duration' in label or 'duration' in label:
                candidates['duration'].append((text, score))
            elif 'visa' in label:
                candidates['visa_status'].append((text, score))
            elif 'skill' in label or 'technology' in label:
                if text not in parsed['skills']:
                    parsed['skills'].append(text)

        # Pick the highest scoring candidate for single-value fields
        for field, items in candidates.items():
            if items:
                items.sort(key=lambda x: x[1], reverse=True)
                parsed[field] = items[0][0]

        return parsed
