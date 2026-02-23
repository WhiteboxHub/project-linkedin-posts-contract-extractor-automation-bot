import pandas as pd
import re
import dns.resolver
import logging
import argparse
import sys
from typing import List, Dict, Optional
import smtplib
import socket
from concurrent.futures import ThreadPoolExecutor, as_completed

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger("EmailListValidator")

class EmailListValidator:
    EMAIL_REGEX = r'^[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,}$'
    def __init__(self, filepath: Optional[str] = None, data_list: Optional[List[Dict]] = None):
        self.filepath = filepath
        self.df = None
        self.domain_mx_cache = {}
        if data_list is not None:
            self.df = pd.DataFrame(data_list)
        elif filepath is not None:
            self.load_data()

    def load_data(self) -> None:
        """Loads email data from CSV file."""
        try:
            self.df = pd.read_csv(self.filepath)
            logger.info(f"Loaded {len(self.df)} rows from {self.filepath}")
        except Exception as e:
            logger.error(f"Failed to load file: {e}")
            raise

    def normalize_emails(self, column_name: str = 'email') -> None:
        """Normalizes email column by stripping whitespace and converting to lowercase."""
        if column_name not in self.df.columns:
            logger.warning(f"Column '{column_name}' not found - skipping normalization.")
            return
            
        self.df[column_name] = self.df[column_name].astype(str).str.strip().str.lower()
        logger.info(f"Normalized '{column_name}' column.")

    def validate_syntax_individual(self, email: str) -> bool:
        return bool(re.match(self.EMAIL_REGEX, str(email)))

    def validate_syntax(self, column_name: str = 'email') -> None:
        logger.info("Starting syntax validation...")
        self.df['syntax_valid'] = self.df[column_name].apply(self.validate_syntax_individual)
        valid_count = self.df['syntax_valid'].sum()
        logger.info(f"Syntax validation complete. {valid_count}/{len(self.df)} valid.")

    def _has_mx(self, domain: str) -> bool:
        if not domain or domain == 'nan':
            return False
            
        if domain in self.domain_mx_cache:
            return self.domain_mx_cache[domain]

        try:
            dns.resolver.resolve(domain, 'MX')
            self.domain_mx_cache[domain] = True
            return True
        except (dns.resolver.NoAnswer, dns.resolver.NXDOMAIN, dns.exception.Timeout):
            self.domain_mx_cache[domain] = False
            return False
        except Exception as e:
            logger.warning(f"DNS check failed for {domain}: {e}")
            self.domain_mx_cache[domain] = False
            return False

    def validate_mx(self, column_name: str = 'email', max_workers: int = 20) -> None:
        """Validates MX records for generic domains using concurrent DNS lookups."""
        logger.info("Starting MX record validation...")
        
        # Extract domain
        self.df['domain'] = self.df[column_name].apply(
            lambda x: x.split('@')[-1] if isinstance(x, str) and '@' in x else None
        )

        # Get unique domains to minimize DNS queries
        unique_domains = self.df['domain'].dropna().unique()
        total_unique = len(unique_domains)
        logger.info(f"Checking DNS for {total_unique} unique domains with {max_workers} workers...")
        
        # Use ThreadPoolExecutor for concurrent DNS lookups
        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            future_to_domain = {executor.submit(self._has_mx, str(domain)): domain for domain in unique_domains}
            
            completed = 0
            for future in as_completed(future_to_domain):
                completed += 1
                if completed % 100 == 0:
                    logger.info(f"Processed {completed}/{total_unique} domains...")

        self.df['mx_valid'] = self.df['domain'].apply(lambda d: self.domain_mx_cache.get(str(d), False))
        
        mx_valid_count = self.df['mx_valid'].sum()
        logger.info(f"MX validation complete. {mx_valid_count} valid domains found.")

    def verify_mailbox(self, email: str) -> str:
        """
        Checks if the mailbox exists on the remote SMTP server.
        Returns: 'valid', 'invalid', 'unknown', or 'error'
        """
        if not email or '@' not in str(email):
            return "error"
            
        domain = str(email).split('@')[1]
        
        try:
            # Resolve MX record
            try:
                mx_records = dns.resolver.resolve(domain, 'MX')
                if not mx_records:
                    return "error"
                # Sort by preference
                mx_records = sorted(mx_records, key=lambda r: r.preference)
                mx_host = str(mx_records[0].exchange).rstrip('.')
            except Exception:
                return "error"

            # Connect to SMTP
            server = smtplib.SMTP(timeout=5)
            server.set_debuglevel(0)
            
            try:
                server.connect(mx_host)
                server.helo("talentdirect-connect.com") # Using a generic domain for HELO
                server.mail("outreach@talentdirect-connect.com")
                code, message = server.rcpt(email)
                server.quit()
                
                if code == 250:
                    return "valid"
                elif code == 550:
                    return "invalid"
                else:
                    return "unknown"
            except Exception:
                return "unknown" # Connection failed or other SMTP error
                
        except Exception as e:
            return "error"

    def validate_mailbox(self, column_name: str = 'email', max_workers: int = 20) -> None:
        """Validates mailboxes for syntax-valid and MX-valid emails."""
        logger.info("Starting SMTP mailbox validation...")
        
        emails_to_check = self.df[column_name].tolist()
        total_emails = len(emails_to_check)
        
        results = []
        
        logger.info(f"Checking mailboxes for {total_emails} emails with {max_workers} workers...")
        
        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            unique_emails = list(set([str(e) for e in emails_to_check if '@' in str(e)]))
            future_to_email = {executor.submit(self.verify_mailbox, email): email for email in unique_emails}
            
            email_status_map = {}
            completed = 0
            total_unique = len(unique_emails)
            
            for future in as_completed(future_to_email):
                email = future_to_email[future]
                try:
                    status = future.result()
                except Exception:
                    status = "error"
                email_status_map[email] = status
                
                completed += 1
                if completed % 100 == 0:
                    logger.info(f"Processed {completed}/{total_unique} mailboxes...")
        
        self.df['mailbox_status'] = self.df[column_name].map(lambda e: email_status_map.get(str(e), "error"))
        
        valid_count = len(self.df[self.df['mailbox_status'] == 'valid'])
        invalid_count = len(self.df[self.df['mailbox_status'] == 'invalid'])
        logger.info(f"Mailbox validation complete. {valid_count} valid, {invalid_count} invalid.")

    def export_results(self, output_path: str) -> None:
        """Exports the validated DataFrame to a CSV file."""
        try:
            self.df.to_csv(output_path, index=False)
            logger.info(f"Results exported to {output_path}")
        except Exception as e:
            logger.error(f"Failed to export results: {e}")
            raise

    def run(self, input_col: str = 'email', output_file: str = 'validated_emails.csv', workers: int = 20):
        """Executes the full validation pipeline."""
        if self.df is None or self.df.empty:
            logger.warning("No data to validate.")
            return

        self.normalize_emails(input_col)
        self.validate_syntax(input_col)
        self.validate_mx(input_col, max_workers=workers)
        self.validate_mailbox(input_col, max_workers=workers)
        
        if output_file:
            self.export_results(output_file)
            
            # Export failed mailboxes
            failed_file = output_file.replace('.csv', '_failed_mailbox.csv')
            failed_df = self.df[self.df['mailbox_status'] == 'invalid']
            failed_df.to_csv(failed_file, index=False)
            logger.info(f"Exported {len(failed_df)} invalid mailboxes to {failed_file}")
        
        # Summary
        total = len(self.df)
        valid_syntax = self.df['syntax_valid'].sum()
        valid_mx = self.df['mx_valid'].sum()
        valid_mailbox = len(self.df[self.df['mailbox_status'] == 'valid'])
        
        print("\n--- Validation Summary ---")
        print(f"Total Emails:   {total}")
        print(f"Syntax Valid:   {valid_syntax} ({(valid_syntax/total)*100:.1f}%)")
        print(f"MX Valid:       {valid_mx} ({(valid_mx/total)*100:.1f}%)")
        print(f"Mailbox Valid:  {valid_mailbox} ({(valid_mailbox/total)*100:.1f}%)")
        if output_file:
            print(f"Output saved to: {output_file}")
            print(f"Failed mailboxes saved to: {failed_file}")

def main():
    parser = argparse.ArgumentParser(description="Validate email lists for syntax and MX records.")
    parser.add_argument("input_file", help="Path to the input CSV file.")
    parser.add_argument("--col", default="email", help="Name of the email column in CSV (default: 'email').")
    parser.add_argument("--output", default="validated_emails.csv", help="Path to save the output CSV.")
    parser.add_argument("--workers", type=int, default=50, help="Number of concurrent workers for DNS checks (default: 50).")
    
    args = parser.parse_args()
    
    validator = EmailListValidator(filepath=args.input_file)
    validator.run(input_col=args.col, output_file=args.output, workers=args.workers)

if __name__ == "__main__":
    main()
