"""Complete email reporting module for LinkedIn bot runs - handles generation AND sending."""
import smtplib
import config
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from datetime import datetime
from modules.logger import logger

class BotReporter:
    """Handles complete email reporting for bot execution - generation and sending."""
    def __init__(self, bot_instance):
        self.bot = bot_instance
        self.server = config.SMTP_SERVER
        self.port = config.SMTP_PORT
        self.username = config.SMTP_USERNAME
        self.password = config.SMTP_PASSWORD
        self.email_from = config.EMAIL_FROM
        
        # Support multiple recipients (comma-separated)
        if config.EMAIL_TO:
            self.email_to = [email.strip() for email in config.EMAIL_TO.split(',')]
        else:
            self.email_to = []
    
    def _is_configured(self):
        return all([self.server, self.port, self.username, self.password, self.email_from, self.email_to])
    
    def send_run_report(self):
        try:
            subject, html_body = self._generate_html_report()
            
            if not subject or not html_body:
                logger.warning("Failed to generate report content", extra={"step_name": "BotReporter"})
                return False
            
            return self._send_email(subject, html_body)
                
        except Exception as e:
            logger.error(f"Failed to generate/send email report: {e}", extra={"step_name": "BotReporter"}, exc_info=True)
            return False
    
    def _send_email(self, subject, html_body):
        if not self._is_configured():
            logger.warning("SMTP not configured. Skipping email report.", extra={"step_name": "BotReporter"})
            logger.info(f"--- EMAIL REPORT (DRY RUN) ---\nSubject: {subject}\nBody (HTML size): {len(html_body)} chars\n-----------------------------", extra={"step_name": "BotReporter"})
            return True  # Not strictly a failure, just not configured
            
        msg = MIMEMultipart()
        msg['From'] = self.email_from
        msg['To'] = ', '.join(self.email_to)  # Join multiple recipients for header
        msg['Subject'] = subject
        
        msg.attach(MIMEText(html_body, 'html'))
        
        try:
            logger.info(f"Connecting to SMTP server at {self.server}:{self.port}...", extra={"step_name": "BotReporter"})
            with smtplib.SMTP(self.server, self.port) as server:
                server.starttls()
                server.login(self.username, self.password)
                text = msg.as_string()
                server.sendmail(self.email_from, self.email_to, text)  # Send to list of recipients
                logger.info(f"Email report sent successfully to {len(self.email_to)} recipient(s): {', '.join(self.email_to)}", extra={"step_name": "BotReporter"})
                return True
        except Exception as e:
            logger.error(f"Failed to send email report: {e}", extra={"step_name": "BotReporter"}, exc_info=True)
            return False
    
    def _generate_html_report(self):
        try:
            start_t = self.bot.metrics.metrics.get('start_time')
            end_t = self.bot.metrics.metrics.get('end_time')
            duration = str(end_t - start_t) if start_t and end_t else "N/A"
            
            final_metrics = {
                "Total Posts Seen": self.bot.total_seen,
                "Relevant Posts Found": self.bot.total_relevant,
                "Contacts Extracted (In-Memory)": self.bot.total_saved,
                "Contacts Synced To DB Vendor Table": self.bot.total_synced,
                "Posts Saved to Disk": self.bot.posts_saved,
                "Keywords Processed": ", ".join(self.bot.keyword_metrics.keys()) if self.bot.keyword_metrics else "None",
                "Duration": duration
            }

            
            html_rows = ""
            for k, v in final_metrics.items():
                html_rows += f"<tr><td style='padding: 8px; border: 1px solid #ddd;'>{k}</td><td style='padding: 8px; border: 1px solid #ddd;'>{v}</td></tr>"

            
            keyword_rows = ""
            for k, m in self.bot.keyword_metrics.items():
                keyword_rows += f"<tr><td style='padding: 8px; border: 1px solid #ddd;'>{k}</td><td style='padding: 8px; border: 1px solid #ddd; text-align: center;'>{m['seen']}</td><td style='padding: 8px; border: 1px solid #ddd; text-align: center;'>{m['relevant']}</td><td style='padding: 8px; border: 1px solid #ddd; text-align: center;'>{m['extracted']}</td><td style='padding: 8px; border: 1px solid #ddd; text-align: center;'>{m['saved']}</td></tr>"

           
            error_section = ""
            failed_reasons = self.bot.metrics.metrics.get('failed_reasons', {})
            if failed_reasons:
                error_rows = ""
                for err_name, count in failed_reasons.items():
                    error_rows += f"<li>{err_name}: {count}</li>"
                if error_rows:
                    error_section = f"<h3>Errors/Warnings</h3><ul>{error_rows}</ul>"

            # 5. Build complete HTML body
            email_body = f"""
            <html>
            <body style="font-family: Arial, sans-serif;">
                <h2>LinkedIn Posts Bot Run Report</h2>
                <p><strong>Date:</strong> {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}</p>
                <p><strong>Candidate ID:</strong> {self.bot.candidate_id if self.bot.candidate_id else 'Single User Mode'}</p>
                
                <h3>Session Metrics</h3>
                <table style="border-collapse: collapse; width: 100%; max-width: 600px;">
                    <tr style="background-color: #f2f2f2;">
                        <th style="padding: 10px; border: 1px solid #ddd; text-align: left;">Metric</th>
                        <th style="padding: 10px; border: 1px solid #ddd; text-align: left;">Value</th>
                    </tr>
                    {html_rows}
                </table>

                <h3>Keyword Breakdown</h3>
                <table style="border-collapse: collapse; width: 100%; max-width: 800px;">
                    <tr style="background-color: #f2f2f2;">
                        <th style="padding: 8px; border: 1px solid #ddd; text-align: left;">Keyword</th>
                        <th style="padding: 8px; border: 1px solid #ddd; text-align: center;">Seen</th>
                        <th style="padding: 8px; border: 1px solid #ddd; text-align: center;">Relevant</th>
                        <th style="padding: 8px; border: 1px solid #ddd; text-align: center;">Extracted</th>
                        <th style="padding: 8px; border: 1px solid #ddd; text-align: center;">Saved</th>
                    </tr>
                    {keyword_rows}
                </table>
                
                {error_section}
                
                <p style="font-size: 0.9em; color: #666;">
                    <em>Report generated by LinkedIn Posts Contract Extractor Bot.</em>
                </p>
            </body>
            </html>
            """
            
            # 6. Generate subject
            subject = f"LinkedIn Posts Contract Extractor Bot Report - {datetime.now().strftime('%Y-%m-%d')}"
            if self.bot.candidate_id:
                subject += f" (Cand: {self.bot.candidate_id})"
            
            return subject, email_body
            
        except Exception as e:
            logger.error(f"Failed to generate report HTML: {e}", extra={"step_name": "BotReporter"}, exc_info=True)
            return None, None
