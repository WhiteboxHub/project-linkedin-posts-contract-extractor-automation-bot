"""Complete email reporting module for LinkedIn bot runs - handles generation AND sending."""
import html
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
        config_values = {
            "server": self.server,
            "port": self.port,
            "username": self.username,
            "password": "set" if self.password else None,
            "from": self.email_from,
            "to": self.email_to
        }
        missing = [k for k, v in config_values.items() if not v]
        if missing:
            logger.info(f"SMTP configuration missing: {', '.join(missing)} - Reporting will be skipped.", extra={"step_name": "BotReporter"})
            return False
        return True
    
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
            date_time_str = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
            raw_cand_id = self.bot.candidate_id if self.bot.candidate_id else 'Single User Mode'
            escaped_cand_id = html.escape(str(raw_cand_id))
            
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
                escaped_k = html.escape(str(k))
                escaped_v = html.escape(str(v))
                html_rows += f"""
                <tr class="metric-row">
                    <td class="metric-name" style="padding: 12px 15px; border-bottom: 1px solid #f1f5f9; color: #475569; font-size: 13px;">{escaped_k}</td>
                    <td class="metric-val" style="padding: 12px 15px; border-bottom: 1px solid #f1f5f9; text-align: right; font-weight: 600; color: #1e293b; font-size: 13px;">{escaped_v}</td>
                </tr>
                """

            keyword_rows = ""
            for k, m in self.bot.keyword_metrics.items():
                escaped_k = html.escape(str(k))
                seen = m.get('seen', 0) if isinstance(m, dict) else getattr(m, 'seen', 0)
                relevant = m.get('relevant', 0) if isinstance(m, dict) else getattr(m, 'relevant', 0)
                extracted = m.get('extracted', 0) if isinstance(m, dict) else getattr(m, 'extracted', 0)
                saved = m.get('saved', 0) if isinstance(m, dict) else getattr(m, 'saved', 0)
                keyword_rows += f"""
                <tr class="metric-row">
                    <td style="padding: 12px 15px; border-bottom: 1px solid #f1f5f9; color: #334155; font-size: 13px;">{escaped_k}</td>
                    <td style="padding: 12px 15px; border-bottom: 1px solid #f1f5f9; text-align: center; color: #64748b; font-size: 13px;">{seen}</td>
                    <td style="padding: 12px 15px; border-bottom: 1px solid #f1f5f9; text-align: center; color: #2563eb; font-weight: 600; font-size: 13px;">{relevant}</td>
                    <td style="padding: 12px 15px; border-bottom: 1px solid #f1f5f9; text-align: center; color: #16a34a; font-weight: 600; font-size: 13px;">{extracted}</td>
                    <td style="padding: 12px 15px; border-bottom: 1px solid #f1f5f9; text-align: center; color: #0d9488; font-weight: 600; font-size: 13px;">{saved}</td>
                </tr>
                """

            error_section = ""
            failed_reasons = self.bot.metrics.metrics.get('failed_reasons', {})
            if failed_reasons:
                error_rows = ""
                for err_name, count in failed_reasons.items():
                    error_rows += f"<li style='margin-bottom: 4px;'><strong>{html.escape(str(err_name))}</strong>: {html.escape(str(count))}</li>"
                if error_rows:
                    error_section = f"""
                    <div style="margin-top: 25px; padding: 18px 20px; background-color: #fef2f2; border: 1px solid #fee2e2; border-radius: 8px;">
                        <div style="color: #991b1b; font-size: 14px; font-weight: bold; margin-bottom: 10px; display: flex; align-items: center;">
                            <span style="margin-right: 8px;">⚠️</span> Errors / Warnings
                        </div>
                        <ul style="margin: 0; padding-left: 20px; color: #b91c1c; font-size: 13px; line-height: 1.5;">
                            {error_rows}
                        </ul>
                    </div>
                    """

            email_body = f"""
            <html>
                <head>
                    <style>
                        body {{ font-family: 'Segoe UI', Tahoma, Geneva, Verdana, sans-serif; margin: 0; padding: 0; background-color: #f4f7f9; color: #333; }}
                        .header {{ background: linear-gradient(135deg, #1e4eb8 0%, #3b82f6 100%); color: white; padding: 35px 25px 45px 25px; text-align: left; border-radius: 8px 8px 0 0; }}
                        .header h1 {{ margin: 8px 0 4px 0; font-size: 24px; font-weight: bold; color: #ffffff; }}
                        .header p {{ margin: 0; font-size: 13px; opacity: 0.9; }}
                        .header .sub {{ font-size: 11px; font-weight: bold; text-transform: uppercase; letter-spacing: 1.5px; opacity: 0.85; }}
                        .content {{ padding: 30px; max-width: 680px; margin: 0 auto; background: white; border-radius: 8px; box-shadow: 0 4px 15px rgba(0,0,0,0.05); margin-top: -20px; }}
                        .intro-text {{ color: #64748b; font-size: 14px; line-height: 1.6; margin-top: 0; margin-bottom: 25px; }}
                        .section-title {{ font-size: 16px; font-weight: bold; margin: 25px 0 12px 0; display: flex; align-items: center; color: #1e293b; }}
                        table {{ width: 100%; border-collapse: collapse; margin-bottom: 20px; }}
                        th {{ background-color: #f8fafc; color: #64748b; font-size: 11px; text-transform: uppercase; text-align: left; padding: 12px 15px; border-bottom: 1px solid #e2e8f0; }}
                        td {{ padding: 12px 15px; font-size: 13px; border-bottom: 1px solid #f1f5f9; }}
                        .footer {{ text-align: center; padding: 25px 20px 10px 20px; font-size: 12px; color: #94a3b8; line-height: 1.6; }}
                    </style>
                </head>
                <body style="font-family: 'Segoe UI', Tahoma, Geneva, Verdana, sans-serif; margin: 0; padding: 0; background-color: #f4f7f9; color: #333;">
                    <div style="max-width: 680px; margin: 20px auto;">
                        <div style="background: linear-gradient(135deg, #1e4eb8 0%, #3b82f6 100%); color: white; padding: 35px 25px 45px 25px; text-align: left; border-radius: 8px 8px 0 0;">
                            <div style="font-size: 11px; font-weight: bold; text-transform: uppercase; letter-spacing: 1.5px; opacity: 0.85;">WHITEBOX LEARNING</div>
                            <h1 style="margin: 8px 0 4px 0; font-size: 24px; font-weight: bold; color: #ffffff;">LinkedIn Posts Bot Run Report</h1>
                            <p style="margin: 0; font-size: 13px; opacity: 0.9;">Run completed on {date_time_str} • Candidate: {escaped_cand_id}</p>
                        </div>

                        <div style="padding: 30px; background: #ffffff; border-radius: 8px; box-shadow: 0 4px 15px rgba(0,0,0,0.05); margin-top: -20px;">
                            <p style="color: #64748b; font-size: 14px; line-height: 1.6; margin-top: 0; margin-bottom: 25px;">
                                The automated LinkedIn posts extraction pipeline has completed for candidate <strong>{escaped_cand_id}</strong>.
                            </p>

                            <div style="display: flex; gap: 12px; margin-bottom: 25px; flex-wrap: wrap;">
                                <div style="flex: 1; min-width: 120px; padding: 16px 14px; border-radius: 8px; text-align: center; background-color: #eff6ff; border: 1px solid #dbeafe; color: #1e40af;">
                                    <span style="font-size: 24px; font-weight: bold; display: block; margin-bottom: 4px; color: #1e40af;">{self.bot.total_seen}</span>
                                    <span style="font-size: 10px; font-weight: bold; text-transform: uppercase; color: #64748b; letter-spacing: 0.5px;">POSTS SEEN</span>
                                </div>
                                <div style="flex: 1; min-width: 120px; padding: 16px 14px; border-radius: 8px; text-align: center; background-color: #f0fdfa; border: 1px solid #ccfbf1; color: #115e59;">
                                    <span style="font-size: 24px; font-weight: bold; display: block; margin-bottom: 4px; color: #115e59;">{self.bot.total_relevant}</span>
                                    <span style="font-size: 10px; font-weight: bold; text-transform: uppercase; color: #64748b; letter-spacing: 0.5px;">RELEVANT</span>
                                </div>
                                <div style="flex: 1; min-width: 120px; padding: 16px 14px; border-radius: 8px; text-align: center; background-color: #f0fdf4; border: 1px solid #dcfce7; color: #166534;">
                                    <span style="font-size: 24px; font-weight: bold; display: block; margin-bottom: 4px; color: #166534;">{self.bot.total_saved}</span>
                                    <span style="font-size: 10px; font-weight: bold; text-transform: uppercase; color: #64748b; letter-spacing: 0.5px;">EXTRACTED</span>
                                </div>
                                <div style="flex: 1; min-width: 120px; padding: 16px 14px; border-radius: 8px; text-align: center; background-color: #fffbeb; border: 1px solid #fef3c7; color: #92400e;">
                                    <span style="font-size: 24px; font-weight: bold; display: block; margin-bottom: 4px; color: #92400e;">{self.bot.total_synced}</span>
                                    <span style="font-size: 10px; font-weight: bold; text-transform: uppercase; color: #64748b; letter-spacing: 0.5px;">SYNCED</span>
                                </div>
                            </div>

                            <div style="font-size: 16px; font-weight: bold; margin: 25px 0 12px 0; color: #1e293b;">
                                <span style="margin-right: 8px;">📊</span> Session Metrics
                            </div>

                            <table style="width: 100%; border-collapse: collapse; margin-bottom: 20px;">
                                <thead>
                                    <tr>
                                        <th style="background-color: #f8fafc; color: #64748b; font-size: 11px; text-transform: uppercase; text-align: left; padding: 12px 15px; border-bottom: 1px solid #e2e8f0;">Metric</th>
                                        <th style="background-color: #f8fafc; color: #64748b; font-size: 11px; text-transform: uppercase; text-align: right; padding: 12px 15px; border-bottom: 1px solid #e2e8f0;">Value</th>
                                    </tr>
                                </thead>
                                <tbody>
                                    {html_rows}
                                </tbody>
                            </table>

                            <div style="font-size: 16px; font-weight: bold; margin: 25px 0 12px 0; color: #1e293b;">
                                <span style="margin-right: 8px;">🔍</span> Keyword Breakdown
                            </div>

                            <table style="width: 100%; border-collapse: collapse; margin-bottom: 20px;">
                                <thead>
                                    <tr>
                                        <th style="background-color: #f8fafc; color: #64748b; font-size: 11px; text-transform: uppercase; text-align: left; padding: 12px 15px; border-bottom: 1px solid #e2e8f0;">Keyword</th>
                                        <th style="background-color: #f8fafc; color: #64748b; font-size: 11px; text-transform: uppercase; text-align: center; padding: 12px 15px; border-bottom: 1px solid #e2e8f0;">Seen</th>
                                        <th style="background-color: #f8fafc; color: #64748b; font-size: 11px; text-transform: uppercase; text-align: center; padding: 12px 15px; border-bottom: 1px solid #e2e8f0;">Relevant</th>
                                        <th style="background-color: #f8fafc; color: #64748b; font-size: 11px; text-transform: uppercase; text-align: center; padding: 12px 15px; border-bottom: 1px solid #e2e8f0;">Extracted</th>
                                        <th style="background-color: #f8fafc; color: #64748b; font-size: 11px; text-transform: uppercase; text-align: center; padding: 12px 15px; border-bottom: 1px solid #e2e8f0;">Saved</th>
                                    </tr>
                                </thead>
                                <tbody>
                                    {keyword_rows}
                                </tbody>
                            </table>

                            {error_section}
                        </div>

                        <div style="text-align: center; padding: 25px 20px 10px 20px; font-size: 12px; color: #94a3b8; line-height: 1.6;">
                            Report generated by LinkedIn Posts Contract Extractor Bot.
                        </div>
                    </div>
                </body>
            </html>
            """
            
            # Generate subject
            subject = f"LinkedIn Posts Contract Extractor Bot Report - {datetime.now().strftime('%Y-%m-%d')}"
            if self.bot.candidate_id:
                subject += f" (Cand: {self.bot.candidate_id})"
            
            return subject, email_body
            
        except Exception as e:
            logger.error(f"Failed to generate report HTML: {e}", extra={"step_name": "BotReporter"}, exc_info=True)
            return None, None

class ConsolidatedBotReporter:
    """Handles consolidated email reporting for multiple bot runs."""
    def __init__(self, results_list):
        """
        results_list: List of dictionaries containing metrics for each candidate/run.
        Each dict should have: candidate_id, email, seen, relevant, saved, synced, 
        posts_disk, keywords, duration, and failed_reasons.
        """
        self.results = results_list
        self.server = config.SMTP_SERVER
        self.port = config.SMTP_PORT
        self.username = config.SMTP_USERNAME
        self.password = config.SMTP_PASSWORD
        self.email_from = config.EMAIL_FROM
        
        if config.EMAIL_TO:
            self.email_to = [email.strip() for email in config.EMAIL_TO.split(',')]
        else:
            self.email_to = []

    def _is_configured(self):
        config_values = {
            "server": self.server,
            "port": self.port,
            "username": self.username,
            "password": "set" if self.password else None,
            "from": self.email_from,
            "to": self.email_to
        }
        missing = [k for k, v in config_values.items() if not v]
        if missing:
            logger.info(f"SMTP consolidated configuration missing: {', '.join(missing)} - Reporting will be skipped.", extra={"step_name": "ConsolidatedBotReporter"})
            return False
        return True

    def send_consolidated_report(self):
        try:
            if not self.results:
                logger.warning("No results to report.", extra={"step_name": "ConsolidatedBotReporter"})
                return False

            subject, html_body = self._generate_consolidated_html_report()
            
            if not subject or not html_body:
                return False
            
            return self._send_email(subject, html_body)
        except Exception as e:
            logger.error(f"Failed to send consolidated report: {e}", extra={"step_name": "ConsolidatedBotReporter"}, exc_info=True)
            return False

    def _send_email(self, subject, html_body):
        if not self._is_configured():
            logger.warning("SMTP not configured. Skipping consolidated report.", extra={"step_name": "ConsolidatedBotReporter"})
            return True
            
        msg = MIMEMultipart()
        msg['From'] = self.email_from
        msg['To'] = ', '.join(self.email_to)
        msg['Subject'] = subject
        msg.attach(MIMEText(html_body, 'html'))
        
        try:
            with smtplib.SMTP(self.server, self.port) as server:
                server.starttls()
                server.login(self.username, self.password)
                server.sendmail(self.email_from, self.email_to, msg.as_string())
            logger.info(f"Consolidated report sent successfully to {len(self.email_to)} recipient(s).", extra={"step_name": "ConsolidatedBotReporter"})
            return True
        except Exception as e:
            logger.error(f"SMTP error: {e}", extra={"step_name": "ConsolidatedBotReporter"})
            return False

    def _generate_consolidated_html_report(self):
        try:
            total_runs = len(self.results)
            total_seen = sum(r.get('seen', 0) for r in self.results)
            total_relevant = sum(r.get('relevant', 0) for r in self.results)
            total_saved = sum(r.get('saved', 0) for r in self.results)
            total_synced = sum(r.get('synced', 0) for r in self.results)
            total_positions_found = sum(r.get('positions_found', 0) for r in self.results)
            total_positions_synced = sum(r.get('positions_synced', 0) for r in self.results)
            total_email_positions_synced = sum(r.get('email_positions_synced', 0) for r in self.results)
            date_time_str = datetime.now().strftime('%Y-%m-%d %H:%M:%S')

            summary_rows = ""
            for r in self.results:
                cand_id = html.escape(str(r.get('candidate_id', 'N/A')))
                email = html.escape(str(r.get('email', 'N/A')))
                keywords = html.escape(str(r.get('keywords', 'N/A')))
                seen = r.get('seen', 0)
                relevant = r.get('relevant', 0)
                saved = r.get('saved', 0)
                synced = r.get('synced', 0)
                pos_found = r.get('positions_found', 0)
                pos_synced = r.get('positions_synced', 0)
                email_pos_synced = r.get('email_positions_synced', 0)

                summary_rows += f"""
                <tr class="metric-row">
                    <td class="metric-name" style="padding: 10px 12px; border-bottom: 1px solid #f1f5f9; font-size: 13px; font-weight: 600; color: #1e293b;">{cand_id}</td>
                    <td style="padding: 10px 12px; border-bottom: 1px solid #f1f5f9; font-size: 12px; color: #64748b;"><a href="mailto:{email}" style="color: #3b82f6; text-decoration: none;">{email}</a></td>
                    <td style="padding: 10px 12px; border-bottom: 1px solid #f1f5f9; font-size: 12px; color: #64748b;">{keywords}</td>
                    <td style="padding: 10px 12px; border-bottom: 1px solid #f1f5f9; text-align: center; font-size: 13px; color: #64748b;">{seen}</td>
                    <td style="padding: 10px 12px; border-bottom: 1px solid #f1f5f9; text-align: center; font-size: 13px; font-weight: 600; color: #2563eb;">{relevant}</td>
                    <td style="padding: 10px 12px; border-bottom: 1px solid #f1f5f9; text-align: center; font-size: 13px; font-weight: 600; color: #16a34a;">{saved}</td>
                    <td style="padding: 10px 12px; border-bottom: 1px solid #f1f5f9; text-align: center; font-size: 13px; font-weight: 600; color: #ea580c;">{synced}</td>
                    <td style="padding: 10px 12px; border-bottom: 1px solid #f1f5f9; text-align: center; font-size: 13px; font-weight: 600; color: #0d9488;">{pos_found}</td>
                    <td style="padding: 10px 12px; border-bottom: 1px solid #f1f5f9; text-align: center; font-size: 13px; font-weight: 600; color: #2563eb;">{pos_synced}</td>
                    <td style="padding: 10px 12px; border-bottom: 1px solid #f1f5f9; text-align: center; font-size: 13px; font-weight: 600; color: #16a34a;">{email_pos_synced}</td>
                </tr>
                """

            html_body = f"""
            <html>
                <head>
                    <style>
                        body {{ font-family: 'Segoe UI', Tahoma, Geneva, Verdana, sans-serif; margin: 0; padding: 0; background-color: #f4f7f9; color: #333; }}
                        .header {{ background: linear-gradient(135deg, #1e4eb8 0%, #3b82f6 100%); color: white; padding: 35px 25px 45px 25px; text-align: left; border-radius: 8px 8px 0 0; }}
                        .header h1 {{ margin: 8px 0 4px 0; font-size: 24px; font-weight: bold; color: #ffffff; }}
                        .header p {{ margin: 0; font-size: 13px; opacity: 0.9; }}
                        .header .sub {{ font-size: 11px; font-weight: bold; text-transform: uppercase; letter-spacing: 1.5px; opacity: 0.85; }}
                        .content {{ padding: 30px; max-width: 900px; margin: 0 auto; background: white; border-radius: 8px; box-shadow: 0 4px 15px rgba(0,0,0,0.05); margin-top: -20px; }}
                        .intro-text {{ color: #64748b; font-size: 14px; line-height: 1.6; margin-top: 0; margin-bottom: 25px; }}
                        .section-title {{ font-size: 16px; font-weight: bold; margin: 25px 0 12px 0; display: flex; align-items: center; color: #1e293b; }}
                        table {{ width: 100%; border-collapse: collapse; margin-bottom: 20px; }}
                        th {{ background-color: #f8fafc; color: #64748b; font-size: 11px; text-transform: uppercase; text-align: left; padding: 12px 15px; border-bottom: 1px solid #e2e8f0; }}
                        td {{ padding: 12px 15px; font-size: 13px; border-bottom: 1px solid #f1f5f9; }}
                        .footer {{ text-align: center; padding: 25px 20px 10px 20px; font-size: 12px; color: #94a3b8; line-height: 1.6; }}
                    </style>
                </head>
                <body style="font-family: 'Segoe UI', Tahoma, Geneva, Verdana, sans-serif; margin: 0; padding: 0; background-color: #f4f7f9; color: #333;">
                    <div style="max-width: 900px; margin: 20px auto;">
                        <div style="background: linear-gradient(135deg, #1e4eb8 0%, #3b82f6 100%); color: white; padding: 35px 25px 45px 25px; text-align: left; border-radius: 8px 8px 0 0;">
                            <div style="font-size: 11px; font-weight: bold; text-transform: uppercase; letter-spacing: 1.5px; opacity: 0.85;">WHITEBOX LEARNING</div>
                            <h1 style="margin: 8px 0 4px 0; font-size: 24px; font-weight: bold; color: #ffffff;">LinkedIn Posts Extractor Bot Run Report</h1>
                            <p style="margin: 0; font-size: 13px; opacity: 0.9;">Run completed on {date_time_str} • {total_runs} Candidate Runs</p>
                        </div>

                        <div style="padding: 30px; background: #ffffff; border-radius: 8px; box-shadow: 0 4px 15px rgba(0,0,0,0.05); margin-top: -20px;">
                            <p style="color: #64748b; font-size: 14px; line-height: 1.6; margin-top: 0; margin-bottom: 25px;">
                                This report summarises the automated LinkedIn posts extraction workflow across all candidate runs. The run completed with the following overall metrics.
                            </p>

                            <div style="display: flex; gap: 10px; margin-bottom: 15px; flex-wrap: wrap;">
                                <div style="flex: 1; min-width: 100px; padding: 14px 12px; border-radius: 8px; text-align: center; background-color: #eff6ff; border: 1px solid #dbeafe; color: #1e40af;">
                                    <span style="font-size: 22px; font-weight: bold; display: block; margin-bottom: 4px; color: #1e40af;">{total_runs}</span>
                                    <span style="font-size: 9px; font-weight: bold; text-transform: uppercase; color: #64748b; letter-spacing: 0.5px;">TOTAL RUNS</span>
                                </div>
                                <div style="flex: 1; min-width: 100px; padding: 14px 12px; border-radius: 8px; text-align: center; background-color: #f0fdfa; border: 1px solid #ccfbf1; color: #115e59;">
                                    <span style="font-size: 22px; font-weight: bold; display: block; margin-bottom: 4px; color: #115e59;">{total_seen}</span>
                                    <span style="font-size: 9px; font-weight: bold; text-transform: uppercase; color: #64748b; letter-spacing: 0.5px;">POSTS SEEN</span>
                                </div>
                                <div style="flex: 1; min-width: 100px; padding: 14px 12px; border-radius: 8px; text-align: center; background-color: #eff6ff; border: 1px solid #dbeafe; color: #1e40af;">
                                    <span style="font-size: 22px; font-weight: bold; display: block; margin-bottom: 4px; color: #1e40af;">{total_relevant}</span>
                                    <span style="font-size: 9px; font-weight: bold; text-transform: uppercase; color: #64748b; letter-spacing: 0.5px;">RELEVANT POSTS</span>
                                </div>
                                <div style="flex: 1; min-width: 100px; padding: 14px 12px; border-radius: 8px; text-align: center; background-color: #f0fdf4; border: 1px solid #dcfce7; color: #166534;">
                                    <span style="font-size: 22px; font-weight: bold; display: block; margin-bottom: 4px; color: #166534;">{total_saved}</span>
                                    <span style="font-size: 9px; font-weight: bold; text-transform: uppercase; color: #64748b; letter-spacing: 0.5px;">CONTACTS FOUND</span>
                                </div>
                            </div>
                            <div style="display: flex; gap: 10px; margin-bottom: 25px; flex-wrap: wrap;">
                                <div style="flex: 1; min-width: 100px; padding: 14px 12px; border-radius: 8px; text-align: center; background-color: #fffbeb; border: 1px solid #fef3c7; color: #92400e;">
                                    <span style="font-size: 22px; font-weight: bold; display: block; margin-bottom: 4px; color: #92400e;">{total_synced}</span>
                                    <span style="font-size: 9px; font-weight: bold; text-transform: uppercase; color: #64748b; letter-spacing: 0.5px;">CONTACTS SYNCED</span>
                                </div>
                                <div style="flex: 1; min-width: 100px; padding: 14px 12px; border-radius: 8px; text-align: center; background-color: #f0fdfa; border: 1px solid #ccfbf1; color: #115e59;">
                                    <span style="font-size: 22px; font-weight: bold; display: block; margin-bottom: 4px; color: #115e59;">{total_positions_found}</span>
                                    <span style="font-size: 9px; font-weight: bold; text-transform: uppercase; color: #64748b; letter-spacing: 0.5px;">JOBS FOUND</span>
                                </div>
                                <div style="flex: 1; min-width: 100px; padding: 14px 12px; border-radius: 8px; text-align: center; background-color: #eff6ff; border: 1px solid #dbeafe; color: #1e40af;">
                                    <span style="font-size: 22px; font-weight: bold; display: block; margin-bottom: 4px; color: #1e40af;">{total_positions_synced}</span>
                                    <span style="font-size: 9px; font-weight: bold; text-transform: uppercase; color: #64748b; letter-spacing: 0.5px;">RAW SYNCED</span>
                                </div>
                                <div style="flex: 1; min-width: 100px; padding: 14px 12px; border-radius: 8px; text-align: center; background-color: #f0fdf4; border: 1px solid #dcfce7; color: #166534;">
                                    <span style="font-size: 22px; font-weight: bold; display: block; margin-bottom: 4px; color: #166534;">{total_email_positions_synced}</span>
                                    <span style="font-size: 9px; font-weight: bold; text-transform: uppercase; color: #64748b; letter-spacing: 0.5px;">EMAIL SYNCED</span>
                                </div>
                            </div>

                            <div style="font-size: 16px; font-weight: bold; margin: 25px 0 12px 0; color: #1e293b;">
                                <span style="margin-right: 8px;">📊</span> Total Summary
                            </div>

                            <table style="width: 100%; max-width: 600px; border-collapse: collapse; margin-bottom: 25px;">
                                <thead>
                                    <tr>
                                        <th style="width: 70%; background-color: #f8fafc; color: #64748b; font-size: 11px; text-transform: uppercase; text-align: left; padding: 12px 15px; border-bottom: 1px solid #e2e8f0;">Metric</th>
                                        <th style="text-align: right; background-color: #f8fafc; color: #64748b; font-size: 11px; text-transform: uppercase; padding: 12px 15px; border-bottom: 1px solid #e2e8f0;">Value</th>
                                    </tr>
                                </thead>
                                <tbody>
                                    <tr class="metric-row">
                                        <td class="metric-name" style="padding: 12px 15px; border-bottom: 1px solid #f1f5f9; color: #475569; font-size: 13px;">Total Runs</td>
                                        <td class="metric-val" style="padding: 12px 15px; border-bottom: 1px solid #f1f5f9; text-align: right; font-weight: 600; color: #1e293b; font-size: 13px;">{total_runs}</td>
                                    </tr>
                                    <tr class="metric-row">
                                        <td class="metric-name" style="padding: 12px 15px; border-bottom: 1px solid #f1f5f9; color: #475569; font-size: 13px;">Total Posts Seen</td>
                                        <td class="metric-val" style="padding: 12px 15px; border-bottom: 1px solid #f1f5f9; text-align: right; font-weight: 600; color: #1e293b; font-size: 13px;">{total_seen}</td>
                                    </tr>
                                    <tr class="metric-row">
                                        <td class="metric-name" style="padding: 12px 15px; border-bottom: 1px solid #f1f5f9; color: #475569; font-size: 13px;">Total Relevant Posts</td>
                                        <td class="metric-val" style="padding: 12px 15px; border-bottom: 1px solid #f1f5f9; text-align: right; font-weight: 600; color: #2563eb; font-size: 13px;">{total_relevant}</td>
                                    </tr>
                                    <tr class="metric-row">
                                        <td class="metric-name" style="padding: 12px 15px; border-bottom: 1px solid #f1f5f9; color: #475569; font-size: 13px;">Total Contacts Found</td>
                                        <td class="metric-val" style="padding: 12px 15px; border-bottom: 1px solid #f1f5f9; text-align: right; font-weight: 600; color: #16a34a; font-size: 13px;">{total_saved}</td>
                                    </tr>
                                    <tr class="metric-row">
                                        <td class="metric-name" style="padding: 12px 15px; border-bottom: 1px solid #f1f5f9; color: #475569; font-size: 13px;">Total Contacts Synced</td>
                                        <td class="metric-val" style="padding: 12px 15px; border-bottom: 1px solid #f1f5f9; text-align: right; font-weight: 600; color: #ea580c; font-size: 13px;">{total_synced}</td>
                                    </tr>
                                    <tr class="metric-row">
                                        <td class="metric-name" style="padding: 12px 15px; border-bottom: 1px solid #f1f5f9; color: #475569; font-size: 13px;">Total Jobs Identified</td>
                                        <td class="metric-val" style="padding: 12px 15px; border-bottom: 1px solid #f1f5f9; text-align: right; font-weight: 600; color: #0d9488; font-size: 13px;">{total_positions_found}</td>
                                    </tr>
                                    <tr class="metric-row">
                                        <td class="metric-name" style="padding: 12px 15px; border-bottom: 1px solid #f1f5f9; color: #475569; font-size: 13px;">Total Jobs Synced (Raw)</td>
                                        <td class="metric-val" style="padding: 12px 15px; border-bottom: 1px solid #f1f5f9; text-align: right; font-weight: 600; color: #2563eb; font-size: 13px;">{total_positions_synced}</td>
                                    </tr>
                                    <tr class="metric-row">
                                        <td class="metric-name" style="padding: 12px 15px; border-bottom: 1px solid #f1f5f9; color: #475569; font-size: 13px;">Total Jobs Synced (Email)</td>
                                        <td class="metric-val" style="padding: 12px 15px; border-bottom: 1px solid #f1f5f9; text-align: right; font-weight: 600; color: #16a34a; font-size: 13px;">{total_email_positions_synced}</td>
                                    </tr>
                                </tbody>
                            </table>

                            <div style="font-size: 16px; font-weight: bold; margin: 25px 0 12px 0; color: #1e293b;">
                                <span style="margin-right: 8px;">👤</span> Candidate Breakdown
                            </div>

                            <div style="overflow-x: auto;">
                                <table style="width: 100%; border-collapse: collapse; margin-bottom: 20px;">
                                    <thead>
                                        <tr>
                                            <th style="background-color: #f8fafc; color: #64748b; font-size: 11px; text-transform: uppercase; text-align: left; padding: 12px 10px; border-bottom: 1px solid #e2e8f0;">Candidate ID</th>
                                            <th style="background-color: #f8fafc; color: #64748b; font-size: 11px; text-transform: uppercase; text-align: left; padding: 12px 10px; border-bottom: 1px solid #e2e8f0;">Email</th>
                                            <th style="background-color: #f8fafc; color: #64748b; font-size: 11px; text-transform: uppercase; text-align: left; padding: 12px 10px; border-bottom: 1px solid #e2e8f0;">Keywords</th>
                                            <th style="background-color: #f8fafc; color: #64748b; font-size: 11px; text-transform: uppercase; text-align: center; padding: 12px 10px; border-bottom: 1px solid #e2e8f0;">Seen</th>
                                            <th style="background-color: #f8fafc; color: #64748b; font-size: 11px; text-transform: uppercase; text-align: center; padding: 12px 10px; border-bottom: 1px solid #e2e8f0;">Relevant</th>
                                            <th style="background-color: #f8fafc; color: #64748b; font-size: 11px; text-transform: uppercase; text-align: center; padding: 12px 10px; border-bottom: 1px solid #e2e8f0;">Contacts</th>
                                            <th style="background-color: #f8fafc; color: #64748b; font-size: 11px; text-transform: uppercase; text-align: center; padding: 12px 10px; border-bottom: 1px solid #e2e8f0;">Synced</th>
                                            <th style="background-color: #f8fafc; color: #64748b; font-size: 11px; text-transform: uppercase; text-align: center; padding: 12px 10px; border-bottom: 1px solid #e2e8f0;">Jobs Found</th>
                                            <th style="background-color: #f8fafc; color: #64748b; font-size: 11px; text-transform: uppercase; text-align: center; padding: 12px 10px; border-bottom: 1px solid #e2e8f0;">Raw Synced</th>
                                            <th style="background-color: #f8fafc; color: #64748b; font-size: 11px; text-transform: uppercase; text-align: center; padding: 12px 10px; border-bottom: 1px solid #e2e8f0;">Email Synced</th>
                                        </tr>
                                    </thead>
                                    <tbody>
                                        {summary_rows}
                                        <tr style="background-color: #f8fafc; font-weight: bold; border-top: 2px solid #e2e8f0;">
                                            <td style="padding: 10px 12px; color: #111; font-size: 13px;">TOTAL</td>
                                            <td style="padding: 10px 12px; color: #64748b; font-size: 11px;">-</td>
                                            <td style="padding: 10px 12px; color: #64748b; font-size: 11px;">-</td>
                                            <td style="padding: 10px 12px; text-align: center; font-size: 13px; color: #64748b;">{total_seen}</td>
                                            <td style="padding: 10px 12px; text-align: center; font-size: 13px; color: #2563eb;">{total_relevant}</td>
                                            <td style="padding: 10px 12px; text-align: center; font-size: 13px; color: #16a34a;">{total_saved}</td>
                                            <td style="padding: 10px 12px; text-align: center; font-size: 13px; color: #ea580c;">{total_synced}</td>
                                            <td style="padding: 10px 12px; text-align: center; font-size: 13px; color: #0d9488;">{total_positions_found}</td>
                                            <td style="padding: 10px 12px; text-align: center; font-size: 13px; color: #2563eb;">{total_positions_synced}</td>
                                            <td style="padding: 10px 12px; text-align: center; font-size: 13px; color: #16a34a;">{total_email_positions_synced}</td>
                                        </tr>
                                    </tbody>
                                </table>
                            </div>
                        </div>

                        <div style="text-align: center; padding: 25px 20px 10px 20px; font-size: 12px; color: #94a3b8; line-height: 1.6;">
                            This is an automated report from the LinkedIn Posts Bot.
                        </div>
                    </div>
                </body>
            </html>
            """
            
            subject = f"LinkedIn Posts Contract Extractor Bot Report - {total_runs} Runs - {datetime.now().strftime('%Y-%m-%d')}"
            return subject, html_body
        except Exception as e:
            logger.error(f"Error generating consolidated HTML: {e}", extra={"step_name": "ConsolidatedBotReporter"})
            return None, None
