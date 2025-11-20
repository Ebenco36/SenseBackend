# resources/contact_resource.py
from flask_restful import Resource, reqparse
from flask_mail import Message, Mail
from flask import current_app
import logging
import re
from datetime import datetime

class ContactFormResource(Resource):
    """Self-contained contact form resource that handles email sending"""
    
    def __init__(self):
        """Initialize parser with validation rules"""
        self.parser = reqparse.RequestParser()
        self.parser.add_argument(
            'name', 
            type=str, 
            required=True, 
            help="Name is required",
            trim=True
        )
        self.parser.add_argument(
            'email', 
            type=str, 
            required=True, 
            help="Email is required",
            trim=True
        )
        self.parser.add_argument(
            'subject', 
            type=str, 
            required=True, 
            help="Subject is required",
            trim=True
        )
        self.parser.add_argument(
            'message', 
            type=str, 
            required=True, 
            help="Message is required",
            trim=True
        )
        
        super(ContactFormResource, self).__init__()
    
    def _get_mail_instance(self):
        """Get mail instance from current app extensions"""
        return current_app.extensions.get('mail')
    
    def _validate_email(self, email):
        """Validate email format"""
        pattern = r'^[^\s@]+@[^\s@]+\.[^\s@]+$'
        return re.match(pattern, email) is not None
    
    def _validate_message_length(self, message):
        """Validate minimum message length"""
        return len(message.strip()) >= 10
    
    def post(self):
        """
        Handle POST request to send contact form via email
        
        Expected JSON:
        {
            "name": "string",
            "email": "string",
            "subject": "string",
            "message": "string"
        }
        
        Returns:
        {
            "success": bool,
            "message": "string"
        }
        """
        try:
            # Parse and validate arguments
            args = self.parser.parse_args()
            
            name = args['name'].strip()
            email = args['email'].strip()
            subject = args['subject'].strip()
            message = args['message'].strip()
            
            # Additional validation
            if not self._validate_email(email):
                return {
                    "success": False, 
                    "error": "Invalid email format"
                }, 400
            
            if not self._validate_message_length(message):
                return {
                    "success": False, 
                    "error": "Message must be at least 10 characters"
                }, 400
            
            # Get mail instance from app
            mail = self._get_mail_instance()
            if not mail:
                raise Exception("Mail extension not initialized")
            
            # Get recipient email from config
            recipient_email = current_app.config.get(
                'CONTACT_RECIPIENT_EMAIL', 
                'your-email@example.com'
            )
            
            # Create email message
            msg = Message(
                subject=f"📧 Contact Form: {subject}",
                sender=email,
                recipients=[recipient_email],
                reply_to=email
            )
            
            # Professional HTML email template
            msg.html = self._generate_html_email(name, email, subject, message)
            
            # Plain text fallback
            msg.body = self._generate_text_email(name, email, subject, message)
            
            # Send email
            mail.send(msg)
            
            # Log successful submission
            logging.info(
                f"✅ Contact form submitted - "
                f"Name: {name}, Email: {email}, Subject: {subject}"
            )
            
            return {
                "success": True,
                "message": "Your message has been sent successfully! We'll get back to you soon."
            }, 200
            
        except Exception as e:
            logging.error(f"❌ Contact form error: {str(e)}", exc_info=True)
            return {
                "success": False,
                "error": "Failed to send message. Please try again later."
            }, 500
    
    def _generate_html_email(self, name, email, subject, message):
        """Generate professional HTML email template"""
        timestamp = datetime.now().strftime('%A, %B %d, %Y at %I:%M %p UTC')
        
        return f"""
        <!DOCTYPE html>
        <html>
        <head>
            <style>
                body {{
                    font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, Arial, sans-serif;
                    line-height: 1.6;
                    color: #333;
                    margin: 0;
                    padding: 0;
                    background-color: #f5f5f5;
                }}
                .container {{
                    max-width: 600px;
                    margin: 20px auto;
                    background-color: #ffffff;
                    border-radius: 12px;
                    overflow: hidden;
                    box-shadow: 0 4px 12px rgba(0, 0, 0, 0.1);
                }}
                .header {{
                    background: linear-gradient(135deg, #153a9d 0%, #1e4db7 100%);
                    color: white;
                    padding: 30px;
                    text-align: center;
                }}
                .header h1 {{
                    margin: 0;
                    font-size: 24px;
                    font-weight: 600;
                }}
                .content {{
                    padding: 30px;
                }}
                .field {{
                    margin-bottom: 20px;
                    padding-bottom: 15px;
                    border-bottom: 1px solid #e0e0e0;
                }}
                .field:last-child {{
                    border-bottom: none;
                }}
                .label {{
                    font-weight: 600;
                    color: #153a9d;
                    font-size: 13px;
                    text-transform: uppercase;
                    letter-spacing: 0.5px;
                    margin-bottom: 5px;
                }}
                .value {{
                    color: #555;
                    font-size: 15px;
                    margin-top: 5px;
                    white-space: pre-wrap;
                    word-wrap: break-word;
                }}
                .email-link {{
                    color: #153a9d;
                    text-decoration: none;
                }}
                .email-link:hover {{
                    text-decoration: underline;
                }}
                .message-box {{
                    background-color: #f9f9f9;
                    padding: 15px;
                    border-radius: 8px;
                    border-left: 4px solid #153a9d;
                }}
                .footer {{
                    background-color: #f5f5f5;
                    text-align: center;
                    padding: 20px;
                    font-size: 12px;
                    color: #999;
                }}
                .reply-button {{
                    display: inline-block;
                    margin-top: 20px;
                    padding: 12px 24px;
                    background-color: #153a9d;
                    color: white;
                    text-decoration: none;
                    border-radius: 6px;
                    font-weight: 600;
                }}
            </style>
        </head>
        <body>
            <div class="container">
                <div class="header">
                    <h1>📬 New Contact Form Message</h1>
                </div>
                <div class="content">
                    <div class="field">
                        <div class="label">From</div>
                        <div class="value">{name}</div>
                    </div>
                    
                    <div class="field">
                        <div class="label">Email Address</div>
                        <div class="value">
                            <a href="mailto:{email}" class="email-link">{email}</a>
                        </div>
                    </div>
                    
                    <div class="field">
                        <div class="label">Subject</div>
                        <div class="value">{subject}</div>
                    </div>
                    
                    <div class="field">
                        <div class="label">Message</div>
                        <div class="value message-box">{message}</div>
                    </div>
                    
                    <div class="field">
                        <div class="label">Submitted At</div>
                        <div class="value">{timestamp}</div>
                    </div>
                    
                    <center>
                        <a href="mailto:{email}?subject=Re: {subject}" class="reply-button">
                            Reply to {name}
                        </a>
                    </center>
                </div>
                <div class="footer">
                    This message was sent via the contact form on your Research Platform<br>
                    <strong>Do not reply to this email directly</strong> - Use the button above to respond
                </div>
            </div>
        </body>
        </html>
        """
    
    def _generate_text_email(self, name, email, subject, message):
        """Generate plain text email fallback"""
        timestamp = datetime.now().strftime('%Y-%m-%d %H:%M:%S UTC')
        
        return f"""
            ═══════════════════════════════════════
                    NEW CONTACT FORM SUBMISSION
            ═══════════════════════════════════════

            FROM: {name}
            EMAIL: {email}
            SUBJECT: {subject}

            MESSAGE:
            {message}

            ───────────────────────────────────────
            Submitted: {timestamp}

            Reply to: {email}
            ═══════════════════════════════════════
        """
