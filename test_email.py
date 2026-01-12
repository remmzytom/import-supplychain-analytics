"""
Test script to verify email notification functionality
Run this to test if email configuration is correct
"""

import os
import sys
import smtplib
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from datetime import datetime
import traceback

# Email configuration (from environment variables)
EMAIL_FROM = os.getenv('EMAIL_FROM')
EMAIL_TO = os.getenv('EMAIL_TO')
EMAIL_PASSWORD = os.getenv('EMAIL_PASSWORD')
SMTP_SERVER = os.getenv('SMTP_SERVER', 'smtp.gmail.com')
SMTP_PORT = int(os.getenv('SMTP_PORT') or '587')

def test_email():
    """Test email sending functionality"""
    print("=" * 60)
    print("EMAIL CONFIGURATION TEST")
    print("=" * 60)
    
    # Check configuration
    print(f"\nEmail Configuration:")
    print(f"  EMAIL_FROM: {'✓ Set' if EMAIL_FROM else '✗ Not set'}")
    print(f"  EMAIL_TO: {'✓ Set' if EMAIL_TO else '✗ Not set'}")
    print(f"  EMAIL_PASSWORD: {'✓ Set' if EMAIL_PASSWORD else '✗ Not set'}")
    print(f"  SMTP_SERVER: {SMTP_SERVER}")
    print(f"  SMTP_PORT: {SMTP_PORT}")
    
    if not EMAIL_FROM or not EMAIL_TO or not EMAIL_PASSWORD:
        print("\n❌ ERROR: Email configuration is incomplete!")
        print("Please set the following environment variables:")
        print("  - EMAIL_FROM: Your email address")
        print("  - EMAIL_TO: Recipient email address")
        print("  - EMAIL_PASSWORD: Your email app password")
        print("  - SMTP_SERVER: SMTP server (default: smtp.gmail.com)")
        print("  - SMTP_PORT: SMTP port (default: 587)")
        return False
    
    print("\n" + "=" * 60)
    print("ATTEMPTING TO SEND TEST EMAIL")
    print("=" * 60)
    
    try:
        # Create message
        print("\n1. Creating email message...")
        msg = MIMEMultipart()
        msg['From'] = EMAIL_FROM
        msg['To'] = EMAIL_TO
        msg['Subject'] = "Test Email - Freight Import Data Pipeline"
        
        body = f"""
This is a test email from the Freight Import Data Pipeline automation.

Timestamp: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}

If you received this email, your email configuration is working correctly!

---
This is an automated test message.
        """
        
        msg.attach(MIMEText(body, 'plain'))
        print("   ✓ Message created")
        
        # Connect to SMTP server
        print(f"\n2. Connecting to SMTP server ({SMTP_SERVER}:{SMTP_PORT})...")
        server = smtplib.SMTP(SMTP_SERVER, SMTP_PORT, timeout=30)
        print("   ✓ Connected to SMTP server")
        
        # Start TLS
        print("\n3. Starting TLS encryption...")
        server.starttls()
        print("   ✓ TLS started")
        
        # Login
        print("\n4. Logging in to SMTP server...")
        server.login(EMAIL_FROM, EMAIL_PASSWORD)
        print("   ✓ Login successful")
        
        # Send message
        print("\n5. Sending email message...")
        server.send_message(msg)
        print("   ✓ Message sent")
        
        # Close connection
        print("\n6. Closing SMTP connection...")
        server.quit()
        print("   ✓ Connection closed")
        
        print("\n" + "=" * 60)
        print("✅ SUCCESS: Test email sent successfully!")
        print("=" * 60)
        print(f"\nCheck your inbox at: {EMAIL_TO}")
        print("If you don't see the email, check your spam folder.")
        
        return True
        
    except smtplib.SMTPAuthenticationError as e:
        print("\n" + "=" * 60)
        print("❌ AUTHENTICATION ERROR")
        print("=" * 60)
        print(f"Error: {str(e)}")
        print("\nPossible causes:")
        print("  1. Incorrect email address or password")
        print("  2. For Gmail: You need to use an 'App Password', not your regular password")
        print("     - Go to: https://myaccount.google.com/apppasswords")
        print("     - Generate an app password for 'Mail'")
        print("     - Use that app password in EMAIL_PASSWORD")
        print("  3. 'Less secure app access' might need to be enabled (for non-Gmail)")
        return False
        
    except smtplib.SMTPConnectError as e:
        print("\n" + "=" * 60)
        print("❌ CONNECTION ERROR")
        print("=" * 60)
        print(f"Error: {str(e)}")
        print("\nPossible causes:")
        print("  1. Incorrect SMTP server address")
        print("  2. Incorrect SMTP port")
        print("  3. Firewall blocking SMTP connection")
        print("  4. Network connectivity issues")
        return False
        
    except smtplib.SMTPException as e:
        print("\n" + "=" * 60)
        print("❌ SMTP ERROR")
        print("=" * 60)
        print(f"Error Type: {type(e).__name__}")
        print(f"Error Message: {str(e)}")
        print("\nFull traceback:")
        traceback.print_exc()
        return False
        
    except Exception as e:
        print("\n" + "=" * 60)
        print("❌ UNEXPECTED ERROR")
        print("=" * 60)
        print(f"Error Type: {type(e).__name__}")
        print(f"Error Message: {str(e)}")
        print("\nFull traceback:")
        traceback.print_exc()
        return False


if __name__ == "__main__":
    success = test_email()
    sys.exit(0 if success else 1)

