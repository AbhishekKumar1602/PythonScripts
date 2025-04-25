import smtplib
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from email.mime.base import MIMEBase
from email import encoders
import pyodbc
import os
import logging
from concurrent.futures import ThreadPoolExecutor, as_completed
from contextlib import contextmanager

# Load Environment Variables Using .env File
connection_string = os.getenv("DB_CONNECTION_STRING")
smtp_host = os.getenv("SMTP_HOST")
smtp_port = int(os.getenv("SMTP_PORT", 25))  # Default port 25
smtp_user = os.getenv("SMTP_USER")
smtp_pass = os.getenv("SMTP_PASS")
from_address = os.getenv("FROM_ADDRESS", "noreply@example.com")
from_name = os.getenv("FROM_NAME", "Your Name")

# Configure Logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# SQL Queries
FETCH_EMAILS_QUERY = """
    SELECT ApplicationNo, RollNo, CandidateName, Email, AttachmentPath, bar
    FROM YourTable
    WHERE SomeCondition = ?
"""
UPDATE_STATUS_QUERY = """
    UPDATE YourTable
    SET EmailStatus = ?
    WHERE ApplicationNo = ? AND RollNo = ?
"""

@contextmanager
def db_connection():
    conn = pyodbc.connect(connection_string)
    try:
        yield conn.cursor()
    finally:
        conn.close()

def fetch_emails():
    email_details_list = []
    with db_connection() as cursor:
        try:
            cursor.execute(FETCH_EMAILS_QUERY, ('condition_value',))  # Use proper condition here
            rows = cursor.fetchall()
            for row in rows:
                email_details = {
                    'ApplicationNo': row.ApplicationNo,
                    'RollNo': row.RollNo,
                    'CandidateName': row.CandidateName,
                    'Email': row.Email,
                    'AttachmentPath': row.AttachmentPath,
                    'bar': row.bar
                }
                email_details_list.append(email_details)
        except Exception as e:
            logging.error(f"Error fetching emails: {e}")
    return email_details_list

def attach_file(msg, attachment_path):
    try:
        with open(attachment_path, "rb") as attachment:
            part = MIMEBase('application', 'octet-stream')
            part.set_payload(attachment.read())
            encoders.encode_base64(part)
            part.add_header('Content-Disposition', f"attachment; filename= {os.path.basename(attachment_path)}")
            msg.attach(part)
    except FileNotFoundError:
        logging.error(f"Attachment Not Found: {attachment_path}")

def send_email(to_address, subject, body, attachment_path=None):
    try:
        msg = MIMEMultipart()
        msg['From'] = from_address
        msg['To'] = to_address
        msg['Subject'] = subject

        msg.attach(MIMEText(body, 'html'))

        if attachment_path:
            attach_file(msg, attachment_path)

        with smtplib.SMTP(smtp_host, smtp_port) as server:
            server.starttls()  # Use if TLS is required
            server.login(smtp_user, smtp_pass)
            server.sendmail(from_address, to_address, msg.as_string())
            logging.info(f'Mail Sent to {to_address}')
            return True

    except Exception as e:
        logging.error(f"Error Sending Email to {to_address}: {e}")
        return False

def update_email_status(app_no, roll_no, bar):
    with db_connection() as cursor:
        try:
            cursor.execute(UPDATE_STATUS_QUERY, ('Sent', app_no, roll_no, bar))
            cursor.connection.commit()
            logging.info(f"Email status updated for ApplicationNo {app_no}")
        except Exception as e:
            logging.error(f"Error updating email status for ApplicationNo {app_no}: {e}")

def send_email_batch(email_details_list):
    for email_details in email_details_list:
        subject = "Your Subject Here"
        body = f"""
        Dear {email_details['CandidateName']},<br><br>
        Your custom email body here.
        """
        attachment_path = email_details['AttachmentPath']
        email_sent = send_email(email_details['Email'], subject, body, attachment_path)

        if email_sent:
            update_email_status(email_details['ApplicationNo'], email_details['RollNo'], email_details['bar'])

def process_batches(email_details_list, batch_size, worker_threads):
    batches = [email_details_list[i:i + batch_size] for i in range(0, len(email_details_list), batch_size)]
    with ThreadPoolExecutor(max_workers=worker_threads) as executor:
        futures = [executor.submit(send_email_batch, batch) for batch in batches]
        for future in as_completed(futures):
            try:
                future.result()
            except Exception as e:
                logging.error(f"Batch processing failed: {e}")

if __name__ == "__main__":
    email_details_list = fetch_emails()
    batch_size = 50  # Adjust as needed
    worker_threads = 25  # Adjust as needed
    process_batches(email_details_list, batch_size, worker_threads)
    print('All emails sent')
