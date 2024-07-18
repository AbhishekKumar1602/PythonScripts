import smtplib
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from email.mime.base import MIMEBase
from email import encoders
import pyodbc
import os
import logging
from concurrent.futures import ThreadPoolExecutor

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

connection_string = "Your Database Connection String"
smtp_host = "smtp.your_smtp_host.com"
smtp_port = 25
smtp_user = "your_smtp_username"
smtp_pass = "your_smtp_password"
from_address = "noreply@example.com"
from_name = "Your Name"

def create_connection():
    return pyodbc.connect(connection_string)

def fetch_emails():
    conn = create_connection()
    cursor = conn.cursor()
    query = "SQL Query to Fetch Details Like Application Number, Roll Number, Candidate Name, Email ID, Attachment Path"
    email_details_list = []
    try:
        cursor.execute(query)
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
    finally:
        cursor.close()
        conn.close()
    return email_details_list

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
            server.login(smtp_user, smtp_pass)
            server.sendmail(from_address, to_address, msg.as_string())
            logging.info(f'Mail Sent to {to_address}')
            return True

    except Exception as e:
        logging.error(f"Error Sending Email to {to_address}: {e}")
        return False

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

def update_email_status(app_no, roll_no, bar):
    conn = create_connection()
    cursor = conn.cursor()
    update_query = "SQL Query to Update Email Status"
    try:
        cursor.execute(update_query, (app_no, roll_no, bar))
        conn.commit()
    except Exception as e:
        logging.error(f"Error updating email status for ApplicationNo {app_no}: {e}")
    finally:
        cursor.close()
        conn.close()

def process_batches(email_details_list, batch_size, worker_threads):
    batches = [email_details_list[i:i + batch_size] for i in range(0, len(email_details_list), batch_size)]
    with ThreadPoolExecutor(max_workers=worker_threads) as executor:
        for batch in batches:
            executor.submit(send_email_batch, batch)

def send_email_batch(email_details_list):
    for email_details in email_details_list:
        subject = f"Your Subject Here"
        body = f'''
        Dear {email_details['CandidateName']},<br><br>
        Your custom email body here.
        '''
        attachment_path = email_details['AttachmentPath']
        email_sent = send_email(email_details['Email'], subject, body, attachment_path)

        if email_sent:
            update_email_status(email_details['ApplicationNo'], email_details['RollNo'], email_details['bar'])

if __name__ == "__main__":
    email_details_list = fetch_emails()
    batch_size = 50  # Adjust as needed
    worker_threads = 25  # Adjust as needed
    process_batches(email_details_list, batch_size, worker_threads)
    print('All emails sent')
