import requests
import duckdb
import json
import smtplib
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText

"""EMail was sent to Valeria Rosati :)"""

CON = duckdb.connect("../data/db_src.duckdb")
CURRENT_DATE = "2024-04-30"  # according to task
SUBJECT = "All contacts with HIGH buying intent AND a valid email address - outreach more than 14 days ago"


def create_payload():
    out_reach = CON.sql("SELECT * FROM crm.outreach_emails")
    valid_emails = CON.sql("SELECT * FROM crm.valid_emails WHERE is_email_valid = 'true'")
    out_reach_valid_emails = CON.sql(
    """
    SELECT * FROM (
    SELECT o.*,
    CAST(JULIAN(CAST(CURRENT_DATE AS DATE)) - JULIAN(o.sent_date_utc) AS INTEGER) AS days
    FROM out_reach as o JOIN valid_emails as v ON o.to = v.email WHERE o.subject NOT LIKE 'Re:%')
    WHERE days > 14
    """
    )
    
    payload = {
    "from": "test@gmail.com",  # valid email address
    "to": "receiver@company.com",  # valid email address
    "message": [row[0] for row in out_reach_valid_emails.to.fetchall()],
    "subject": [row[0] for row in out_reach_valid_emails.subject.fetchall()]
    }
    
    return payload


def send_mail_via_gmail(payload):
    smtp_server = "smtp.gmail.com"
    smtp_port = 587
    username = payload["from"]
    password = "......."  # Valid password
    
    if isinstance(payload["message"], list):
        payload["message"] = ", ".join(payload["message"])
    if isinstance(payload["subject"], list):
        payload["subject"] = ", ".join(payload["subject"])
    json_payload = json.dumps(payload, indent=4)

    msg = MIMEMultipart()
    msg['From'] = payload["from"]
    msg['To'] = payload["to"]
    msg['Subject'] = SUBJECT
    
    msg.attach(MIMEText(json_payload, 'plain'))

    try:
        server = smtplib.SMTP(smtp_server, smtp_port)
        server.starttls() 
        server.login(username, password) 

        server.sendmail(payload["from"], payload["to"], msg.as_string())
        print("Email sent successfully!")
    except Exception as e:
        print(f"Failed to send email: {e}")
    finally:
        server.quit()
    

def main():
    
    payload = create_payload()
    send_mail_via_gmail(payload)
    CON.close()
   
if __name__ == "__main__":
    main()