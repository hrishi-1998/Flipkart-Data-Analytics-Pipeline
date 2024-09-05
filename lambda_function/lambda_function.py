import boto3
import json
from datetime import datetime

def send_email_notification(sns_client, SNS_TOPIC_ARN, glue_job_name):
    timestamp = datetime.utcnow().strftime("%d %b %Y at %H:%M hrs")

    # Message for the email notification
    message = f"A Glue job named '{glue_job_name}' was triggered on the arrival of new sales data in your data lake on {timestamp}"

    # Publish to SNS
    sns_client.publish(
        TopicArn=SNS_TOPIC_ARN,
        Message=message,
        Subject=f"Glue Job Triggered: {glue_job_name}"
    )

def lambda_handler(event, context):
    glue_client = boto3.client('glue')
    sns_client = boto3.client('sns')

    glue_job_name = 'Sales_Glue_Job'
    try:
        # Start the Glue job
        response = glue_client.start_job_run(JobName=glue_job_name)
        run_id = response['JobRunId']

        print(f"Glue job {glue_job_name} started successfully. Run ID: {run_id}")

        # Send email notification
        send_email_notification(sns_client, SNS_TOPIC_ARN, glue_job_name)

    except Exception as e:
        print(f"Error starting Glue job {glue_job_name}: {e}")
