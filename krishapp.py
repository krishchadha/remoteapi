import requests
import boto3
from datetime import datetime
import time

sns_client = boto3.client('sns', region_name='ap-south-1')
s3_client = boto3.client('s3')
API_URL = 'https://jsonplaceholder.typicode.com/posts'
SNS_TOPIC_ARN = "arn:aws:sns:ap-south-1:980921721207:krish-sns-topic"  
S3_BUCKET_NAME = "krish-logs-assignment" 

def fetch_messages_from_api():
    response = requests.get(API_URL)
    if response.status_code == 200:
        print("Sucess")
        return response.json()
    else:
        print(f"Failed with Status Code: {response.status_code}")
        return []
        

def publish_message_to_sns(message):
    try:
        response = sns_client.publish(
            TopicArn=SNS_TOPIC_ARN,
            Message=str(message)
        )
        print(f"Message to SNS: {response['MessageId']}")
    except Exception as e:
        print(f"Failed SNS")


def log_message_to_s3(message):
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S_%f")
    log_file_name = f'logs/log_{timestamp}.txt'
    
    try:
        s3_client.put_object(
            Bucket=S3_BUCKET_NAME,
            Key=log_file_name,
            Body=str(message)
        )
        print("Saved on the S3")
    except Exception as e:
        print("Failed on S3")


def main():
    while True:
      messages = fetch_messages_from_api()
      for message in messages:
            publish_message_to_sns(message)
            log_message_to_s3(message)
            time.sleep(1)
            

if __name__ == "__main__":
    main()
