import os
import json
import csv
import boto3
from datetime import datetime, timedelta, timezone
import urllib.request

# Initialize AWS clients
s3 = boto3.client("s3")
sns = boto3.client("sns")

# Load inventory from S3
def load_inventory_from_s3(bucket_name, file_key):
    try:
        print(f"Loading inventory from bucket: {bucket_name}, key: {file_key}")
        obj = s3.get_object(Bucket=bucket_name, Key=file_key)
        content = obj['Body'].read().decode('utf-8').splitlines()
        
        # Parse CSV with DictReader
        reader = csv.DictReader(content)
        inventory = []
        for row in reader:
            # Normalize keys and skip rows with missing DESCRIPTION or BRAND
            normalized_row = {key.strip().upper(): value.strip() for key, value in row.items()}
            if not normalized_row.get("DESCRIPTION") or not normalized_row.get("BRAND"):
                print(f"Skipping invalid row: {normalized_row}")
                continue
            inventory.append(normalized_row)

        # Check if inventory is empty
        if not inventory:
            print("Inventory is empty after filtering invalid rows.")
            return []

        # Log column headers and sample data safely
        print(f"Inventory columns: {list(inventory[0].keys())}")
        print(f"Sample data: {inventory[:3]}")
        return inventory
    except Exception as e:
        print(f"Error loading inventory from S3: {e}")
        return []

# Fetch FDA recall data
def fetch_fda_recall_data():
    utc_now = datetime.now(timezone.utc)
    two_weeks_ago = utc_now - timedelta(days=14)
    start_date = two_weeks_ago.strftime("%Y-%m-%d")
    end_date = utc_now.strftime("%Y-%m-%d")
    
    api_url = f"https://api.fda.gov/food/enforcement.json?search=report_date:[{start_date}+TO+{end_date}]"
    print(f"Fetching FDA recall data from: {api_url}")
    
    try:
        with urllib.request.urlopen(api_url, timeout=10) as response:
            data = json.loads(response.read().decode())
            recall_data = data.get("results", [])
            print(f"Fetched {len(recall_data)} recall records.")
            return recall_data
    except Exception as e:
        print(f"Error fetching FDA recall data: {e}")
        return []

# Compare recall data to inventory
def find_recalled_inventory(recall_data, inventory_data):
    matches = []
    for recall in recall_data:
        for item in inventory_data:
            if (
                item["DESCRIPTION"].lower() in recall["product_description"].lower()
                or item["BRAND"].lower() in recall["recalling_firm"].lower()
            ):
                matches.append({
                    "inventory_item": item,
                    "recall_details": recall
                })
    return matches

# Publish SNS message
def publish_sns_message(sns_topic_arn, subject, message):
    try:
        # Add header to the message
        header = "Weekly FDA Recall Alert: Current Inventory Contains Recalled Items\n\n"
        full_message = header + message

        # Publish the message
        sns.publish(TopicArn=sns_topic_arn, Message=full_message, Subject=subject)
        print(f"SNS message published successfully: {subject}")
    except Exception as e:
        print(f"Error publishing to SNS: {e}")
# Lambda handler
def lambda_handler(event, context):
    # Load environment variables
    s3_bucket = os.getenv("S3_BUCKET")
    s3_key = os.getenv("S3_KEY")
    sns_topic_arn = os.getenv("SNS_TOPIC_ARN")
    
    if not (s3_bucket and s3_key and sns_topic_arn):
        return {"statusCode": 500, "body": "Environment variables are missing"}

    # Load inventory data from S3
    inventory_data = load_inventory_from_s3(s3_bucket, s3_key)
    if not inventory_data:
        return {"statusCode": 500, "body": "Failed to load inventory data"}

    # Fetch FDA recall data
    recall_data = fetch_fda_recall_data()
    if not recall_data:
        publish_sns_message(sns_topic_arn, "FDA Recall Updates", "No FDA recall data available.")
        return {"statusCode": 200, "body": "No FDA recall data available"}

    # Find recalls in inventory
    recalled_items = find_recalled_inventory(recall_data, inventory_data)
    if recalled_items:
        matches_message = [
            f"INVENTORY MATCH:\n"
            f"NUM: {item['inventory_item']['NUM']}\n"
            f"Item #: {item['inventory_item']['ITEM #']}\n"
            f"Category: {item['inventory_item']['CATEGORY']}\n"
            f"Description: {item['inventory_item']['DESCRIPTION']}\n"
            f"Brand: {item['inventory_item']['BRAND']}\n"
            f"Recall Number: {item['recall_details'].get('recall_number', 'Unknown')}\n"
            f"Lot Number: {item['recall_details'].get('code_info', 'Unknown')}\n"
            f"Recall Reason: {item['recall_details']['reason_for_recall']}\n"
            f"Recall Date: {item['recall_details'].get('recall_initiation_date', 'Unknown')}\n"
            "---"
            for item in recalled_items
        ]
        match_message = "\n\n".join(matches_message)
        publish_sns_message(sns_topic_arn, "FDA Recall Matches in Inventory", match_message)
    else:
        publish_sns_message(sns_topic_arn, "FDA Recall Matches in Inventory", "No current FDA recalls in inventory.")

    return {"statusCode": 200, "body": "Function executed successfully"}
