FDA Recall Inventory Monitoring System
======================================

**Overview**
------------
This project uses a Python script deployed in AWS Lambda and the FDA's open API to monitor FDA recall data for potential matches with your restaurant inventory. It integrates AWS S3, EventBridge, and SNS to automate recall checks and notify stakeholders of any matches.

* * * * *

**Features**
------------

-   Fetches FDA recall data from the last two weeks using the FDA's Open API.
-   Compares the recall data to a restaurant inventory stored in an S3 bucket.
-   Sends a weekly alert via SNS with:
    -   All FDA recalls from the last two weeks.
    -   Any matching items found in the restaurant inventory.
-   Scheduled to run every Wednesday at 3 PM Eastern Time using EventBridge.

* * * * *

**Architecture**
----------------

-   **AWS Lambda**: Processes FDA data, compares it with inventory, and publishes results to SNS.
-   **Amazon S3**: Stores the restaurant inventory file in CSV format.
-   **Amazon SNS**: Sends notifications about FDA recalls and inventory matches.
-   **Amazon EventBridge**: Schedules the Lambda function to run weekly.
-   **FDA Open API**: Retrieves recall data.
  

![Alt Text](https://github.com/autumnhicks01/FDA-recall-alert-system/blob/main/CloudArchitecture.drawio.png "Cloud Architecture Diagram")

* * * * *

**Setup Instructions**
----------------------

### **1\. Prerequisites**

-   AWS account with access to S3, Lambda, EventBridge, and SNS.
-   Installed AWS CLI and Python 3.9+.
-   FDA API key (if required for higher request limits).

* * * * *

### **2\. Components**

1.  **Lambda Function**:

    -   Python script that:
        -   Fetches recall data from the FDA API.
        -   Loads and cleans the inventory file from S3.
        -   Compares inventory to recall data.
        -   Publishes results to SNS.
2.  **S3 Bucket**:

    -   Stores the restaurant inventory CSV file. Feel free to use the CSV files in the data folder to test inventory data. The file should have the following structure:

        csv
        `NUM,ITEM #,CATEGORY,DESCRIPTION,BRAND,PACK/SIZE
        3,340403,ALMONDS,ALMONDS SLICED BLANCHED,HEMISPHERE,1/25LB`

3.  **EventBridge Rule**:

    -   Triggers the Lambda function every Wednesday at 3 PM Eastern Time.
4.  **SNS Topic**:

    -   Sends recall notifications to subscribed stakeholders.

* * * * *

### **3\. Deployment Steps**

#### **Step 1: Prepare the Inventory File**

1.  Create a CSV file named `restaurant-inventory.csv` with your inventory data. Feel free to use the CSV in the data folder of this project. 
2.  Example structure:

    csv
    `NUM,ITEM #,CATEGORY,DESCRIPTION,BRAND,PACK/SIZE
    3,340403,ALMONDS,ALMONDS SLICED BLANCHED,HEMISPHERE,1/25LB
    5,192402,ANCHOVIES,ANCHOVIES IN SOY OIL,PACKER,24/28 OZ`

3.  Upload the file to your S3 bucket.

* * * * *

#### **Step 2: Create the Lambda Function**

1.  Write the Python script (`lambda_function.py`) and ensure it includes:
    -   Fetching FDA recall data.
    -   Comparing inventory data.
    -   Publishing results to SNS.
2.  Include required libraries (`boto3`, `urllib.request`, `csv`).
3.  Deploy the script in AWS Lambda with the environment variables:
    -   `S3_BUCKET`: Name of the S3 bucket containing the inventory file.
    -   `S3_KEY`: Key (path) to the inventory file (e.g., `restaurant-inventory.csv`).
    -   `SNS_TOPIC_ARN`: ARN of the SNS topic to publish notifications.

* * * * *

#### **Step 3: Configure SNS**

1.  Create an SNS topic (e.g., `FDA_Recall_Alerts`).
2.  Subscribe stakeholders (email or SMS).
3.  Note the topic ARN and add it to the Lambda function's environment variables.

* * * * *

#### **Step 4: Schedule with EventBridge**

1.  Create an EventBridge rule with the cron expression:

    plaintext
    `cron(0 20 ? * 3 *)`

2.  Set the target as your Lambda function.

* * * * *

### **4\. Testing**

#### **Manually Trigger Lambda**

1.  Use the AWS Lambda console to invoke the function.
2.  Check CloudWatch logs for debug information.

#### **Verify SNS Messages**

-   Confirm stakeholders receive notifications.
-   Example match message:

    plaintext

    `Weekly FDA Recall Alert: Current Inventory Contains Recalled Items

    INVENTORY MATCH:
    NUM: 5
    Item #: 192402
    Category: ANCHOVIES
    Description: ANCHOVIES IN SOY OIL
    Brand: PACKER
    Recall Number: F-12345-2025
    Lot Number: Lot A123
    Recall Reason: Potential contamination with Listeria
    Recall Date: 2025-01-05
    ---`

* * * * *

**Maintenance**
---------------

-   Update the inventory CSV file regularly.
-   Monitor CloudWatch logs for any errors.
-   Adjust the Lambda function if the FDA API changes.

* * * * *

**Future Enhancements**
-----------------------

-   Add retry logic for API failures.
-   Improve SNS message formatting (e.g., HTML emails).
-   Expand to monitor recalls for multiple inventory files.
