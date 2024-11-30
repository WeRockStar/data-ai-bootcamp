
# Gemini Code Assist for Data Engineering  - Prompt


## Generate unittest

Prerequisites

Before you begin, ensure the following:
- Gemini Code Assist is installed: Follow the installation guide in the official documentation.
- Configured project: Your project should be set up with a supported testing framework (e.g., pytest, unittest, etc.).
- Access to source code: The source code to be tested should be accessible and properly structured.

Step 1: Open the Code File
- Open the file you want to create unit tests for in your preferred editor.
- Ensure the file contains methods or classes that you intend to test.

Step 2: Launch Gemini Code Assist
- Open Gemini Code Assist within your IDE or development environment.
- Select the Unit Test Generation feature from the main menu.

Step 3: Select the Function or Class to Test
- Highlight the specific function or class you want to test.
- Right-click and select Generate Unit Test from the context menu (or use the shortcut specified in your environment).

Step 4: Configure the Test Parameters
- Gemini Code Assist allows you to configure the unit test generation process:
    - Select the testing framework: Choose your preferred framework (e.g., pytest, unittest, etc.).
    - Mocking preferences: Specify whether dependencies should be mocked automatically.

Step 5: Generate the Unit Test
- Click Generate.
- Review the auto-generated test cases. Gemini Code Assist will create test methods.

Step 6: Save and Review the Test File
- Save the generated test file in the appropriate test directory (e.g., tests/).
- Review the test cases to ensure they meet your project requirements.

Step 7: Run the Tests
- Execute the unit tests using your testing framework: python -m unittest tests/test_file_name.py
- Verify that all tests pass or address any failing tests.

## DAG_1 - ingest data from API to BigQuery

### 1. Prompt to Create Data Pipeline 

```
Create Airflow DAG that contain 2 tasks:
Objective is to collect data and upload Create/Append to BigQuery Table 
projec_id=`YOUR_PROJECT_ID`

1. Collect Data from API using PythonOperator
and Save results in GSC 
bucket_name=`deb-gemini-code-assist-YOUR_NAME` 
folder=`raw/coingecko`
https://api.coingecko.com/api/v3/simple/price?ids=bitcoin,ethereum,tether&vs_currencies=usd,thb&include_market_cap=true&include_24hr_vol=true&include_24hr_change=true&include_last_updated_at=true

2. Upload data from GSC to BigQuery
destination_project_dataset_table=`gemini_assist_workshop.coingecko_price`
```
### 2. Chat with you code after prompt is done
```
Fix as the following 
- Add Schedule to interval to every monday,wenesday,friday 21:00 
- DAG's owner to `gemini-code-assist`
```

## DAG_2 - ingest data from Json File to BigQuery

```
Create Airflow DAG that contain 3 tasks:
projec_id=`YOUR_PROJECT_ID`
bucket_name=`deb-gemini-code-assist-YOUR_NAME` 
destination_project_dataset_table=`gemini_assist_workshop.customer_feedback`

1. Extract: Load data from path `customer_feedback.json` and convert to csv and upload to GCS 
folder=`raw/customer_feedback` using pythonOperator

2. Transform: 
Get data from raw and then clean and transform the customer_feedback data using pandas.
folder=`processed/customer_feedback\`
Add the following columns:
count_character, age : calculate from birthdate, and age_geration

3. Upload data to BigQuery 
```


## DAG_2 
2. Code Completeion for Airflow DAG
```
Add task to send LINE notication if the all the upstream task is Done.
```