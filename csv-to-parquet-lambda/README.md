# Portfolio Hands-On AWS
## CSV to Parquet with AWS Lambda

The goal of this project is to convert CSV files stored in the **landing** zone of an S3 bucket into Parquet files in the **clean** zone. The Lambda function dynamically organizes data into S3 folders based on the file path, and registers the output in AWS Glue Catalog.

### Architecture Overview:

1 - User / External Source:
    Can be a system or a person uploading .csv files to the landing bucket in S3.

2 - Amazon S3 (Landing Zone):
    Receives the CSV files organized by path: landing/{db_name}/{table_name}/file.csv

3 - S3 Event Trigger:
    When a new file is detected, it automatically triggers the execution of the Lambda function.

4 - AWS Lambda:
    Function written in Python using awswrangler.

    Responsible for:

        - Reading the CSV file;
        - Checking and creating the Glue database if needed;
        - Converting the data to Parquet;
        - Writing to the clean zone in S3;
        - Registering metadata in the AWS Glue Catalog.

5 - Amazon S3 (Clean Zone):
    Stores the Parquet files organized by path: /{db_name}/{table_name}/.

6 - AWS Glue Catalog:
    Creates the database and table if needed, enabling future queries via Amazon Athena.

This architecture is represented schematically as follows:

┌────────────────────────────────────────────────────────────────┐
│                       📤 User / System                         │
│     Upload CSV to S3: landing-bucket/{db}/{table}/file.csv     │
└────────────────────────────────────────────────────────────────┘
                                │
                                ▼
              ┌────────────────────────────────────┐
              │     Amazon S3 - Landing Zone       │
              │  (Raw CSV storage, triggers Lambda)│
              └────────────────────────────────────┘
                                │
                                ▼
               ┌──────────────────────────────┐
               │    🔁 S3 Event Notification  │
               └──────────────────────────────┘
                                │
                                ▼
            ┌────────────────────────────────────────────┐
            │        AWS Lambda (csv → parquet)          │
            │   - Reads CSV from S3                      │
            │   - Converts to Parquet using awswrangler  │
            │   - Creates Glue DB/table if needed        │
            └────────────────────────────────────────────┘
                                │
               ┌────────────────┴───────────────┐
               ▼                                ▼
 ┌───────────────────────────┐     ┌─────────────────────────────┐
 │  Amazon S3 - Clean Zone   │     │     AWS Glue Catalog        │
 │ (Parquet files organized  │     │ (Schema registry for Athena │
 │  by db/table)             │     │   and Glue jobs)            │
 └───────────────────────────┘     └─────────────────────────────┘


- **Input**: CSV files uploaded to S3 under the structure `landing-bucket/{db_name}/{table_name}/file.csv`
- **Output**: Parquet files written to `s3://clean-bucket/{db_name}/{table_name}/`
- **Catalog**: If the database doesn't exist in Glue, it's created automatically. Data is stored in a format compatible with Athena.

### Technologies Used:

- **AWS S3** – Source and target storage;
- **AWS Lambda** – Serverless processing;
- **AWS Glue Catalog** – Metadata management;
- **Python** – Language used for transformation;
- **[awswrangler](https://aws-data-wrangler.readthedocs.io/)** – AWS SDK for Pandas.

### Project Structure: 
- README.md: Documentation of the project;
- lambda_function.py: Lambda handler using awswrangler;
- sample_input.csv: Exemple input file;
- lambda-csv-to-parquet-policy.json: required IAM permissions for this Lambda function.


### How It Works

1. A CSV file is uploaded to the S3 path `landing-bucket/{db_name}/{table_name}/file.csv`;
2. This triggers a Lambda function via an S3 Event;
3. The function:
   - Parses the S3 key to extract `db_name` and `table_name`;
   - Checks file size and logs it;
   - Reads the CSV file using `awswrangler.s3.read_csv()`;
   - Creates the Glue database if it doesn't exist;
   - Converts the data to Parquet and writes to `clean-bucket/{db_name}/{table_name}/`;
   - Registers or updates the Glue table metadata.

### Reproducibility
To run this project in your own AWS environment, make sure to **replace the hardcoded S3 bucket names** in the Lambda function and policy file with your actual bucket names for each data zone:

- `dataeng-landing-zone-livro` → your **landing zone** bucket name  
- `dataeng-clean-zone-livro` → your **clean zone** bucket name

These values appear in the following files:

- `lambda_function.py`  
- `lambda-csv-to-parquet-policy.json` (IAM policy attached to the Lambda execution role)

### References

- **Book**: *Data Engineering with AWS – Acquire the skills to design and build AWS-based data transformation pipelines like a pro (2nd ed.)*  
  ISBN-13: 978-1804614426 | ISBN-10: 1804614424  
  _This project was inspired by a hands-on exercise from this book, adapted and expanded for personal learning and practice._

- [AWS Lambda Docs](https://docs.aws.amazon.com/lambda/latest/dg/welcome.html)
- [awswrangler Docs](https://aws-data-wrangler.readthedocs.io/)
- [Glue Catalog](https://docs.aws.amazon.com/glue/latest/dg/components-overview.html)
- [AWS S3 Events](https://docs.aws.amazon.com/AmazonS3/latest/userguide/NotificationHowTo.html)
