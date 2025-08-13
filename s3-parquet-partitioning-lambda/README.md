# Portfolio Hands-On AWS
## Automated data partitioner with AWS Lambda

Automated serverless data pipeline that processes non-partitioned Parquet files from S3 *landing zone*, intelligently detects date columns, and creates partitioned datasets in the *clean zone* with automatic AWS Glue catalog integration.

### Architecture Overview:

**1 - User / External Source:**
User or system uploads .parquet files to the landing bucket in S3, inside the specific folder.

**2 - Amazon S3 (Landing Zone):**
Receives the .parquet files organized by path: `landing-bucket/non-partitioned-folder/{db_name}/{table_name}/file.parquet`

**3 - S3 Event Trigger:**
When a new file is detected, it automatically triggers the execution of the Lambda function.

**4 - AWS Lambda:**
Function written in Python using awswrangler.

Responsible for:
- Reading the .parquet file;
- Auto-detecting date columns intelligently;
- Creating year/month/day partition columns;
- Checking and creating the Glue database if needed;
- Writing partitioned data to the clean zone in S3;
- Registering metadata in the AWS Glue Catalog.

**5 - Amazon S3 (Clean Zone):**
Stores the partitioned Parquet files organized by path: `clean-bucket/{db_name}/{table_name}/year=YYYY/month=MM/day=DD/`

**6 - AWS Glue Catalog:**
Creates the database and table if needed, enabling future queries via Amazon Athena with partition pruning.

This architecture is represented schematically as follows:

<pre> ```
    
┌───────────────────────────────────────────────────────────────────────────────────────────┐
│                                   📤 User / System                                        │
│Upload parquet to S3: landing-bucket//non-partitioned-folder/{db}/{table}/file.parquet     │
└───────────────────────────────────────────────────────────────────────────────────────────┘
                                           │
                                           ▼
                     ┌────────────────────────────────── ─────┐
                     │         Amazon S3 - Landing Zone       │
                     │  (Raw parquet storage, triggers Lambda)│
                     └────────────────────────────────────────┘
                                        │
                                        ▼
                         ┌────────────────────────────────┐
                         │    🔁 S3 Event Notification    │
                         │ Prefix: non-partitioned-folder/│
                         │         Sufix: .parquet        │
                         └────────────────────────────────┘
                                        │
                                        ▼
                ┌───────────────────────────────────────────────┐
                │        AWS Lambda (csv → parquet)             │
                │   - Reads Parquet from S3;                    │
                │   - Auto-detects date column;                 │
                │   - Creates year/month/day partition columns  │
                │   - Writes partitioned data using awswrangler │
                │   - Creates Glue DB/table if needed           │
                └───────────────────────────────────────────────┘
                                         │
                        ┌────────────────┴───────────────┐
                        ▼                                ▼
            ┌───────────────────────────┐     ┌─────────────────────────────┐
            │  Amazon S3 - Clean Zone   │     │     AWS Glue Catalog        │
            │ (Parquet files organized  │     │ (Schema registry for Athena │
            │  by db/table)             │     │   and Glue jobs)            │
            └───────────────────────────┘     └─────────────────────────────┘

    ``` </pre>

- **Input**: CSV files uploaded to S3 under the structure `landing-bucket/non-partitioned-folder/{db_name}/{table_name}/file.parquet`
- **Output**: Parquet partitioned files written to `s3://clean-bucket/{db_name}/{table_name}/`
- **Catalog**: If the database doesn't exist in Glue, it's created automatically. Data is stored in a format compatible with Athena.


### Technologies Used:

- **AWS S3** – Source and target storage;
- **AWS Lambda** – Serverless processing in Python 3.9;
- **AWS Glue Catalog** – Metadata management;
- **Python** – Language used for transformation;
- **[awswrangler](https://aws-data-wrangler.readthedocs.io/)** – AWS SDK for Pandas.

### Project Structure: 
- README.md: Documentation of the project;
- lambda_function.py: Lambda handler using awswrangler;
- sample_transactions.parquet: Exemple input file;
- s3-parquet-partitioning-lambda-policy.json: required IAM permissions for this Lambda function.

### How It Works

1. A CSV file is uploaded to the S3 path `landing-bucket/non-partitioned-folder/{db_name}/{table_name}/file.csv`;
2. This upload triggers the AWS Lambda function via an S3 Event Notification, configured to monitor the prefix non-partitioned-folder/;
3. The function:
   - Extracts bucket and object key from the S3 event payload;
   - Parses the S3 key to identify db_name and table_name from the file path;
   - Builds input and output S3 paths, where the output path points to the clean zone bucket: s3://dataeng-clean-zone-livro/{db_name}/{table_name}/
   - Reads the Parquet file from the landing zone into a Pandas DataFrame using awswrangler.s3.read_parquet().
   - Date column detection:
        - If the environment variable DATE_COLUMN_NAME is set, the function uses it directly;
        - Otherwise, it automatically searches for possible date columns based on:
            - Column names containing date/time keywords (e.g., "date", "timestamp", "transaction_date");
            - Data type analysis, attempting to parse values as dates.
        - If multiple date columns are detected, the first one found is used (priority given to keyword matches).
    - Data conversion:
        - Ensures the date column is in datetime format, using:
            - ISO format parsing (%Y-%m-%d), or
            - Automatic date format inference.
        - Logs the date range found after conversion.
    - Partition columns creation:
        - From the detected date column, creates three new columns: year, month, and day.
    - Glue Catalog management:
        - Checks if the Glue database {db_name} exists; if not, creates it.
    - Write partitioned data to S3:
        - Saves the DataFrame as Parquet with snappy compression, partitioned by year, month, and day;
        - Uses awswrangler.s3.to_parquet() with mode='overwrite' to replace any existing data for the same partitions.
    - Glue table registration/update: 
        - Registers or updates the corresponding Glue table {table_name} in database {db_name} to make the data queryable via Amazon Athena


### Reproducibility
To run this project in your own AWS environment, make sure to:

1. **replace the hardcoded S3 bucket names** in the Lambda function and policy file with your actual bucket names for each data zone:
    - `dataeng-landing-zone-livro` → your **landing zone** bucket name  
    - `dataeng-clean-zone-livro` → your **clean zone** bucket name

    These values appear in the following files:

    - `lambda_function.py`  
    - `s3-parquet-partitioning-lambda-policy.json` (IAM policy attached to the Lambda execution role)

2. S3 Event Notification: configure your landing zone bucket to trigger the Lambda only for objects with the prefix: non-partitioned-folder/

3. Required IAM Permissions: ensure the Lambda execution role has permissions to:

    - Read from the landing bucket;
    - Write to the clean bucket;
    - Interact with the AWS Glue Catalog.

    The sample policy is provided in the file `s3-parquet-partitioning-lambda-policy`.json

4. Configure Lambda environment variables (optional): Set the variable DATE_COLUMN_NAME if you want to force the use of a specific date column for partitioning. If not set, the function will attempt to automatically detect the date column.


### References

- [AWS Lambda Docs](https://docs.aws.amazon.com/lambda/latest/dg/welcome.html)
- [awswrangler Docs](https://aws-data-wrangler.readthedocs.io/)
- [Glue Catalog](https://docs.aws.amazon.com/glue/latest/dg/components-overview.html)
- [AWS S3 Events](https://docs.aws.amazon.com/AmazonS3/latest/userguide/NotificationHowTo.html)
