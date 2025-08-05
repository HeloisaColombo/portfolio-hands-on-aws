import boto3
import awswrangler as wr
from urllib.parse import unquote_plus
import logging

# Configure basic logging for visibility in CloudWatch
logger = logging.getLogger()
logger.setLevel(logging.INFO)

def lambda_handler(event, context):
    try:
        logger.info('Lambda function started')
        
        # Get bucket and object key from the S3 event trigger
        for record in event['Records']:
            bucket = record['s3']['bucket']['name']
            key = unquote_plus(record['s3']['object']['key'])  # Decodes URL-encoded characters
        
        # Split the S3 key to extract database and table names
        key_list = key.split("/")
        logger.info(f'key_list: {key_list}')
        
        # Validate the path structure to avoid index errors
        if len(key_list) < 3:
            raise ValueError(f"Path structure invalid. Expected at least 3 segments, got: {key_list}")
        
        # Extract database and table names from path
        db_name = key_list[-3]
        table_name = key_list[-2]
        
        logger.info(f'Bucket: {bucket}')
        logger.info(f'Key: {key}')
        logger.info(f'DB Name: {db_name}')
        logger.info(f'Table Name: {table_name}')
        
        # Build full S3 URIs for input and output
        input_path = f"s3://{bucket}/{key}"
        logger.info(f'Input_Path: {input_path}')
        
        output_path = f"s3://dataeng-clean-zone-livro/{db_name}/{table_name}"
        logger.info(f'Output_Path: {output_path}')
        
        # (Optional) Check file size before processing
        try:
            s3_client = boto3.client('s3')
            obj_info = s3_client.head_object(Bucket=bucket, Key=key)
            file_size = obj_info['ContentLength']
            logger.info(f'File size: {file_size} bytes ({file_size/1024/1024:.2f} MB)')
            
            # Warn if file is larger than 100MB
            if file_size > 100 * 1024 * 1024:
                logger.warning(f'Large file detected: {file_size/1024/1024:.2f} MB. May take longer to process.')
                
        except Exception as e:
            logger.warning(f'Could not get file size: {e}')
        
        # Read the CSV file from S3 using awswrangler
        logger.info('Starting CSV read...')
        input_df = wr.s3.read_csv(input_path)
        logger.info(f'CSV read successfully. Shape: {input_df.shape}')
        
        # Check if the Glue database exists
        logger.info('Checking if database exists...')
        try:
            # Try listing tables in the database; if it fails, the DB doesn't exist
            existing_tables = wr.catalog.get_tables(database=db_name)
            logger.info(f'Database {db_name} exists with {len(existing_tables)} tables')
        except Exception:
            logger.info(f'Database {db_name} does not exist, creating...')
            try:
                wr.catalog.create_database(db_name)
                logger.info(f'Database {db_name} created successfully')
            except Exception as e:
                logger.error(f'Error creating database: {e}')
                # Continue processing even if DB creation fails
        
        logger.info('Starting parquet conversion...')
        
        # Convert DataFrame to Parquet and write to S3, registering metadata in Glue
        result = wr.s3.to_parquet(
            df=input_df, 
            path=output_path, 
            dataset=True,               # Writes in a format compatible with Glue/Athena
            database=db_name,           # Glue database
            table=table_name,           # Glue table
            mode="append"               # Appends data to existing dataset
        )
        
        logger.info("RESULT: ")
        logger.info(f'{result}')
        
        # Return success response
        return {
            'statusCode': 200,
            'body': f'Successfully processed {input_path} to {output_path}',
            'rows_processed': len(input_df),
            'result': result
        }

    except Exception as e:
        # Log the error details and re-raise the exception
        logger.error(f'Error processing file: {str(e)}')
        logger.error(f'Error type: {type(e).__name__}')
        raise e