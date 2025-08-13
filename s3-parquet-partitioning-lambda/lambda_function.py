import boto3
import awswrangler as wr
from urllib.parse import unquote_plus
import logging
import pandas as pd
import os

logger = logging.getLogger()
logger.setLevel(logging.INFO)

def detect_date_column(df):
    """
    Automatically detects columns that might contain dates by keywords
    Returns a list of candidates for better transparency
    """
    
    date_keywords = ['date', 'time', 'timestamp', 'created_at', 'updated_at', 'transaction_date']
    date_candidates = []

    # Search by column name
    for column in df.columns:
        if any(keyword in column.lower() for keyword in date_keywords):
            try:
                # Test a sample to verify if it's a date
                pd.to_datetime(df[column].head(100))  
                date_candidates.append({
                    'column': column,
                    'method': 'keyword_match',
                    'priority': 1
                })
            except:
                continue

    # Search by data type
    for column in df.columns: 
        if column not in [candidate['column'] for candidate in date_candidates]:  
            if df[column].dtype in ['datetime64[ns]', 'object']:
                try:
                    pd.to_datetime(df[column].head(100))
                    date_candidates.append({
                        'column': column,
                        'method': 'dtype_match',
                        'priority': 2
                    })
                except:
                    continue
    
    # Sort by priority
    date_candidates.sort(key=lambda x: x['priority'])
    logger.info(f"Date candidates: {[candidate['column'] for candidate in date_candidates]}") 

    return date_candidates

def lambda_handler(event, context):
    try:
        logger.info("Lambda function started.")

        # Get date column name from environment variable
        date_column = os.environ.get('DATE_COLUMN_NAME', None) 

        # Get bucket and object key from s3 event
        for record in event['Records']:
            bucket = record['s3']['bucket']['name']
            key = unquote_plus(record['s3']['object']['key'])

            logger.info(f"Bucket: {bucket}")
            logger.info(f"Key: {key}")

            # Extract database and table name from the s3 path
            key_list = key.split('/')
            logger.info(f"Key list: {key_list}")
            
            # Remove o prefixo se existir
            if key_list[0] == 'non-partitioned-folder': 
                key_list = key_list[1:]

            # Validate the path structure
            if len(key_list) < 3:
                raise ValueError(f"Path structure invalid. Expected at least 3 segments after prefix, got: {key_list}")

            # Extract database and table names from path
            db_name = key_list[-3]
            table_name = key_list[-2].replace('.parquet', '')

            logger.info(f"Database name: {db_name}")
            logger.info(f"Table name: {table_name}")

            # Build s3 path to the input Parquet file
            input_path = f"s3://{bucket}/{key}"
            logger.info(f"Input path: {input_path}")

            # Build output path for the partitioned data
            bucket_output = 'dataeng-clean-zone-livro'
            output_path = f"s3://{bucket_output}/{db_name}/{table_name}/" 

            logger.info(f"Output path: {output_path}")

            # Read the input Parquet file
            df = wr.s3.read_parquet(path=input_path)
            logger.info(f"Input file read successfully. Shape: {df.shape}")  

            # Detect date column
            date_candidates = []
            if not date_column:
                date_candidates = detect_date_column(df)
                if not date_candidates:
                    raise ValueError("No date column detected. Please specify DATE_COLUMN_NAME environment variable.")
                
                if len(date_candidates) > 1: 
                    logger.warning(f"Multiple date columns detected: {[candidate['column'] for candidate in date_candidates]}") 
                    logger.warning(f"Using the first one: {date_candidates[0]['column']}")  
                
                date_column = date_candidates[0]['column']
            
            # Verify if the date column exists in the dataframe and convert to datetime format
            if date_column not in df.columns:
                raise ValueError(f"Date column '{date_column}' not found in the input file.")
            
            # Debugging: Show original data
            logger.info(f"Original date samples from '{date_column}': {df[date_column].head(10).tolist()}")
            logger.info(f"Original data type: {df[date_column].dtype}")
            
            # Robust date conversion
            try:
                df[date_column] = pd.to_datetime(df[date_column], format='%Y-%m-%d')
                logger.info("Date conversion successful with ISO format")
            except:
                try:
                    df[date_column] = pd.to_datetime(df[date_column], infer_datetime_format=True)
                    logger.info("Date conversion successful with auto-inference")
                except Exception as e:
                    logger.error(f"Date conversion failed: {e}")
                    df[date_column] = pd.to_datetime(df[date_column], errors='coerce')

            # Verify conversion results
            logger.info(f"Date range after conversion: {df[date_column].min()} to {df[date_column].max()}")
            logger.info(f"Date column '{date_column}' detected and converted to datetime format.")
            
            # Create year, month, day columns for partitioning
            df['year'] = df[date_column].dt.year
            df['month'] = df[date_column].dt.month
            df['day'] = df[date_column].dt.day

            logger.info("Year, month, day columns created successfully.")

            # Check if the Glue database exists
            logger.info("Checking if the Glue database exists.")

            try:
                existing_tables = wr.catalog.get_tables(database=db_name)
                logger.info(f"Database {db_name} exists with {len(existing_tables)} tables")
            except Exception:
                logger.info("Database does not exist. Creating it.")
                try:
                    wr.catalog.create_database(db_name)
                    logger.info("Database created successfully.")
                except Exception as e:
                    logger.error(f"Error creating database: {str(e)}")
        
            # Write partitioned data to S3 and catalog in Glue
            logger.info("Starting partitioned parquet write")

            result = wr.s3.to_parquet(
                df=df,
                path=output_path,
                dataset=True,
                database=db_name,
                table=table_name,
                partition_cols=['year', 'month', 'day'],
                mode='overwrite',
                compression='snappy'
            )
            logger.info("Partitioning result:")
            logger.info(f'{result}')

        return {
            'statusCode': 200,
            'message': 'Partitioned data processing complete.', 
            'date_column_used': date_column,
            'date_candidates_found': [c['column'] for c in date_candidates] if not os.environ.get('DATE_COLUMN_NAME') else None,
            'rows_processed': len(df),  
            'output_path': output_path  
        }

    except Exception as e:
        logger.error(f"Error: {str(e)}")
        logger.error(f"Error type: {type(e).__name__}")  
        raise e
