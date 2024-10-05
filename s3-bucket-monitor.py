import os
import configparser
import boto3
import pandas as pd
import time
import logging
from botocore.exceptions import ClientError
from watchdog.observers import Observer
from watchdog.events import FileSystemEventHandler

class S3BucketHandler(FileSystemEventHandler):
    def __init__(self, bucket_name, credentials_file='aws_credentials.ini', required_columns=[]):
        self.bucket_name = bucket_name
        self.credentials_file = credentials_file
        self.s3 = self._load_credentials()
        self.last_contents = set()
        self.new_csv_files = []
        self.required_columns = required_columns

    def _load_credentials(self):
        config = configparser.ConfigParser()
        config.read(self.credentials_file)
        access_key_id = config.get('aws_credentials', 'aws_access_key_id')
        secret_access_key = config.get('aws_credentials', 'aws_secret_access_key')

        return boto3.client('s3', aws_access_key_id=access_key_id, aws_secret_access_key=secret_access_key)

    def on_any_event(self, event):
        try:
            current_contents = set(self._list_bucket_contents())
            new_files = current_contents - self.last_contents
            
            if new_files:
                for file in new_files:
                    if file.endswith('.csv'):
                        logging.info(f"Alert: New .csv file detected in bucket {self.bucket_name}: {file}")
                        df = self._read_csv_from_s3(file)
                        
                        # Check for required columns
                        missing_columns = set(self.required_columns) - set(df.columns)
                        if missing_columns:
                            logging.warning(f"Missing required columns in {file}: {missing_columns}")
                        else:
                            self.new_csv_files.append(df)
                            logging.info(f"All required columns present in {file}")
                            
                        # Process the DataFrame as needed
                        print(df.head())  # Example: Print the first few rows

                    else:
                        logging.info(f"New file detected in bucket {self.bucket_name}, but it's not a CSV file: {file}")
            
            self.last_contents = current_contents #set the last state of the bucket.

            # Merge dataframes if multiple new CSV files were detected
            if len(self.new_csv_files) > 1:
                merged_df = pd.concat(self.new_csv_files, ignore_index=True)
                logging.info(f"Merged {len(self.new_csv_files)} CSV files into a single DataFrame.")
                merged_df = merged_df[self.required_columns]
                merged_df = merged_df[~merged_df['application_id'].isnull()] # excluded records where application_id is null
                merged_df.reset_index(drop=True, inplace=True)
                merged_df['application_id'] = merged_df['application_id'].astype(int)
                merged_df.fillna(0.0, inplace=True)
                # Process the merged DataFrame as needed
                print(merged_df.head())  # Example: Print the first few rows

            # Clear the list of new CSV files
            self.new_csv_files.clear()
            
        except ClientError as e:
            logging.info(f"Error accessing S3 bucket: {e}")

    def _list_bucket_contents(self):
        try:
            response = self.s3.list_objects_v2(Bucket=self.bucket_name)
            return [obj['Key'] for obj in response.get('Contents', [])]
        except ClientError as e:
            # Handle bucket not found error here (e.response['Error']['Code'] == 'NoSuchBucket')
            logging.error(f"Bucket {self.bucket_name} does not exist or access denied: {e}")
            return []  # Return an empty list to avoid further errors

    def _read_csv_from_s3(self, file_key):
        obj = self.s3.get_object(Bucket=self.bucket_name, Key=file_key)
        df = pd.read_csv(obj['Body'])
        return df

    @staticmethod
    def convert_columns(df, column_types):
        """
        Converts columns in the DataFrame to the specified data types.

        Parameters:
        df (pd.DataFrame): The DataFrame to be converted.
        column_types (dict): A dictionary where keys are column names and values are the desired data types.

        Returns:
        pd.DataFrame: The DataFrame with converted columns.
        """
        for col, dtype in column_types.items():
            if col in df.columns:
                if dtype == 'date':
                    df[col] = pd.to_datetime(df[col]).dt.date
                else:
                    df[col] = df[col].astype(dtype)
            else:
                print(f"Warning: Column '{col}' does not exist in the DataFrame.")
        return df

def monitor_s3_bucket(bucket_name, interval=10, required_columns=[]):
    handler = S3BucketHandler(bucket_name, required_columns=required_columns)
    observer = Observer()
    observer.schedule(handler, path='.', recursive=False)

    try:
        # Check bucket existence and credential validity before starting monitoring
        if not handler._list_bucket_contents():
            logging.error(f"Bucket {bucket_name} does not exist or access denied.")
            return

        logging.info(f"Bucket {bucket_name} exists and access successful. Starting monitoring.")
        observer.start()
        
        while True:
            time.sleep(interval)
    except KeyboardInterrupt:
        observer.stop()
    observer.join()

if __name__ == "__main__":
    bucket_name = "scetru-ml-bucket"
    required_columns = [
        'bvn', 'application_id', 'amount_requested', 'date_created', 'airtime_in_90days',
        'bill_payment_in_90days', 'cable_tv_in_90days', 'deposit_in_90days', 'easy_payment_in_90days',
        'farmer_in_90days', 'inter_bank_in_90days', 'mobile_in_90days', 'utility_bills_in_90days',
        'withdrawal_in_90days'
    ]
    logging.basicConfig(level=logging.INFO)  # Configure logging level
    monitor_s3_bucket(bucket_name, required_columns=required_columns)
