import os
import configparser
import boto3
import pandas as pd
import numpy as np
import time
import logging
from botocore.exceptions import ClientError
from watchdog.observers import Observer
from watchdog.events import FileSystemEventHandler
from model_pipeline import model_pipeline

class S3BucketHandler(FileSystemEventHandler):
    def __init__(self, credentials_file='aws_credentials.ini'):
        self.credentials_file = credentials_file
        self.s3 = self._load_credentials()
        self.last_contents = set()
        self.new_csv_files = [] #empty list to hold new files detected.

    def _load_credentials(self):
        config = configparser.ConfigParser()
        config.read(self.credentials_file)
        access_key_id = config.get('aws_credentials', 'aws_access_key_id')
        secret_access_key = config.get('aws_credentials', 'aws_secret_access_key')
        return boto3.client('s3', aws_access_key_id=access_key_id, aws_secret_access_key=secret_access_key)

    def on_any_event(self, event):
        required_columns = [
        'bvn', 'application_id', 'amount_requested', 'date_created', 'airtime_in_90days',
        'bill_payment_in_90days', 'cable_tv_in_90days', 'deposit_in_90days', 'easy_payment_in_90days',
        'farmer_in_90days', 'inter_bank_in_90days', 'mobile_in_90days', 'utility_bills_in_90days',
        'withdrawal_in_90days'
        ]
        bucket_name = "scetru-ml-bucket"
        try:
            current_contents = set(self._list_bucket_contents(bucket_name))
            new_files = current_contents - self.last_contents
            
            for file in new_files:
                if file.endswith('.csv'):
                    logging.info(f"Alert: New .csv file detected in bucket {bucket_name}: {file}")
                    df = self._read_csv_from_s3(bucket_name, file)
                        
                    # Check for required columns
                    missing_columns = set(required_columns) - set(df.columns)
                    if missing_columns:
                        logging.warning(f"Missing required columns in {file}: {missing_columns}")
                    else:
                        self.new_csv_files.append(df)
                        logging.info(f"All required columns present in {file}")                         
    
            self.last_contents = current_contents #set the last state of the bucket.

            # Merge dataframes if multiple new CSV files were detected
            if self.new_csv_files: 
                trans_data = pd.concat(self.new_csv_files, ignore_index=True) 
                trans_data = trans_data[required_columns]
                trans_data = trans_data[~trans_data['application_id'].isnull()] # excluded records where application_id is null
                trans_data.reset_index(drop=True, inplace=True)
                trans_data['application_id'] = trans_data['application_id'].astype(int)
                trans_data.fillna(0.0, inplace=True)
                logging.info(f"Merged {len(self.new_csv_files)} CSV files into a single DataFrame.")
                
                # Save the merged DataFrame to a CSV file in the working directory 
                trans_data.to_csv("trans_data.csv", index=False) 
                #logging.info(f"Saved merged DataFrame to {trans_data.csv}")
                
                # Clear the list of new CSV files
                self.new_csv_files.clear()

                # merge trans with do_good_table
                merged_df = self.join_trans_with_do_good(trans_data)
                model_outcome = model_pipeline(merged_df)
                model_outcome.to_csv("model_outcome.csv", index=False) 
                
                # read complete_table
                complete_table = self.read_complete_table()
                complete_table.to_csv("complete_table.csv", index=False)
                
        except ClientError as e:
            logging.info(f"Error accessing S3 bucket: {e}")

    def _list_bucket_contents(self, bucket_name):
        try:
            response = self.s3.list_objects_v2(Bucket=bucket_name)
            return [obj['Key'] for obj in response.get('Contents', [])]
        except ClientError as e:
            # Handle bucket not found error here (e.response['Error']['Code'] == 'NoSuchBucket')
            logging.error(f"Bucket {self.bucket_name} does not exist or access denied: {e}")
            return []  # Return an empty list to avoid further errors

    def _read_csv_from_s3(self, bucket_name, file_key):
        obj = self.s3.get_object(Bucket=bucket_name, Key=file_key)
        df = pd.read_csv(obj['Body'])
        return df

    def join_trans_with_do_good(self, trans_data):
        do_good_table = self.collate_file("scetru-fcmb-do-good-table")
        columns_ = ['bvn', 'applicationID', 'date_of_default', 'outstanding_balance']
        do_good_table = do_good_table[columns_]

        merged_df = trans_data.merge(do_good_table, on='bvn', how='left')
        merged_df['date_of_default'] = pd.to_datetime(merged_df['date_of_default'], errors='coerce')
        current_date = pd.Timestamp.now()
        merged_df['default_in_last_90days'] = np.where(
            ((current_date - merged_df['date_of_default']).dt.days <= 90) & (merged_df['outstanding_balance'] != 0), 
            'Y', 
            'N'
        )
        merged_df['has_it_make_it_good'] = np.where(
            (merged_df['outstanding_balance'] == 0) | (merged_df['default_in_last_90days'] == 'N'), 
            'Y', 
            'N'
        )
        
        merged_df['bvn'] = merged_df['bvn'].astype(str)
        merged_df.drop(columns=['date_of_default', 'outstanding_balance', 'applicationID'], inplace=True)
        return merged_df

    def read_complete_table(self):
        complete_table = self.collate_file("complete-table")
        # Ensure application_id columns are of the same type (string)
        complete_table['application_id'] = complete_table['application_id'].astype(str)
        complete_table['bvn'] = complete_table['bvn'].astype(str)
        # Excluding transactions previously processed by ml services or streaming process.
        complete_table = complete_table[(complete_table['decline_reason'].isnull()) & (complete_table['amount_approved'].isnull())].reset_index(drop=True)
        return complete_table

    def collate_file(self, bucket_name):
        new_files = []
        files = set(self._list_bucket_contents(bucket_name))
        for file in files:
            df = self._read_csv_from_s3(bucket_name, file)
            new_files.append(df)
        return pd.concat(new_files, ignore_index=True)

    
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

def monitor_s3_bucket(bucket_name, interval=1):
    handler = S3BucketHandler(bucket_name)
    observer = Observer()
    observer.schedule(handler, path='.', recursive=False)

    try:
        # Check bucket existence and credential validity before starting monitoring
        if not handler._list_bucket_contents(bucket_name):
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
    logging.basicConfig(level=logging.INFO)  # Configure logging level
    monitor_s3_bucket(bucket_name)
