import os
import boto3
import pandas as pd
import numpy as np
from dotenv import load_dotenv
import time
import logging
from botocore.exceptions import ClientError
from watchdog.observers import Observer
from watchdog.events import FileSystemEventHandler
from model_pipeline import model_pipeline

# Explicitly load the .env file at the top of your script 
load_dotenv()

class S3BucketHandler(FileSystemEventHandler):
    def __init__(self, credentials_file='.env'):
        self.credentials_file = credentials_file
        self.s3 = self._load_credentials()
        self.last_contents = set()
        self.new_csv_files = [] #empty list to hold new files detected.

    def _load_credentials(self):
        load_dotenv(self.credentials_file)
        access_key_id = os.getenv('AWS_ACCESS_KEY_ID')
        secret_access_key = os.getenv('AWS_SECRET_ACCESS_KEY')
        # Print statements to check if the credentials are loaded 
        print(f"AWS_ACCESS_KEY_ID: {access_key_id}") 
        print(f"AWS_SECRET_ACCESS_KEY: {secret_access_key}")
        
        if not access_key_id or not secret_access_key: 
            raise ValueError("AWS credentials not found in the environment variables")
        return boto3.client('s3', aws_access_key_id=access_key_id, aws_secret_access_key=secret_access_key)

    def on_any_event(self, event):
        column_types = {
            'bvn': 'object',
            'application_id': 'object',
            'amount_requested': 'float',
            'date_created': 'date',
            'airtime_in_90days': 'float',
            'bill_payment_in_90days': 'float',
            'cable_tv_in_90days': 'float',
            'deposit_in_90days': 'float',
            'easy_payment_in_90days': 'float',
            'farmer_in_90days': 'float',
            'inter_bank_in_90days': 'float',
            'mobile_in_90days': 'float',
            'utility_bills_in_90days': 'float',
            'withdrawal_in_90days': 'float',
            }
        
        required_columns = list(column_types.keys())
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
            while self.new_csv_files: 
                trans_data = pd.concat(self.new_csv_files, ignore_index=True) 
                trans_data = trans_data[required_columns]
                trans_data = trans_data[~trans_data['application_id'].isnull()] # excluded records where application_id is null
                trans_data.reset_index(drop=True, inplace=True)
                # convert columns to the right data types
                trans_data = self.convert_columns_type(trans_data, column_types)
                trans_data.fillna(0.0, inplace=True)
                logging.info(f"Merged {len(self.new_csv_files)} CSV files into a single DataFrame.")
                
                # merge trans_data with do_good_table
                merged_df = self.join_trans_with_do_good(trans_data)
                model_outcome = model_pipeline(merged_df)
                logging.info(f"Parsed the merged DataFrame to model pipeline")

                # read and update the complete_table
                return_complete_table = self.read_and_update_complete_table(model_outcome)
                if not return_complete_table.empty:
                    return_complete_table.to_csv("return_complete_table.csv", index=False) 
                    S3BucketHandler.saved_processed_df_as_csv(return_complete_table) # save for audit
                    # upload the complete table
                    self.upload_files_to_s3_bucket(
                            local_filepath = "processed_loan_request", 
                            bucket_name = "complete-table" ,
                            s3_path_prefix = return_complete_table.file_key.iloc[0].split('/')[0],
                            files_to_upload = [str(i) for i in return_complete_table.application_id])
                
                    # Clear the list of new CSV files
                    self.new_csv_files.clear()
                else:
                    print("No records in complete table, continuing to watch for new files.")
                    self.new_csv_files.clear()
                     
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
        df['file_key'] = file_key  # Add file_key as a new column
        return df

    def join_trans_with_do_good(self, trans_data):
        do_good_table = self.collate_file("scetru-fcmb-do-good-table")
        columns_ = ['bvn', 'applicationID', 'date_of_default', 'outstanding_balance']
        do_good_table = do_good_table[columns_]
        do_good_table['bvn'] = do_good_table['bvn'].astype(str)

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
        
        merged_df.drop(columns=['date_of_default', 'outstanding_balance', 'applicationID'], inplace=True)
        return merged_df

    def read_and_update_complete_table(self, outcome_table):
        # Read and create complete table
        complete_table = self.collate_file("complete-table")
        complete_table['application_id'] = complete_table['application_id'].astype(str)
        complete_table['bvn'] = complete_table['bvn'].astype(str)
        # Excluding transactions previously processed by ml services or streaming process.
        complete_table = complete_table[(complete_table['decline_reason'].isnull()) & (complete_table['amount_approved'].isnull())].reset_index(drop=True)
        complete_table = complete_table.drop(columns=['amount_approved', 'decline_reason'])
        update_complete_table = complete_table.merge(outcome_table[outcome_table['application_id'].isin(complete_table['application_id'].unique())][['bvn', 'application_id', 'amount_approved','decline_reason']],
                               on=['bvn', 'application_id'], how='inner', suffixes=('', '_outcome'))

        # Add new columns and reorder (consider using pipe syntax)
        update_complete_table['updated_date'] = pd.Timestamp.now().floor('min')
        update_complete_table['loan_message'] = 'COMPLETED'
        update_complete_table = update_complete_table[['bvn', 'dob', 'amount_requested', 'application_id', 'loan_tenure','loan_repayment_structure',
                              'internal_id', 'amount_approved','created_date', 'updated_date', 'decline_reason', 'loan_message',
                              'file_key']]
        return complete_table

    def collate_file(self, bucket_name):
        new_files = []
        files = set(self._list_bucket_contents(bucket_name))
        for file in files:
            df = self._read_csv_from_s3(bucket_name, file)
            new_files.append(df)
        return pd.concat(new_files, ignore_index=True)


    def upload_files_to_s3_bucket(self, local_filepath, bucket_name, s3_path_prefix='', files_to_upload=None):
        """
        Uploads all CSV files from a folder to an S3 bucket.
        
        Args:
            local_filepath (str): Path to the folder containing CSV files.
            bucket_name (str): Name of the S3 bucket to upload the files to.
            s3_path_prefix (str, optional): Prefix to add to the S3 file paths. Defaults to an empty string.
            files_to_upload (list, optional): List of filenames without extension to upload. Defaults to None.
        
        Returns:
            None
        """

        # Filter for CSV files to upload
        csv_files = [file for file in os.listdir(local_filepath) if file.endswith('.csv') 
                     and (files_to_upload is None or os.path.splitext(file)[0] 
                          in files_to_upload)
                    ]
        
        if not csv_files:
            print(f"No CSV files found in {local_filepath}.")
            return
        
        for filename in csv_files:
            local_file = os.path.join(local_filepath, filename)
            s3_file = f"{s3_path_prefix}/{filename}" if s3_path_prefix else filename

            try:
                self.s3.upload_file(local_file, bucket_name, s3_file)
                print(f"Upload Successful: from {local_file} to s3://{bucket_name}/{s3_file}")
            except FileNotFoundError:
                print(f"The file {local_file} was not found")
            except NoCredentialsError:
                print("Credentials not available")

    
    @staticmethod
    def convert_columns_type(df, column_types):
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

    @staticmethod
    def saved_processed_df_as_csv(df_outcome):
        if len(df_outcome) > 0:
            # Ensure the directory to save the files exists
            output_dir = 'processed_loan_request' #staging area
            os.makedirs(output_dir, exist_ok=True)
    
            # Iterate over each unique application_id and save records to separate CSV files
            for application_id, group in df_outcome.groupby('application_id'):
                # Define the file path locally
                local_file_path = os.path.join(output_dir, f"{application_id}.csv")
    
                # Save the group to a CSV file
                group.to_csv(local_file_path, index=False)
    
            print("Files saved successfully.")


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
