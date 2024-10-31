import boto3
import pandas as pd
import numpy as np
from io import StringIO
import sys
from botocore.exceptions import ClientError
from model_pipeline import model_pipeline
## Adding logger instead of print


class S3FileHandler:
    """
    This class 
    """
    def __init__(self, bucket_name, access_key, secret_access_key):
        self.bucket_name = bucket_name
        self.access_key = access_key
        self.secret_access_key = secret_access_key
        self.s3_client = boto3.client(
            's3',
            aws_access_key_id=access_key,
            aws_secret_access_key=secret_access_key
        )
        
    def check_s3_bucket_exists(self):
        """
        This method is used to check if the provided s3 path does exist.
        Uses the list_csv_files method to return all the files in the bucket as a list.
        Uses the read_csv_from_s3 method to read all the files as a Data Frame.
        """
        try:
            self.s3_client.head_bucket(Bucket=self.bucket_name)
            print(f"Bucket '{self.bucket_name}' exists.")
            csv_files = self.list_csv_files()
            return self.read_csv_from_s3(csv_files)
        except ClientError as e:
            print(f"Bucket '{self.bucket_name}' does not exist or you do not have access. Error: {e}")
            sys.exit(1)  # Exit the script if the bucket does not exist or there's an error

    def list_csv_files(self):
        """
        Lists all .csv objects in an S3 bucket and returns them as a list.
        """
        try:
            paginator = self.s3_client.get_paginator('list_objects_v2')
            csv_files = [
                obj['Key'] for page in paginator.paginate(Bucket=self.bucket_name)
                for obj in page.get('Contents', []) if obj['Key'].endswith('.csv')
            ]
            return csv_files
        except ClientError as e:
            print(f"An error occurred while listing objects in the bucket: {e}")
            sys.exit(1)  # Exit the script if there is an error

    def read_csv_from_s3(self, csv_files):
        """
        Reads CSV files from an S3 bucket into Pandas DataFrames and joins them
        if the files were more than one.

        Args:
            csv_files (list): List of .csv file keys returned by list_csv_files method.

        Returns:
            DataFrame: Merged DataFrame of all .csv files.
        """
        dataframes = []
        for file_key in csv_files:
            try:
                obj = self.s3_client.get_object(Bucket=self.bucket_name, Key=file_key)
                df = pd.read_csv(StringIO(obj['Body'].read().decode('utf-8')))
                df['file_key'] = file_key  # Add file_key as a new column
                dataframes.append(df)
            except ClientError as e:
                print(f"An error occurred while reading the file {file_key}: {e}")

        if dataframes:
            merged_df = pd.concat(dataframes, ignore_index=True)
        else:
            merged_df = pd.DataFrame()  # Return an empty DataFrame if no dataframes were read
            print("No dataframes to merge")

        return merged_df

    def clean_ml_bucket(self):
        """
        Deletes all .csv files from the S3 bucket.
        """
        files_to_delete = self.list_csv_files()

        if files_to_delete:
            delete_objects = {'Objects': [{'Key': key} for key in files_to_delete]}
            try:
                self.s3_client.delete_objects(Bucket=self.bucket_name, Delete=delete_objects)
                print(f"Deleted {len(files_to_delete)} .csv files from {self.bucket_name}")
            except ClientError as e:
                print(f"An error occurred while deleting objects: {e}")
        else:
            print("No .csv files found in the bucket")


class ApplicationProcessor(S3FileHandler):

    def __init__(self, bucket_name, access_key, secret_access_key):
        super().__init__(bucket_name, access_key, secret_access_key)

    
    def read_data(self):
        """
        This method uses check_s3_bucket_exists method to check if the provided s3 path does exist, 
        rename the column of the read files if the file is not empty.
        """
        trans_data = self.check_s3_bucket_exists()
        if not trans_data.empty:
            columns = [
                'bvn', 'application_id', 'amount_requested', 'date_created', 'airtime_in_90days',
                'bill_payment_in_90days', 'cable_tv_in_90days', 'deposit_in_90days', 'easy_payment_in_90days',
                'farmer_in_90days', 'inter_bank_in_90days', 'mobile_in_90days', 'utility_bills_in_90days',
                'withdrawal_in_90days'
            ]

            trans_data = trans_data[columns]
            trans_data = trans_data[~trans_data['application_id'].isnull()]
            trans_data.reset_index(drop=True, inplace=True)
            trans_data['application_id'] = trans_data['application_id'].astype(int)
            trans_data.fillna(0.0, inplace=True)
        else:
            print(f"{self.bucket_name} does not exist or could not be accessed.")
        
        return trans_data

    
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

    
    def merge_with_do_good(self, do_good_bucket_name, df):
        """
        Merges the input DataFrame with the Do Good table from another S3 bucket.

        Parameters:
        do_good_bucket_name (str): The S3 bucket name where the Do Good table is stored.
        df (pd.DataFrame): The DataFrame to merge with the Do Good table.

        Returns:
        pd.DataFrame: The merged DataFrame.
        """
        do_good_reader_instance = S3FileHandler(do_good_bucket_name, self.access_key, self.secret_access_key)
        
        columns_ = ['bvn', 'applicationID', 'date_of_default', 'outstanding_balance']
        do_good = do_good_reader_instance.check_s3_bucket_exists()[columns_]

        merged_df = df.merge(do_good, on='bvn', how='left')
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

        
def ml_orchestrator(access_key, secret_access_key, bucket_name='scetru-ml-bucket'):
    """
    Orchestrates the ML pipeline, reading data from the specified S3 bucket,
    processing it, and returning the processed DataFrame.

    Args:
        access_key (str): Access key for S3 bucket access.
        secret_access_key (str): Secret access key for S3 bucket access.
        bucket_name (str, optional): The S3 bucket name to read data from.
        Defaults to 'scetru-ml-bucket'.

    Returns:
        pd.DataFrame: The processed DataFrame.
    """
    processor = ApplicationProcessor(bucket_name, access_key, secret_access_key)
    # read the data from seetru_ml_bucket
    trans_data = processor.read_data()
    
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
    
    #trans_data = read_data('scetru-ml-bucket', access_key, secret_access_key)
    if len(trans_data) > 0:
        # convert columns to the right data types
        trans_data = processor.convert_columns(trans_data, column_types)
        trans_data = processor.merge_with_do_good('scetru-fcmb-do-good-table', trans_data)
        trans_data_ = model_pipeline(trans_data)
        trans_data_['application_id'] = trans_data_['application_id'].astype(str)
        trans_data_['bvn'] = trans_data_['bvn'].astype(str)
        return trans_data_
    else:
        return f"no data for processing in scetru-ml-bucket"


def merge_complete_table(access_key: str, secret_access_key: str) -> pd.DataFrame:
    """
    Merges complete table with outcome data, filtering out declined and missing data.
      Args:
          access_key (str): Access key for S3 bucket access.
          secret_access_key (str): Secret access key for S3 bucket access. 
      Returns:
      pd.DataFrame: Merged and processed DataFrame.
    """
    # Read complete table from S3 with potential error handling
    complete_table = S3FileHandler('complete-table', access_key, secret_access_key).check_s3_bucket_exists()

    # Ensure application_id and bvn columns are strings (if necessary)
    if not pd.api.types.is_string_dtype(complete_table['application_id']):
        complete_table['application_id'] = complete_table['application_id'].astype(str)
    if not pd.api.types.is_string_dtype(complete_table['bvn']):
        complete_table['bvn'] = complete_table['bvn'].astype(str)

    # Filter complete table (combine conditions using logical OR)
    filtered_df = complete_table[~complete_table['decline_reason'].isna() | ~complete_table['amount_approved'].isna()]
    
    outcome = ml_orchestrator(access_key, secret_access_key)
    # Handle cases where outcome is not provided
    if len(outcome) == 0:
        print("No outcome data provided. Merging with empty outcome data.")
        
    # Merge on bvn and application_id (optimized using set intersection)
    merged_df = filtered_df.merge(outcome[outcome['application_id'].isin(filtered_df['application_id'].unique())][['bvn', 'application_id', 'amount_approved','decline_reason']],
                                   on=['bvn', 'application_id'], how='inner', suffixes=('', '_outcome'))

    # Add new columns and reorder (consider using pipe syntax)
    merged_df['updated_date'] = pd.Timestamp.now().floor('min')
    merged_df['loan_message'] = 'Completed'
    merged_df = merged_df[['bvn', 'dob', 'amount_requested', 'application_id', 'loan_tenure','loan_repayment_structure',
                          'internal_id', 'amount_approved','created_date', 'updated_date', 'decline_reason', 'loan_message',
                          'file_key']]

    return merged_df


def read_complete_table(access_key: str, secret_access_key: str):
    complete_table = S3FileHandler('complete-table', access_key, secret_access_key).check_s3_bucket_exists()
    # Ensure application_id columns are of the same type (string)
    complete_table['application_id'] = complete_table['application_id'].astype(str)
    complete_table['bvn'] = complete_table['bvn'].astype(str)
    # Excluding transactions previously processed by ml services or streaming process.
    return complete_table[(complete_table['decline_reason'].isnull()) & (complete_table['amount_approved'].isnull())].reset_index(drop=True)

