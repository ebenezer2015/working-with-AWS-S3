import os
import boto3
from botocore.exceptions import NoCredentialsError
from credentials import *

def saved_processed_data_as_csv(df_outcome):
    if len(df_outcome) > 0:
        # Ensure the directory to save the files exists
        output_dir = 'processed_loan_request'
        os.makedirs(output_dir, exist_ok=True)

        # Iterate over each unique application_id and save records to separate CSV files
        for application_id, group in df_outcome.groupby('application_id'):
            # Define the file path locally
            local_file_path = os.path.join(output_dir, f"{application_id}.csv")

            # Save the group to a CSV file
            group.to_csv(local_file_path, index=False)

        print("Files saved successfully.")


def upload_to_s3(local_file, bucket, s3_file):
    session = boto3.Session(
            aws_access_key_id=access_key,
            aws_secret_access_key=secret_access_key)
    
    s3 = session.client('s3')
    
    try:
        s3.upload_file(local_file, bucket, s3_file)
        print(f"Upload Successful: {local_file}")
        return True
    except FileNotFoundError:
        print(f"The file {local_file} was not found")
        return False
    except NoCredentialsError:
        print("Credentials not available")
        return False

def upload_csv_files(folder_path, bucket_name, s3_path_prefix, files_to_upload):
    """
        Uploads all CSV files from a folder to an S3 bucket.
        Args:
            folder_path (str): Path to the folder containing CSV files.
            bucket_name (str): Name of the S3 bucket to upload the files to.
            s3_path_prefix (str, optional): Prefix to add to the S3 file paths. Defaults to None.

        Returns:
            None
    """

    # Get all files in the folder
    all_files = os.listdir(folder_path)

    # Filter for CSV files
    csv_files = [
        file for file in all_files 
        if file.endswith('.csv') and os.path.splitext(file)[0] in files_to_upload
    ]
    
    # Upload each CSV file
    for filename in csv_files:
        local_file = os.path.join(folder_path, filename)
        s3_file = os.path.join(s3_path_prefix, filename) if s3_path_prefix else filename

    # Call your upload_to_s3 function (modify for your specific implementation)
    upload_to_s3(local_file, bucket_name, s3_file)
    print(f"Upload Successful: from {local_file} into {s3_file}")

    # Inform about no CSV files found (optional)
    if not csv_files:
        print(f"No CSV files found in {folder_path}.")