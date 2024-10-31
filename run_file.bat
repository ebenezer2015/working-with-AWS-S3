@echo off
call s3_env\Scripts\activate
python Desktop\Projects\github\working-with-AWS-S3\s3-bucket-monitor.py
call s3_env\Scripts\deactivate

