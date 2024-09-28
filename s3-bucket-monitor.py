import boto3
import time
from botocore.exceptions import ClientError
from watchdog.observers import Observer
from watchdog.events import FileSystemEventHandler

class S3BucketHandler(FileSystemEventHandler):
    def __init__(self, bucket_name):
        self.bucket_name = bucket_name
        self.s3 = boto3.client('s3')
        self.last_contents = set()

    def on_any_event(self, event):
        try:
            current_contents = set(self._list_bucket_contents())
            new_files = current_contents - self.last_contents
            
            if new_files:
                for file in new_files:
                    print(f"Alert: New file detected in bucket {self.bucket_name}: {file}")
            
            self.last_contents = current_contents
        
        except ClientError as e:
            print(f"Error accessing S3 bucket: {e}")

    def _list_bucket_contents(self):
        response = self.s3.list_objects_v2(Bucket=self.bucket_name)
        return [obj['Key'] for obj in response.get('Contents', [])]

def monitor_s3_bucket(bucket_name, interval=60):
    handler = S3BucketHandler(bucket_name)
    observer = Observer()
    observer.schedule(handler, path='.', recursive=False)
    observer.start()

    try:
        while True:
            time.sleep(interval)
    except KeyboardInterrupt:
        observer.stop()
    observer.join()

if __name__ == "__main__":
    bucket_name = "your-bucket-name"
    monitor_s3_bucket(bucket_name)
