from google.cloud import storage
import os
import argparse

def upload_blob(bucket_name, source_file_name, destination_blob_name):
    """Uploads a file to the GCS bucket."""
    storage_client = storage.Client()
    bucket = storage_client.bucket(bucket_name)
    blob = bucket.blob(destination_blob_name)
    blob.upload_from_filename(source_file_name)
    print(f"File {source_file_name} uploaded to {destination_blob_name} in bucket {bucket_name}.")

def main():
    parser = argparse.ArgumentParser(description="Process data and upload to Google Cloud Storage")
    parser.add_argument("--source", type=str, help="Source File name")
    parser.add_argument("--dest", type=str, help="Path to the destination blob")
    parser.add_argument("--all", action='store_true', help="Upload all csv")
    args = parser.parse_args()
    if args.all:
        folder_path = os.getcwd()
        csv_files = [os.path.join(folder_path, file) for file in os.listdir(folder_path) if file.endswith(".csv")]

        for csv_file in csv_files: 
            upload_blob(bucket_name, csv_file, csv_file.split('/')[-1])

    else:
        source_file_name = args.source
        destination_blob_name = args.dest

        bucket_name = os.environ["BUCKET_NAME"]
        upload_blob(bucket_name, source_file_name, destination_blob_name)

if __name__ == "__main__":
    main()