import configparser
import boto3
import logging
import os
from botocore.exceptions import ClientError
from PandasOperation import *
import yaml
from yaml.loader import SafeLoader

class S3Operations:
    # read configurations
    def get_configuration(self, config_file_path):
        print('inside get_configuration method')
        config = configparser.RawConfigParser()
        config.read(config_file_path)
        details_dict = dict(config.items('aws'))
        return details_dict['aws_access_key_id_value'], details_dict['aws_secret_access_key_value']

    # connecting to aws service
    def connect_to_aws_service(self, service_name, aws_access_key, aws_secret_key):
        print('connect_to_aws_service method called')
        client = boto3.client(
            service_name,
            aws_access_key_id=aws_access_key,
            aws_secret_access_key=aws_secret_key
            , verify=False  # must if we want to bypass the SSL security
        )
        return client

    # create bucket
    def create_bucket(self, bucket_name, client):
        print('Inside create_bucket method')
        response = client.create_bucket(
            ACL='public-read-write',
            Bucket=bucket_name
        )
        return response

    # list all the buckets present
    def list_buckets(self, client):
        print('Inside list_buckets method')
        list_bucket = client.list_buckets()
        all_buckets = []
        # Output the bucket names
        for bucket in list_bucket['Buckets']:
            all_buckets.append(bucket["Name"])
        return all_buckets

    # Upload files to s3 storage
    def upload_file_to_s3(self, client, bucket_name, file_name, object_name):
        print('upload_file_to_s3 method called')
        if object_name is None:
            object_name = os.path.basename(file_name)
        try:
            response = client.upload_file(file_name, bucket_name, object_name)
        except ClientError as e:
            logging.error(e)
            return False
        return True

    # check file present in a specific bucket
    def list_files_in_bucket(self, client, bucket_name, file_name):
        print('list_files_in_bucket method called ', bucket_name)
        try:
            my_bucket = client.get_object(Bucket=bucket_name,  Key=file_name)
            return True
        except:
            return False

    def delete_files(self, client, bucket_name, file_name):
        print('Inside delete_files method')
        try:
            response = client.delete_object(
            Bucket= bucket_name,
            Key=file_name
            )
            return True
        except:
            return False

    def download_file(self, client, bucket_name, file_name_key, download_file_name):
        with open(download_file_name, 'wb') as data:
            client.download_fileobj(bucket_name, file_name_key, data)

    # copy files from source bucket to destination bucket
    def copy_file_between_bucket(self, client, source_bucket, destination_bucket, file_name_source_key,
                                 file_name_destination_key):
        print('copy_file_between_bucket method called')
        copy_source = {
            'Bucket': source_bucket,
            'Key': file_name_source_key
        }
        print('SOURCE BUCKET AND FILE IS: ',copy_source)
        print('DESTINATION BUCKET IS : ', destination_bucket)
        client.copy_object(CopySource=copy_source, Bucket=destination_bucket, Key=file_name_destination_key)


# create object
s3_obj = S3Operations()

# read config files
with open('config.yaml') as f:
    data = yaml.load(f, Loader=SafeLoader)

# parameters
config_file_path = data['config_file_path'].replace("\\","\\\\")
bucket_name_to_use = data['bucket_name_to_use']
file_name_key = data['file_name_key']
file_name_location = data['file_name_location'].replace("\\","\\\\")
service_name_to_use = data['service_name_to_use']
download_file_name = 'downloaded_'+data['file_name_key']
destination_bucket = data['destination_bucket']
source_bucket = data['source_bucket']
file_name_destination_key = data['file_name_destination_key']

print(data)
# get connection object
aws_access_key, aws_secret_key = s3_obj.get_configuration(config_file_path)
# print(aws_access_key)

client = s3_obj.connect_to_aws_service(service_name_to_use, aws_access_key, aws_secret_key)
# print(client)

response = s3_obj.create_bucket(bucket_name=bucket_name_to_use,client=client)
print(response)

all_buckets = s3_obj.list_buckets(client)
# print(all_buckets)

get_files = s3_obj.list_files_in_bucket(client,bucket_name_to_use, file_name_key)
print('Files present or not IN BUCKET: ', get_files)

#if the file already exists then delete it and then upload it.
if get_files:
    print('File already exists so deleting it and uploading')
    response_delete = s3_obj.delete_files(client, bucket_name_to_use, file_name=file_name_key)
    print('File Delete? ', response_delete if response_delete else 'No')
    upload_file = s3_obj.upload_file_to_s3(client, bucket_name=bucket_name_to_use,
                                           file_name=file_name_location,
                                           object_name=None)
else:
    print('File not found. So uploading new one')
    upload_file = s3_obj.upload_file_to_s3(client, bucket_name=bucket_name_to_use,
                                           file_name=file_name_location,
                                           object_name=None)
    print('UPLOAD FILE STATUS: ',upload_file)

s3_obj.download_file(client=client, bucket_name=bucket_name_to_use, file_name_key=file_name_key,
                     download_file_name=download_file_name)

# copy file from source bucket to destination bucket
s3_obj.copy_file_between_bucket(client, source_bucket, destination_bucket, file_name_key, file_name_destination_key
                                +'_'+file_name_key)


# pandas operation
pd = PandasOperation()
df = pd.read_downloaded_file(file_name=download_file_name)
print('Data read from CSV: ', df)

# write_csv
pd.write_csv_date(file_to_write=file_name_key, data=df)

# uploading the new file:
print('File to be uploaded after modification: ', file_name_location.replace(file_name_key,'upload_'+file_name_key))
s3_obj.upload_file_to_s3(client, bucket_name_to_use, file_name_location.replace(file_name_key,'upload_'+file_name_key),
                         object_name=None)







