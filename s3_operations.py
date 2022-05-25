import boto3
import requests
from jproperties import Properties
import configparser


config = configparser.RawConfigParser()
config.read('C:\\Users\\satissingh\\PycharmProjects\\PySpark_Project\\config\\config.properties')
details_dict = dict(config.items('aws'))
aws_access_key = details_dict['aws_access_key_id_value']
aws_secret_key = details_dict['aws_secret_access_key_value']
print(aws_secret_key)
#
with open('C:\\Users\\satissingh\\PycharmProjects\\PySpark_Project\\config\\config.properties', 'r') as config_file:
    #configs.load('config_file')
    print(type(config_file.read()))
    print()

# client = boto3.session("s3")

# def getVarFromFile(filename):
#     f = open(filename)
#     print(f)
#     data = imp.load_source('data', '', f)
#     print(data)
#     f.close()

# creating client
client = boto3.client(
's3',
    aws_access_key_id=aws_access_key,
    aws_secret_access_key=aws_secret_key
    ,verify=False   # must if we want to bypass the SSL security
)
# creating S3 bucket
# response = client.create_bucket(
#     ACL='public-read-write',
#     Bucket='satish-bucker-13487293',
#     # CreateBucketConfiguration={
#     #     'LocationConstraint': 'us-east-1'
#     # }
# )
# Let's use Amazon S3

response = client.delete_bucket(
    Bucket='sk-my-bucket-0001'

)

# Retrieve the list of existing buckets

list_bucket = client.list_buckets()

# Output the bucket names
print('Existing buckets:')
for bucket in list_bucket['Buckets']:
    print(f'  {bucket["Name"]}')
# print(response)

# take csv from local upload to s3
# if file exists with same name then delete and then upload again
# pick the files from s3 and search : if exists true else false
# if file exists read content and write to s3 another csv file



