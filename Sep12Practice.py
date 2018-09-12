from __future__ import print_function
import boto3
from botocore.client import Config
import json
import decimal

ACCESS_KEY_ID = 'AKIAJWKDXBAPBPXWTLUQ'
ACCESS_SECRET_KEY = '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'
BUCKET_NAME = 'patients1'

s3 = boto3.resource('s3',
                    region_name='us-east-1',
                    aws_access_key_id=ACCESS_KEY_ID,
                    aws_secret_access_key=ACCESS_SECRET_KEY,
                    config=Config(signature_version='s3v4'))

dynamodb = boto3.resource(
    'dynamodb',
    region_name='us-east-1',
    aws_access_key_id=ACCESS_KEY_ID,
    aws_secret_access_key=ACCESS_SECRET_KEY,
    config=Config(signature_version='s3v4'))

table = dynamodb.Table('sep5table')


def lambda_handler(event, context):
    for record in event['Records']:

        s3record = event['Records'][0]['Sns']['Message']

        s3object = json.loads(s3record)
        bucket = s3object['Records'][0]['s3']['bucket']['name']
        key = s3object['Records'][0]['s3']['object']['key']

        episodefile = s3.Object(bucket, key)

        episodedata = episodefile.get()['Body'].read()
        episodejson = json.loads(episodedata)

        s3patient = s3.Object('patients1', 'Patient/Patient.json')

        s3patientdata = s3patient.get()['Body'].read()
        s3patientloads = json.loads(s3patientdata)

        # episodes = json.load(json_file, parse_float=decimal.Decimal)
        # for episode in episodes:

        # print(record)

        # with s3patient as json_file:
        #     for episode in s3patient:

        if 'episodeId' in episodejson:
            EID = episodejson['episodeId']

        FN = ''

        if 'name'[0] in s3patientloads:
            FN = s3patientloads['name'][0]['given'][0]

        LN = ''

        if 'name'[0] in s3patientloads:
            LN = s3patientloads['name'][0]['family']

        PA1 = ''

        if 'address'[0] in s3patientloads:
            PA1 = s3patientloads['address'][0]['line']

        PAC = ''

        if 'address'[0] in s3patientloads:
            PAC = s3patientloads['address'][0]['city']

        PAS = ''

        if 'address'[0] in s3patientloads:
            PAS = s3patientloads['address'][0]['state']

        # PAZ = ''

        # if 'address'[0] in s3patientloads:
        PAZ = s3patientloads['address'][0]['postalCode']

        # EID = ['Records']['episodeId']
        # FN = jsonobject['name']['given']

        print("below is print of PAZ: ")
        print(s3patientloads['address'][0]['postalCode'])
        print(PAZ)

        table.put_item(
            Item={
                'episodeId': EID,
                'firstName': FN,
                'lastName': LN,
                'address': PA1,
                'city': PAC,
                'zipCode': PAZ,
                'state': PAS
            }
        )