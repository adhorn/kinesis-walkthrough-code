import boto3
import json

session = boto3.Session()
credentials = session.get_credentials()
FIREHOSE_STREAM = 'YOUR_STREAM_NAME'
firehose = boto3.client('firehose')

response = firehose.create_delivery_stream(
    DeliveryStreamName=FIREHOSE_STREAM,
    S3DestinationConfiguration={
        'RoleARN': 'arn:aws:iam::YOUR_ACCOUNT_NUMBER:role/YOUR_ROLE_NAME',
        'BucketARN': 'arn:aws:s3:::YOUR_BUCKET_NAME',
        'Prefix': 'raw',
        'BufferingHints': {
            'SizeInMBs': 1,
            'IntervalInSeconds': 60
        },
        'CompressionFormat': 'UNCOMPRESSED',
        'EncryptionConfiguration': {
            'NoEncryptionConfig': 'NoEncryption'
        },
        'CloudWatchLoggingOptions': {
            'Enabled': False
        }
    })

data = {
  u'raw_agent': u'Mozilla/5.0',
  u'ip': u'127.0.0.1',
  u'@timestamp': u'2017-09-11 21:51:36',
  u'request': u'GET /api/echo',
  u'agent': u'macos | chrome 60.0.3112.113',
  u'user': u'guest',
  u'status_code': u'200',
  u'query': u'',
  'country': 'unkown',
  'location': [0.0, 0.0]
}

response = firehose.put_record(
    DeliveryStreamName=FIREHOSE_STREAM,
    Record={'Data': json.dumps(data)}
)

# Batch into firehose
response = client.put_record_batch(
    DeliveryStreamName=FIREHOSE_STREAM,
    Records=[
        {
            'Data': json.dumps(data)
        },
    ]
)

#  Delete firehose stream
firehose.delete_delivery_stream(
    DeliveryStreamName=FIREHOSE_STREAM
)
