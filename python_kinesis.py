import testdata
import json
import time
import boto3
import json

session = boto3.Session()
credentials = session.get_credentials()
kinesis = boto3.client('kinesis')

stream = kinesis.create_stream(
    StreamName="BotoDemo",
    ShardCount=1
)
# kinesis.describe_stream(StreamName="BotoDemo")
# kinesis.list_streams()

# PUT INTO KINESIS


class Users(testdata.DictFactory):
    firstname = testdata.FakeDataFactory('firstName')
    lastname = testdata.FakeDataFactory('lastName')
    age = testdata.RandomInteger(10, 30)
    gender = testdata.RandomSelection(['female', 'male'])

# data =  {
#   'firstname': 'adrian',
#   'lastname': 'hornsby',
#   'age': '37',
#   'gender': 'male'
# }


for user in Users().generate(50):
    print(user)
    kinesis.put_record(
        StreamName="BotoDemo",
        Data=json.dumps(user),
        PartitionKey="partitionkey"
    )

# Batch input into kinesis
i = 0
records = []
for user in Users().generate(50):
    i = i + 1
    record = {
        'Data': json.dumps(user),
        'PartitionKey': str(hash(user["age"]))
    }
    records.append(record)
    if i % 5 == 0:
            kinesis.put_records(
                Records=records,
                StreamName="BotoDemo"
            )
            records = []

# READ FROM KINESIS

shard_id = 'shardId-000000000000'  # we only have one shard!
shard_it = kinesis.get_shard_iterator(
    StreamName="BotoDemo",
    ShardId=shard_id,
    ShardIteratorType="LATEST")["ShardIterator"]


while 1 == 1:
    out = kinesis.get_records(
        ShardIterator=shard_it,
        Limit=2
    )
    for o in out["Records"]:
        jdat = json.loads(o["Data"])
        print jdat
    shard_it = out["NextShardIterator"]
    print out
    time.sleep(0.2)


# DELETE kinesis stream
kinesis.delete_stream(
    StreamName="BotoDemo"
)
