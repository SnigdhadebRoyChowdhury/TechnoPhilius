import boto3
from datetime import datetime, timedelta
import time
import pymongo

#Initializing the AWS PythonSDK client
client = boto3.client('logs', region_name='ap-south-1')

#Forming a simple query that will run on CloudWatch
query = "fields @timestamp, @message | sort @timestamp desc | limit 25"

#The Log group name from which we will be extracting our logs
log_group = '/aws/lambda/testfunc'

#Specifying the details to fetch the logs
start_query_response = client.start_query(
    logGroupName=log_group,
    startTime=int((datetime.today() - timedelta(days=20)).timestamp()),
    endTime=int(datetime.now().timestamp()),
    queryString=query,
)

#Fetching the query id
query_id = start_query_response['queryId']

response = None


while response is None or response['status'] == 'Running':
    print('Waiting for query to complete ...')
    time.sleep(1)
    response = client.get_query_results(
        queryId=query_id
    )

#Initializing the MongoDB details
uri = "mongodb://127.0.0.1:27017"
#Initializing the MongoDB client
client = pymongo.MongoClient(uri)

database = client['log_db'] #the database name
collection = database['cloudWatchLogs'] #the collection name
mongo_insert_list = []

#Creating a list for multi insert into MongoDB collection
for result in response['results']:
    temp_dict = {}
    for values in result:
        if values['field'] == '@timestamp':
            temp_dict['time'] = values['value']
        elif values['field'] == '@message':
            temp_dict['message'] = values['value']
    mongo_insert_list.append(temp_dict)

#Inserting data into MongoDB collection
collection.insert_many(mongo_insert_list)

mongo_result = collection.find()

for result in mongo_result:
    print(result)

#Closing the MongoDB connection
client.close()
