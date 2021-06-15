import psycopg2
import boto3
import base64
import json

def connection_info(secretId):
	session = boto3.session.Session()
	client = session.client(service_name='secretsmanager')

	get_secret_value_response = client.get_secret_value(SecretId=secretId)
	if 'SecretString' in get_secret_value_response:
		secret = json.loads(get_secret_value_response['SecretString'])
	else:
		secret = json.loads(base64.b64decode(get_secret_value_response['SecretBinary']))

	return secret

def lambda_handler(event, context):
    conn = connection_info("myRedshiftSecret")
    con=psycopg2.connect(dbname= conn['database'], host=conn['host'],
                         port= str(conn['port']), user= conn['username'], password= conn['password'])
    cur = con.cursor()
    
    query = 'CALL public.insert_into_final_table();'
    cur.execute(query)
    con.commit()
    cur.close()
    con.close()
    



