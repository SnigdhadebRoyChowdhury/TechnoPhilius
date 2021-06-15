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
    
    delete_query = 'DELETE FROM public.property_incident_details_stg;'
    cur.execute(delete_query)
    
    copy_query = """\
    COPY roy_schema.property_incident_details_stg from 's3://XXXXXXXXX-redshift/'  \
    iam_role 'arn:aws:iam::XXXXXXXXXXXX/RedshiftToS3'\
    csv IGNOREHEADER 1;\
    """
    cur.execute(copy_query)

    select_query = 'SELECT COUNT(*) FROM public.property_incident_details_stg;'
    cur.execute(select_query)
    res = int(str(cur.fetchall()).split(',')[0].split('(')[1])
    if res > 0:
        con.commit()
        print('Data was successfully inserted into the table')
        print('Number of rows inserted is '+str(res))
        return 'Success'
        cur.close()
        con.close()
    else:
        con.rollback()
        print('No data was inserted into the table')
        print('Number of rows inserted is 0')
        return 'Failure'
        cur.close()
        con.close()



