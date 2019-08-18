import json
import time
import boto3
import datetime
import calendar

# athena constant
DATABASE = "locationdata"
TABLE = "labeleddatatable"

# S3 constant
S3_OUTPUT = 's3://location-data-query-results/unstructured/'

S3_BUCKET = 'location-data-query-results'

# number of retries
RETRY_COUNT = 10
    
def lambda_handler(event, context):

    def execute_query(query):
        client = boto3.client('athena')
        
        # Execution
        response = client.start_query_execution(
            QueryString=query,
            QueryExecutionContext={
                'Database': DATABASE
            },
            ResultConfiguration={
                'OutputLocation': S3_OUTPUT,
            }
        )
        
        # get query execution id
        query_execution_id = response['QueryExecutionId']
        
        # get execution status
        for i in range(1, 1 + RETRY_COUNT):
            
            # get query execution
            query_status = client.get_query_execution(QueryExecutionId=query_execution_id)
            query_execution_status = query_status['QueryExecution']['Status']['State']
            
            if query_execution_status == 'SUCCEEDED':
                print("STATUS:" + query_execution_status)
                # get query results
                result = client.get_query_results(QueryExecutionId=query_execution_id)
                return result
            
            if query_execution_status == 'FAILED':
                raise Exception("STATUS:" + query_execution_status)
            
            else:
                print("STATUS:" + query_execution_status)
                time.sleep(i)
        else:
            client.stop_query_execution(QueryExecutionId=query_execution_id)
            raise Exception('TIME OVER')
    
    def save_file(file_name, dict_obj):
        file_path = 'structured/' + file_name + '.json'
        s3 = boto3.resource("s3").Bucket(S3_BUCKET)
        json.dump_s3 = lambda obj, f: s3.Object(key=f).put(Body=json.dumps(obj))
        
        json.dump_s3(dict_obj, file_path)
        
        return
    
    def prepare_table():
        query = 'MSCK REPAIR TABLE %s.%s' % (DATABASE, TABLE)
        
        result = execute_query(query)
        
        print(result)
        
        return
    
    def dau_graph():
        query = 'SELECT year, month, day, COUNT(DISTINCT uuid) FROM "%s"."%s" AS daus GROUP BY year, month, day ORDER BY year, month, day DESC LIMIT 7' % (DATABASE, TABLE)

        result = execute_query(query)
        
        days = []
        counts = []
        
        counter = 0
        for row in result['ResultSet']['Rows']:
            if counter > 0:
                year = row['Data'][0]['VarCharValue']
                month = row['Data'][1]['VarCharValue']
                date = row['Data'][2]['VarCharValue']
                counts.append(int(row['Data'][3]['VarCharValue']))

                days.append(str(calendar.day_name[datetime.datetime(int(year), int(month), int(date)).weekday()]))
            counter += 1
            
        # reverse counts and days so they appear the right way on dashboard
        counts.reverse()
        days.reverse()
        
        results_dict = {}
        results_dict["data"] = []
        
        structured_results = {}
        structured_results["label"] = "DAUs Over Time"
        structured_results["data"] = counts
        structured_results["color"] = "#e74c3c"
        structured_results["labels"] = days
        
        results_dict["data"].append(structured_results)
        
        save_file("dau-graph", results_dict)
        
        return
    
    def mau_graph():
        query = 'SELECT year, month, COUNT(DISTINCT uuid) AS maus FROM "%s"."%s" GROUP BY year, month ORDER BY year, month DESC LIMIT 12' % (DATABASE, TABLE)
        
        result = execute_query(query)
        
        months = []
        counts = []
        
        counter = 0
        for row in result['ResultSet']['Rows']:
            if counter > 0:
                year = row['Data'][0]['VarCharValue']
                month = row['Data'][1]['VarCharValue']
                counts.append(int(row['Data'][2]['VarCharValue']))

                months.append(str(calendar.month_abbr[int(month)]))
            counter += 1
        
        # flip counts and months so they appear forward on dashboard
        months.reverse()
        counts.reverse()
        
        results_dict = {}
        results_dict["data"] = []
        
        structured_results = {}
        structured_results["label"] = "MAUs Over Time"
        structured_results["data"] = counts
        structured_results["color"] = "#e74c3c"
        structured_results["labels"] = months
        
        results_dict["data"].append(structured_results)
        
        save_file("mau-graph", results_dict)
        
        return
    
    def dau_today():
        today = datetime.datetime.today()
        year = today.year
        month = today.month
        day = today.day
        
        query = 'SELECT COUNT(DISTINCT uuid) AS daus FROM "%s"."%s" WHERE year=%s AND month=%s AND day=%s' % (DATABASE, TABLE, int(year), int(month), int(day))
        
        result = execute_query(query)
        
        dau = int(result['ResultSet']['Rows'][1]['Data'][0]['VarCharValue'])
        
        results_dict = {}
        results_dict["data"] = {"value": dau}
        
        save_file("dau-today", results_dict)
        
        return
    
    def points_today():
        today = datetime.datetime.today()
        year = today.year
        month = today.month
        day = today.day
        
        query = 'SELECT COUNT(*) AS points FROM "%s"."%s" WHERE year=%s AND month=%s AND day=%s' % (DATABASE, TABLE, year, month, day)
        
        result = execute_query(query)
        
        points = int(result['ResultSet']['Rows'][1]['Data'][0]['VarCharValue'])
        
        results_dict = {}
        results_dict["data"] = {"value": points}
        
        save_file("points-today", results_dict)
    
    def total_visited():
        query = 'SELECT COUNT(DISTINCT name) AS numplaces FROM "%s"."%s"' % (DATABASE, TABLE)
        
        result = execute_query(query)
        
        num_places = int(result['ResultSet']['Rows'][1]['Data'][0]['VarCharValue'])
        
        results_dict = {}
        results_dict["data"] = {"value": num_places}
        
        save_file("total-visited", results_dict)
        
        return
        
    def top_visited():
        query = 'SELECT name, COUNT(*) AS numplaces FROM "%s"."%s" GROUP BY name ORDER BY numplaces DESC LIMIT 10' % (DATABASE, TABLE)
        
        result = execute_query(query)
        
        results_dict = {}
        results_dict["data"] = []
        
        counter = 0
        for row in result['ResultSet']['Rows']:
            if counter > 0:
                name = row['Data'][0]['VarCharValue']
                count = int(row['Data'][1]['VarCharValue'])
                results_dict["data"].append({"label": name, "value": count})

            counter += 1
        
        save_file("top-visited", results_dict)
        
        return
    
    def total_points():
        query = 'SELECT COUNT(*) AS points FROM "%s"."%s"' % (DATABASE, TABLE)
        
        result = execute_query(query)
        
        points = int(result['ResultSet']['Rows'][1]['Data'][0]['VarCharValue'])
        
        results_dict = {}
        results_dict["data"] = {"value": points}
        
        save_file("total-points", results_dict)
        
        return
    
    
    prepare_table()
    dau_graph()
    mau_graph()
    dau_today()
    points_today()
    total_visited()
    top_visited()
    total_points()
    
    return {
        'statusCode': 200,
        'body': json.dumps('Hello from Lambda!')
    }
