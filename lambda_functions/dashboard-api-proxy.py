import json
import boto3

def lambda_handler(event, context):
    bad_request = {
        'statusCode': 400,
        'headers': {
            "Access-Control-Allow-Origin" : "*", 
            "Access-Control-Allow-Credentials" : True
        },
        'body':json.dumps('Bad Request')
    }
    success_request = {
        'statusCode': 200,
        'headers': {
            "Access-Control-Allow-Origin" : "*", 
            "Access-Control-Allow-Credentials" : True
        },
        'body':json.dumps('Success')
    }
    
    s3 = boto3.client("s3")
    
    def check_parameters(event):
        print(event)
        if 'path' in event:
            path = event['path'].split('/')[-1]
            if (path == 'dau-graph' or path == 'dau-today' or path == 'mau-graph'
                or path == 'points-today' or path == 'top-visited'
                or path == 'total-points' or path == 'total-visited'):
                    return get_file(path)
            else:
                return bad_request
        else:
            return bad_request
    
    def get_file(path):
        filename = 'structured/' + path + '.json'
        fileObj = s3.get_object(Bucket='location-data-query-results', Key=filename)
        data = json.loads(fileObj['Body'].read().decode('utf-8'))
        data = data["data"]
        
        response = {
            'statusCode': 200,
            'headers': {
                "Access-Control-Allow-Origin" : "*", 
                "Access-Control-Allow-Credentials" : True
            },
            'body': json.dumps(data)
        }
        return response
        
    
    return check_parameters(event)
