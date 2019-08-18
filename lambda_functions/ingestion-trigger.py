import json
import urllib.request
import boto3
from io import BytesIO
from gzip import GzipFile
import datetime

def lambda_handler(event, context):
    
    def get_name_of_place(lat, long):
        '''queries google place API to match lat long with name of place'''

        URL = ("https://maps.googleapis.com/maps/api/place/nearbysearch/json?radius=25&location=" +
            str(lat) + "," + str(long) + "&key=" + API_KEY)
        
        response = urllib.request.urlopen(URL).read()
        data = json.loads(response.decode('utf-8'))
        
        if len(data['results']) > 0:
            print('got name string')
            return data['results'][0]['name']
        else:
            print('returned no name string')
            return ""
        
    s3 = boto3.client("s3")
    
    if event:
        file_obj = event["Records"][0]
        filename = str(file_obj['s3']['object']['key'])
        fileObj = s3.get_object(Bucket='event-pipe-data', Key=filename)
        bytestream = BytesIO(fileObj['Body'].read())
        file_content = GzipFile(mode='rb', fileobj=bytestream).read().decode('utf-8')
        
        file_content = file_content.replace('][', ',')
        
        data = json.loads(file_content)
        
        now = datetime.datetime.now()
        now_string = str(now).replace(' ', '_')
        year = str(now.year)
        month = str(now.month)
        day = str(now.day)
        bucket_name = 'visitation-labeled'
        file_name = now_string.replace('.','_')
        file_name = file_name.replace(':', '_')
        file_name = file_name.replace('-', '_')
        file_name += '.json'
        lambda_path = '/tmp/' + file_name
        s3_path = 'year=' + year + '/' + 'month=' + month + '/' + 'day=' + day + '/' + file_name
        
        with open(lambda_path, 'w') as f:
            for point in data:
                name = get_name_of_place(point['latitude'], point['longitude'])
                point['name'] = name
                point['arrival'] = ' '.join(point['arrival'].split(' ')[:-1])
                point['departure'] = ' '.join(point['departure'].split(' ')[:-1])
                f.write(json.dumps(point))
                f.write("\n")
        
        s3.upload_file(lambda_path, bucket_name, s3_path)
        
    
    return {
        'statusCode': 200,
        'body': json.dumps('Hello from Lambda!')
    }

