import json
import urllib.request
import urllib.parse
import base64

def lambda_handler(event, context):
    
    def generate_visitation_data():
        random_points = [[42.28835693, -83.75088172], [42.297895,-83.7150378], [42.29003769,-83.70629844],[42.27663603,-83.74221037],[42.30309661,-83.72258449],[42.30812748,-83.74047564],[42.28422992,-83.74778329],[42.29448473,-83.76452435],[42.27514639,-83.71996192],[42.27479275,-83.70859713]]
        random_datetime = [["2019-08-13 15:10:27 +0000", "2019-08-13 15:15:27 +0000"], ["2019-08-13 15:07:27 +0000", "2019-08-13 15:12:27 +0000"], ["2019-08-13 15:09:27 +0000", "2019-08-13 15:14:27 +0000"], ["2019-08-13 15:04:27 +0000", "2019-08-13 15:09:27 +0000"], ["2019-08-13 15:02:27 +0000", "2019-08-13 15:07:27 +0000"], ["2019-08-13 15:12:27 +0000", "2019-08-13 15:17:27 +0000"], ["2019-08-13 15:05:27 +0000", "2019-08-13 15:10:27 +0000"], ["2019-08-13 15:05:27 +0000", "2019-08-13 15:10:27 +0000"], ["2019-08-13 15:05:27 +0000", "2019-08-13 15:10:27 +0000"], ["2019-08-13 15:05:27 +0000", "2019-08-13 15:10:27 +0000"]]
        data_list = []
        for i in range(10):
            arrival = random_datetime[i][0]
            departure = random_datetime[i][1]
            duration = 5
            horizontal_accuracy = 5.0
            latitude = random_points[i][0]
            longitude = random_points[i][1]
            uuid = "3B625DE8-A0FF-4329-850B-5539F9475482"
            datum = {"arrival": arrival, "departure": departure, "duration": duration, "horizontal_accuracy": horizontal_accuracy, "latitude": latitude, "longitude": longitude, "uuid": uuid}
            data_list.append(datum)
        return data_list
    
    
    generated_data = generate_visitation_data()

    DATA = {"Data":generated_data, "PartitionKey":1}
    print("RAW DATA:", DATA)
    DATA = json.dumps(DATA).encode("utf-8")

    req = urllib.request.Request(url='https://5q2qy4mmbf.execute-api.us-east-1.amazonaws.com/test/streams/event-pipe/record', data=DATA, method='PUT')
    req.add_header('Content-Type', 'application/json')
    f = urllib.request.urlopen(req)
    
    raw_data = f.read()
    encoding = f.info().get_content_charset('utf8')  # JSON default
    data = json.loads(raw_data.decode(encoding))
    print(data)

    
    
    
    return {
        'statusCode': 200,
        'body': json.dumps('Hello from Lambda!')
    }
