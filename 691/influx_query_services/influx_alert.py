import os
import time
from influxdb_client_3 import InfluxDBClient3, Point

token = "K--7f1I33vqCWrOcMXooQyQcg40_2lChOE44QKv_df3fZD3So5oDWuVx3OPQTYeTmGztHm8myMqP0JDQ_xU-gw=="
org = "691"
host = "https://us-east-1-1.aws.cloud2.influxdata.com"

client = InfluxDBClient3(host=host, token=token, org=org)

query = """
SELECT 
    device_id,
    AVG(temperature) AS avg_temperature
FROM 
    weather
WHERE 
    temperature > 30.0 AND  -- Checking if the temperature is above the threshold
    time >= NOW() - INTERVAL '5 minutes'  -- Focusing on the last 5 minutes
GROUP BY 
    device_id
"""


def execute_query(query):
    table = client.query(query=query, database="iot_data", language='sql')
    records = table.to_pandas()
    if len(records):  # Check if the records list is not empty
        print("Temperatures exceeded the threshold in the last 5 minutes:")
        for record in records:
            print(f'Device ID: {record["device_id"]}, Avg. Temperature: {record["_value"]}')
    else:
        print("No temperatures exceeded the threshold in the last 5 minutes.")


# Run the query every 5 minutes indefinitely
while True:
    execute_query(query)
    time.sleep(300)  # Sleep for 300 seconds (5 minutes)