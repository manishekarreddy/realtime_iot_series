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
  MAX("temperature") AS "max_temp",
  MIN("temperature") AS "min_temp",
  MEAN("temperature") AS "average_temp"
FROM "weather"
WHERE
  time >= now() - interval '1 hour' 
  
GROUP BY  "device_id"
"""


def execute_query(query):
    table = client.query(query=query, database="iot_data", language='sql')
    df = table.to_pandas()
    print(df)
    return df


# Run the query every 5 minutes indefinitely
while True:
    execute_query(query)
