import os
import time
from influxdb_client_3 import InfluxDBClient3, Point

token = "K--7f1I33vqCWrOcMXooQyQcg40_2lChOE44QKv_df3fZD3So5oDWuVx3OPQTYeTmGztHm8myMqP0JDQ_xU-gw=="
org = "691"
host = "https://us-east-1-1.aws.cloud2.influxdata.com"

client = InfluxDBClient3(host=host, token=token, org=org)

query = """
SELECT 
    DATE_BIN(INTERVAL '5 minutes', time) AS _time,
    mean("temperature") AS "avg_temperature",
    device_id
    FROM "weather"
    GROUP BY _time, device_id
"""


def execute_query(query):
    table = client.query(query=query, database="iot_data", language='sql')
    df = table.to_pandas().sort_values(by="_time")
    print(df)
    return df


# Run the query every 5 minutes indefinitely
while True:
    execute_query(query)
    time.sleep(300)  # Sleep for 300 seconds (5 minutes)
