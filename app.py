import asyncio
import json
import math
import os
import time

import avro.schema

from avroserializer import write_avro_file_with_schema
from eventhub import send_to_event_hub

json_path = "feature.json"
schema_path = "feature.avsc"
conn_str = os.getenv("EVENT_HUB_CONN_STR")


def get_json(path):
    f = open(path)
    return json.load(f)


def create_nan_issue():
    content = get_json(json_path)
    content["data"][0] = math.nan
    schema = avro.schema.parse(open(schema_path).read())
    return write_avro_file_with_schema(content, schema)


async def simulate_load(sample_time_interval_sec, run_for_sec):
    schema = avro.schema.parse(open(schema_path).read())
    start_time = int(time.time() * 1000000000)
    run_to_time = start_time + run_for_sec * 1000000000
    content = get_json(json_path)
    while start_time < run_to_time:
        content["measurementStartTime"] = start_time
        avro_binary = write_avro_file_with_schema(content, schema)
        await send_to_event_hub(conn_str, avro_binary)
        time.sleep(sample_time_interval_sec)
        start_time = int(time.time() * 1000000000)


avro_as_bytes = create_nan_issue()
loop = asyncio.get_event_loop()
# loop.run_until_complete(send_to_event_hub(conn_str, avro_as_bytes))
loop.run_until_complete(simulate_load(1, 120))
