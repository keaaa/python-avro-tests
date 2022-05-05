import asyncio
import json
import math
import os

import avro.schema

from avroserializer import write_avro_file_with_schema
from eventhub import send_to_event_hub

json_path = "feature_short.json"
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


avro_as_bytes = create_nan_issue()
loop = asyncio.get_event_loop()
loop.run_until_complete(send_to_event_hub(conn_str, avro_as_bytes))
