import io
import json
import os

import avro.schema
from avro.datafile import DataFileWriter
from avro.io import BinaryDecoder, BinaryEncoder, DatumReader, DatumWriter

# Path to user.avsc avro schema
schema_path = "feature.avsc"
avro_path = "feature.avro"
json_path = "feature.json"
schema = avro.schema.parse(open(schema_path).read())


def write_avro_file_no_schema():
    writer = DatumWriter(schema)
    bytes_writer = io.BytesIO()
    encoder = BinaryEncoder(bytes_writer)
    writer.write(get_json(), encoder)
    raw_bytes = bytes_writer.getvalue()
    return raw_bytes


def write_avro_file_with_schema():
    writer = DataFileWriter(open(avro_path, "wb"), DatumWriter(), schema)
    writer.append(get_json())
    writer.close()
    file_size = os.path.getsize(avro_path)
    print("File Size is :", file_size, "bytes")


def read_avro_byte_no_schema(raw_bytes):
    bytes_reader = io.BytesIO(raw_bytes)
    decoder = BinaryDecoder(bytes_reader)
    reader = DatumReader(schema)
    user1 = reader.read(decoder)
    print("{}".format(user1))


def get_json():
    f = open(json_path)
    return json.load(f)


bytes = write_avro_file_no_schema()
print(f"byte length: {len(bytes)}")

# read_avro_byte_no_schema(bytes)

write_avro_file_with_schema()
