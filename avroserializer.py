import io

from avro.datafile import DataFileWriter
from avro.io import BinaryDecoder, BinaryEncoder, DatumReader, DatumWriter

# Path to user.avsc avro schema
write_to_avro_path = "tmp_feature.avro"

def write_avro_file_no_schema(content, schema):
    writer = DatumWriter(schema)
    bytes_writer = io.BytesIO()
    encoder = BinaryEncoder(bytes_writer)
    writer.write(content, encoder)
    raw_bytes = bytes_writer.getvalue()
    return raw_bytes


def write_avro_file_with_schema(content, schema):
    with open(write_to_avro_path, "wb") as in_file:
        writer = DataFileWriter(in_file, DatumWriter(), schema)
        writer.append(content)
        writer.close()
        
    with open(write_to_avro_path, "rb") as in_file:
        data = in_file.read()
        return data


def read_avro_byte_no_schema(raw_bytes, schema):
    bytes_reader = io.BytesIO(raw_bytes)
    decoder = BinaryDecoder(bytes_reader)
    reader = DatumReader(schema)
    user1 = reader.read(decoder)
    print("{}".format(user1))


# content = get_json()
# bytes = write_avro_file_no_schema(content)
# print(f"No schema: {len(bytes)} bytes")

# write_avro_file_with_schema(content)
# file_size = os.path.getsize(avro_path)
# print(f"With schema: : {file_size} bytes")
