from msgpack import packb, unpackb
from snappy import compress, decompress

def encode(data):
    return compress(packb(data))

def decode(data):
    return unpackb(decompress(data))

