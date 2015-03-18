"""
Author: S.J.R. van Schaik <stephan@synkhronix.com>

Abstraction to read and write chunks from and to files.
"""

from binascii import crc32
from collections import namedtuple
from os import SEEK_SET, SEEK_CUR, SEEK_END
from struct import Struct

class ChunkId:
    Commit = 0
    Node = 1
    Leaf = 2

class Chunk(object):
    def __init__(self, f):
        self.struct = Struct('<BLL')
        self.f = f
        self.offset = self.f.tell()
        self.parsed = False

    def _parse_header(self):
        self.offset = self.f.tell()
        data = self.f.read(self.struct.size)

        if len(data) < self.struct.size:
            raise EOFError('no more chunks available.')

        self._id, self.size, self.checksum = self.struct.unpack(data)
        self.parsed = True

    def get_id(self):
        if not self.parsed:
            self._parse_header()

        return self._id

    def get_size(self):
        if not self.parsed:
            self._parse_header()

        return self.size

    def verify(self):
        if not self.parsed:
            self._parse_header()

        return crc32(self.read()) == self.checksum

    def seek(self, pos, whence=SEEK_SET):
        self.f.seek(pos, whence)
        self.parsed = False

    def tell(self):
        if not self.parsed:
            return self.f.tell()

        return self.offset

    def read(self):
        if not self.parsed:
            self._parse_header()

        data = self.f.read(self.size)
        self.f.seek(-len(data), SEEK_CUR)
        
        return data

    def write(self, _id, data):
        self._id = _id
        self.size = len(data)
        self.checksum = crc32(data)
        self.parsed = True

        self.f.seek(0, SEEK_END)
        self.offset = self.f.tell()
        data = self.struct.pack(self._id, self.size, self.checksum) + data
        self.f.write(data)

    def next(self):
        self.seek(self.size, SEEK_CUR)
    
    def close(self):
        self.f.close()

