import struct
import sys

print struct.unpack_from(">Q",sys.stdin.readlines()[1])
