#/usr/bin/python3
from flask import Flask
from flask import request
from werkzeug.datastructures import Headers
import lib
import mimetypes
import struct
import re

from multiprocessing.dummy import Pool as ThreadPool

app = Flask(__name__, static_url_path='')


#support [mimetypes, range bytes, streamread]
@app.route("/get/<name>", methods=['GET'])
def get(name):
    type, encoding = mimetypes.guess_type(name)
    try:
        data = bytearray()
        meta_file_name = "b:" + name
        ret = lib.Get(bytes(meta_file_name, "utf8"))
        headers = Headers()
        headers.add('Content-Type', type)
        headers.add('Content-Encoding', encoding)

        if len(ret.value) == 4:
            size = struct.unpack("!I", ret.value[0:4])[0]
            if request.headers.has_key("Range"):#suport Range get
                headers.add('Accept-Ranges','bytes')
                ranges = re.findall(r"\d+", request.headers["Range"])
                begin = int(ranges[0])
                end = size -1   
                if len(ranges) > 1:
                    end = int(ranges[1])
                headers.add('Content-Range', 'bytes %d-%d/%d' % (begin, end, size))
                beginIdx = int(begin / chunk_size)
                endIdx = int(end / chunk_size)
                names = ["a:" + name + ":" + str(int(i)) for i in range(beginIdx, endIdx+1)]
                #define a function to get chunk
                def get_partial_chunk():
                    for i, name in enumerate(names):
                        ret = lib.Get(bytes(name, "utf8"))
                        if i == 0:
                            yield ret.value[begin % chunk_size:]
                        elif i == endIdx:
                            if (end + 1) % chunk_size == 0:
                                yield ret.value
                            else:
                                yield ret.value[:(end + 1)% chunk_size]
                        else:
                            yield ret.value
                return app.response_class(get_partial_chunk(),206, headers=headers)
            else:#normal get
                #if ret is chunked file. get all chunks one by one
                #read chunk size
                names = ["a:" + name + ":" + str(int(i/chunk_size)) for i in range(0, size, chunk_size)]
                headers.add("Content-Length", size)

                def download_chunk():
                    for name in names:
                        x = lib.Get(bytes(name, "utf8"))
                        yield x.value
            return app.response_class(download_chunk(), headers=headers)
        else:
            #ignore the first 4 bytes
            data.extend(ret.value[4:])
            headers.add("Content-Length", len(data))
        return bytes(data), 200, headers
    except Exception as e:
        print(e)
        return str(e)

@app.route("/list/", methods=['GET'])
def list():
    try:
        t = ""
        for e in lib.List(b"b:", b"b:", (1<<32)-1):
            t += e[2:] + "\n"
        return t, 200, {"Content-Type": "text/plain"}
    except Exception as e:
        return str(e)

chunk_size = 1 * 1024 * 1024

def split(filename, data):
    ## split file into chunks
    ## each filename is "a:"+filename
    i = 0 
    for offset in range(0, len(data), chunk_size):
        chunk_name = "a:" + filename + ":" + str(i)
        yield chunk_name, data[offset:offset + chunk_size]
        i += 1

@app.route("/put/", methods=['POST'])
def put():
    file = request.files['file']
    data = file.read()
    #if len of data is bigger than chunk_size, split data into chunks, and put data one by one
    if len(data) > chunk_size:
        for chunk_name, chunk in split(file.filename, data):
            lib.Put(bytes(chunk_name, "utf8"), chunk)
        #meta file name is "b:" + filename
        meta_file_name = "b:" + file.filename
        meta_value = struct.pack("!I", len(data)) ##big endian u32
        lib.Put(bytes(meta_file_name, "utf8"), meta_value)
    else:
        #concat struct.pack("!I", len(data)) and data
        value = struct.pack("!I", len(data)) + data
        lib.Put(bytes("b:"+ file.filename, "utf8"), value)
    return file.filename

@app.route("/", methods=['GET'])
def index():
    return app.send_static_file('index.html')

@app.route("/<path:path>")
def static_files(path):
    return Flask.send_from_directory('static', path)

lib = lib.AutumnLib()

if __name__ == '__main__':
    lib.Connect()
    app.run(host='0.0.0.0', port=5000, threaded=True)
