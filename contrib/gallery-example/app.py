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


@app.route('/del/<name>', methods=['DELETE'])
def delete(name):
    meta_file_name = "b:" + name
    ret = lib.Get(bytes(meta_file_name, 'utf-8'))
    if ret is None:
        return "File not found", 404
    if len(ret.value) == 4:
        size = struct.unpack("!I", ret.value[0:4])[0]
        n = int(size / chunk_size)
        if size % chunk_size > 0:
            n += 1
        names = ["a:" + name + ":" + str(int(i)) for i in range(n)]
        for name in names:
            lib.Delete(bytes(name, 'utf-8'))
        lib.Delete(bytes(meta_file_name, 'utf-8'))
        return "OK", 200
    else:
        lib.Delete(bytes(meta_file_name, 'utf-8'))
        return "OK", 200

#GET API support [mimetypes, range bytes, streamread]
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
            if "Range" in request.headers:#support Range GET
                headers.add('Accept-Ranges','bytes')
                ranges = re.findall(r"\d+", request.headers["Range"])
                begin = int(ranges[0])
                end = size -1   
                if len(ranges) > 1:
                    end = int(ranges[1])
                headers.add('Content-Range', 'bytes %d-%d/%d' % (begin, end, size))
                beginIdx = int(begin / chunk_size)
                endIdx = int(end / chunk_size)
                #range is [beginIdx, endIdx]
                names = ["a:" + name + ":" + str(int(i)) for i in range(beginIdx, endIdx+1)]
                #define a function to get chunk
                def get_partial_chunk():
                    for i, name in enumerate(names):
                        ret = lib.Get(bytes(name, "utf8"))
                        if i == 0:
                            yield ret.value[begin % chunk_size:]
                        elif i == endIdx:
                            #正常返回: ret.value[:(end + 1)% chunk_size], 其中+1是因为end是最后一个的offset, 但是python的slice的冒号
                            #后是highvalue, 不包括最后一个, 所以用end+1
                            #特殊情况: 如果(end + 1)和chunk_size对齐, (end + 1)% chunk_size就是0, 不能得到数据
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

chunk_size = 4 * 1024 * 1024

def split(filename, data):
    ## split file into chunks
    ## each filename is "a:"+filename
    i = 0 
    for offset in range(0, len(data), chunk_size):
        chunk_name = "a:" + filename + ":" + str(i)
        yield chunk_name, data[offset:offset + chunk_size]
        i += 1



def read_full(stream, size):
    res = bytearray()
    n = size
    while n > 0:
        data = stream.read(n)
        if not data:
            break
        res += data
        n -= len(res)
    return bytes(res)


@app.route("/put/", methods=['POST'])
def put():
    file = request.files['file']
    #if len of data is bigger than chunk_size, split data into chunks, and put data one by one
    i = 0
    size = 0
    while True:
        chunk = read_full(file.stream, chunk_size)
        if chunk is None:
            #clean up data to i
            return "error", 500
        elif len(chunk) < chunk_size and i == 0:
            #small files
            value = struct.pack("!I", len(chunk)) + chunk
            lib.Put(bytes("b:"+ file.filename, "utf8"), value)
            return file.filename
        elif len(chunk) == 0:
            #end of file
            meta_file_name = "b:" + file.filename
            meta_value = struct.pack("!I", size) ##big endian u32
            lib.Put(bytes(meta_file_name, "utf8"), meta_value)
            return file.filename
        elif len(chunk) <= chunk_size:
            #middle of file
            filename = "a:" + file.filename + ":" + str(i)
            lib.Put(bytes(filename, "utf8"), chunk)
            i += 1
            size += len(chunk)
        else:
            print(len(chunk), chunk_size)
            return "file too big"

@app.route("/", methods=['GET'])
def index():
    return app.send_static_file('index.html')

@app.route("/<path:path>")
def static_files(path):
    return Flask.send_from_directory('static', path)

lib = lib.AutumnLib()

if __name__ == '__main__':
    lib.Connect()
    app.run(host='0.0.0.0', port=5001, threaded=True)
