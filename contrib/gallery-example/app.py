#/usr/bin/python3
from flask import Flask
from flask import request
import lib
import mimetypes
import struct

from multiprocessing.dummy import Pool as ThreadPool

app = Flask(__name__, static_url_path='')


def downloadPart(chunk_name):
    ret = lib.Get(bytes(chunk_name, "utf8"))
    return ret.value

@app.route("/get/<name>", methods=['GET'])
def get(name):
    try:
        data = bytearray()
        meta_file_name = "0:" + name
        ret = lib.Get(bytes(meta_file_name, "utf8"))
        if len(ret.value) == 4:
            #if ret is chunked file. get all chunks one by one
            #read chunk size
            size = struct.unpack("!I", ret.value[0:4])[0]
            #read chunk data
            i = 0
            #fixme: threading get chunks
            #multi thread get data from
            names = ["1:" + name + ":" + str(int(i/chunk_size)) for i in range(0, size, chunk_size)]
            pool = ThreadPool(2)
            results = pool.map(downloadPart, names)
            pool.close()
            pool.join()
            #concatenate all chunks
            for chunk in results:
                data.extend(chunk)
        else:
            #ignore the first 4 bytes
            data.extend(ret.value[4:])
        type, encoding = mimetypes.guess_type(name)
        return bytes(data), 200, {"Content-Type": type, "Content-Encoding": encoding}
    except Exception as e:
        print(e)
        return str(e)

@app.route("/list/", methods=['GET'])
def list():
    try:
        t = ""
        for e in lib.List(b"", b"0:", (1<<32)-1):
            t += e[2:] + "\n"
        return t, 200, {"Content-Type": "text/plain"}
    except Exception as e:
        return str(e)

chunk_size = 1 * 1024 * 1024

def split(filename, data):
    ## split file into chunks
    ## each filename is "1:"+filename
    i = 0 
    for offset in range(0, len(data), chunk_size):
        chunk_name = "1:" + filename + ":" + str(i)
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
        #meta file name is "0:" + filename
        meta_file_name = "0:" + file.filename
        meta_value = struct.pack("!I", len(data)) ##big endian u32
        lib.Put(bytes(meta_file_name, "utf8"), meta_value)
    else:
        #concat struct.pack("!I", len(data)) and data
        value = struct.pack("!I", len(data)) + data
        lib.Put(bytes("0:"+ file.filename, "utf8"), value)
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