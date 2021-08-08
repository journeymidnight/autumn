from flask import Flask
from flask import request
import lib

app = Flask(__name__, static_url_path='')

@app.route("/get/<name>", methods=['GET'])
def get(name):
    try:
        ret = lib.Get(bytes(name, "utf8"))
        return ret.value
    except Exception as e:
        return str(e)

@app.route("/put/", methods=['POST'])
def put():
    file = request.files['file']
    data = file.read()
    lib.Put(bytes(file.filename, "utf8"), data)
    return file.filename

@app.route("/<path:path>")
def static_files(path):
    return Flask.send_from_directory('static', path)

lib = lib.AutumnLib()
if __name__ == '__main__':
    lib.Connect()
    app.run(host='0.0.0.0', port=5000, threaded=True)