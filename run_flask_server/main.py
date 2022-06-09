from flask import Flask, jsonify

app = Flask(__name__)


@app.route('/sampleurl', methods = ['GET'])
def samplefunction():
    #access your DB get your results here
    data = {"data":"Processed Data"}
    return jsonify(data)

if __name__ == '__main__':
    port = 8000 #the custom port you want
    app.run(host='0.0.0.0', port=port)