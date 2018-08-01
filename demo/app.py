from flask import Flask, request, jsonify, Response, stream_with_context
from flask_cors import CORS
import requests

app = Flask(__name__)
app.url_map.strict_slashes = False
CORS(app)

# add the above directory for this example to work
import os
import sys
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

# import the actual module to do the analysis
from within_sql_query import calc_stats


# set this so flask doesn't complain
os.environ['FLASK_ENV'] = 'development'


@app.route('/glad-alerts', methods=['POST'])
def glad_alerts():

    geojson = request.get_json().get('geojson', None).get('features', None) if request.get_json() else None
    print geojson

    resp = calc_stats(geojson)

    return jsonify(resp)


@app.route('/glad-alerts/download', methods=['POST'])
def glad_download():

    posted_json = request.get_json()

    # http://flask.pocoo.org/snippets/118/
    url = 'https://3bkj4476d9.execute-api.us-east-1.amazonaws.com/dev/glad-alerts/download'
    req = requests.post(url, json=posted_json, stream=True)

    return Response(stream_with_context(req.iter_content(chunk_size=1024)), content_type = req.headers['content-type'])


if __name__ == '__main__':
    app.run()

