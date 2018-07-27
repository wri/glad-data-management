from flask import Flask, request, jsonify
from flask_cors import CORS

app = Flask(__name__)
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

    print 'GOT POST'
    geojson = request.get_json().get('geojson', None).get('features', None) if request.get_json() else None
    print geojson

    resp = calc_stats(geojson)


    return jsonify(resp)


if __name__ == '__main__':
    app.run()
