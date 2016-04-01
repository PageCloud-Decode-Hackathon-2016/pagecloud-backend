from flask import Flask
from flask_restful import Resource, Api
from elasticsearch import Elasticsearch
from elasticsearch_dsl import Search, Q
import json

app = Flask(__name__)
api = Api(app)

client = Elasticsearch(host='search-pagecloud-legacy-decode-2016-oijvlfnyaac4p6h2kimvltek24.us-east-1.es.amazonaws.com',
                       port=443,
                       use_ssl=True,
                       verify_certs=False)

class Referrers(Resource):
    def get(self):
        results = []

        s = Search(using=client, index='production-logs-*') \
            .fields(['agent', 'clientip', 'referrer', 'timestamp']) \
            .query('match_all')

        for row in s.scan():
            try:
                results.append({
                    'referrer': row.referrer[0]
                })
            except AttributeError as ex:
                print(ex)

        return results

api.add_resource(Requests, '/referrers')

if __name__ == '__main__':
    app.run(debug=True)
