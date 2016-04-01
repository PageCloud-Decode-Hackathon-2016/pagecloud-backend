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

        # s = Search(using=client, index='production-logs-*')\
        #     .fields(['agent', 'clientip', 'referrer', 'timestamp'])\
        #     .query('match_all')

        s = Search(using=client, index='production-logs-*')\
            .fields(['referrer'])\
            .query('match_all')

        response = s.execute().to_dict()

        for hit in response['hits']['hits']:
        	results.append(hit['fields']['referrer'][0].replace('"', ''))

        return results

api.add_resource(Referrers, '/referrers')

if __name__ == '__main__':
    app.run(debug=True)
