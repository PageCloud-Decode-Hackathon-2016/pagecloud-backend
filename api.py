from collections import Counter
from flask import Flask
from flask_restful import Resource, Api
from elasticsearch import Elasticsearch
from elasticsearch_dsl import Search, Q
from urlparse import urlparse
from robot_detection import is_robot

app = Flask(__name__)
api = Api(app)

client = Elasticsearch(host='search-pagecloud-legacy-decode-2016-oijvlfnyaac4p6h2kimvltek24.us-east-1.es.amazonaws.com',
                       port=443,
                       use_ssl=True,
                       verify_certs=False)

class Referrers(Resource):
    def get(self):
        freq = Counter()
        results = {
        	'data': {
        		'referrers': []
        	}
        }

        s = Search(using=client, index='production-logs-*')\
            .fields(['referrer'])\
            .query('match_all')

        response = s.execute().to_dict()

        for hit in response['hits']['hits']:
            url = urlparse(hit['fields']['referrer'][0].replace('"', '')).netloc
            freq[url] += 1

        freq = freq.most_common(None)
        for entry in freq:
            site, count = entry
            results['data']['referrers'].append(
                {
                    'name': site,
                    'count': count
                })

        return results

class Geo(Resource):
    def get(self):
        results = {
        	'data': {
        		'geo': []
        	}
        }

        s = Search(using=client, index='production-logs-*')\
            .fields(['geoip.country_code3'])\
            .query('match_all')

        response = s.execute().to_dict()

        cntry = Counter()

        for hit in response['hits']['hits']:
            cntry[hit['fields']['geoip.country_code3'][0]] +=1

        cntry = cntry.most_common(None)

        for entry in cntry:
            country, count = entry
            results['data']['geo'].append(
        		{
        			'country': country,
        			'count': count
        		})

        return results

class Bots(Resource):
    def get(self):
        pass

class Path(Resource):
    def get(self):
        results = {
        	'data': {
        		'path': []
        	}
        }

        s = Search(using=client, index='production-logs-*')\
            .fields(['request', 'clientip', 'timestamp'])\
            .query('match_all')

        response = s.execute().to_dict()

        clientVisits = []

        #Create Sequences by grouping Client IP
        for request in response['hits']['hits']:

        #
        return response

api.add_resource(Referrers, '/referrers')
api.add_resource(Geo, '/geo')
api.add_resource(Bots, '/bots')
api.add_resource(Path, '/path')

if __name__ == '__main__':
    app.run(host='0.0.0.0',debug=True)
