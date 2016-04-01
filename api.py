from collections import Counter
from collections import defaultdict
from flask import Flask
from flask_restful import Resource, Api
from elasticsearch import Elasticsearch
from elasticsearch_dsl import Search, Q
import json
from urlparse import urlparse

app = Flask(__name__)
api = Api(app)

client = Elasticsearch(host='search-pagecloud-legacy-decode-2016-oijvlfnyaac4p6h2kimvltek24.us-east-1.es.amazonaws.com',
                       port=443,
                       use_ssl=True,
                       verify_certs=False)

class Referrers(Resource):
    def get(self):
        counts = defaultdict(int)
        results = []

        s = Search(using=client, index='production-logs-*')\
            .fields(['referrer'])\
            .query('match_all')

        response = s.execute().to_dict()

        for hit in response['hits']['hits']:
            url = urlparse(hit['fields']['referrer'][0].replace('"', '')).netloc
            counts[url] = counts.get(url, 0) + 1

        for site in counts.keys():
            results.append({
                'name': site,
                'count': counts[site]
            })

        return {
            'data': results
        }

class Geo(Resource):
    def get(self):
        results = []

        s = Search(using=client, index='production-logs-*') \
            .fields(['referrer']) \
            .query('match_all')

        response = s.execute().to_dict()

        for hit in response['hits']['hits']:
            url = urlparse(hit['fields']['referrer'][0].replace('"', '')).netloc
            counts[url] = counts.get(url, 0) + 1

        for site in counts.keys():
            results.append({
                'name': site,
                'count': counts[site]
            })

        return {
            'data': results
        }

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

api.add_resource(Referrers, '/referrers')
api.add_resource(Geo, '/geo')

if __name__ == '__main__':
    app.run(host='0.0.0.0',debug=True)
