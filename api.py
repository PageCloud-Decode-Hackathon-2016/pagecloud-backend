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
        counts = Counter()
        results = []

        s = Search(using=client, index='production-logs-*')\
            .fields(['referrer'])\
            .query('match_all')

        for hit in s.scan():
            response = hit.to_dict()
            url = urlparse(response.get('referrer', [''])[0].replace('"', '')).netloc

            if url[:4] == 'www.':
                url =  url[4:]

            counts[url.lower()] += 1

        for site in counts.keys():
            results.append({
                'name': site,
                'count': counts[site]
            })

        return {
            'data': {
                'referrers': results
            }
        }


class Geo(Resource):
    def get(self):
        results = []
        countries = Counter()

        s = Search(using=client, index='production-logs-*')\
            .fields(['geoip.country_code3'])\
            .query('match_all')

        for hit in s.scan():
            response = hit.to_dict()
            c = response.get('geoip.country_code3', [''])[0]
            countries[c.upper()] += 1

        for country in countries.keys():
            results.append({
                'country': country,
                'count': countries[country]
            })

        return {
            'data': {
                'geo': results
            }
        }


class Bots(Resource):
    def get(self):
        results = {
            'data': {
                'bots': []
            }
        }

        s = Search(using=client, index='production-logs-*')\
            .fields(['agent'])\
            .query('match_all')

        response = s.execute().to_dict()
        agents = Counter()

        for hit in response['hits']['hits']:
            agents[hit['fields']['agent'][0]] +=1

        agents = agents.most_common(None)

        for entry in agents:
            ageent, count = entry
            results['data']['bots'].append({
                'country': country,
                'count': count
            })

        return results


api.add_resource(Referrers, '/referrers')
api.add_resource(Geo, '/geo')
api.add_resource(Bots, '/bots')

if __name__ == '__main__':
    app.run(host='0.0.0.0',debug=True)
