from collections import Counter
from flask import Flask
from flask_restful import Resource, Api
from elasticsearch import Elasticsearch
from elasticsearch_dsl import Search, Q, A
from urlparse import urlparse
from robot_detection import is_robot
import requests
import re

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

        for referrer in counts.keys():
            results.append({
                'name': referrer,
                'count': counts[referrer]
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
            pass

        return response

        for entry in cntry:
            country, count = entry
            results['data']['geo'].append(
                {
                    'country': country,
                    'count': count
                })

        return results



# The most popular/visited pages on the website
class Pages(Resource):
# TODO: exclude bots, only check for page visits on UNIQUE visitors
# list of user agents -> https://github.com/monperrus/crawler-user-agents/blob/master/crawler-user-agents.json

# HOW TO GET LAST DATE MODIFIED
# 1. Access the page's /manifest.json
# 2. Get the pages/lastModified datetime (using Python Requests)
#   -> convert to dictionary and get the datetime, convert to a datetime object
    def get(self):
        results = {
            'data': {
                'pages': []
            }
        }
        # e.g. www.domain.com/page/ <-- 'request' provides you with '/page'
        s = Search(using=client, index='production-logs-*')\
            .fields(['request'])\
            .query('match_all')
        # return s.execute().to_dict()

        url = "http://decode-2016.pagecloud.io/"
        decodeManifest = requests.get(url + 'manifest.json')

        response = s.execute().to_dict()
        pages = Counter()

        for hit in response['hits']['hits']:
            pages[hit['fields']['request'][0]] +=1

        pages = pages.most_common(None)

        for entry in pages:
            page, count = entry
            

            match = re.search('(.*)\?', page)
            print match.group(1)

            results['data']['pages'].append(
                {
                    'name': page,
                    'hits': count
                    # 'lastModified': "TODO"
                })

        return results


class AggregationTestResource(Resource):
    def get(self):
        index = 'production-logs-*'

        search = Search(using=client, index=index) \
            .fields(['referrer', 'geoip.ip', 'http_host' ]) \
            .query("match", http_host='decode-2016.pagecloud.io') \
            .filter("range", **{'@timestamp': {'gte': 'now-7d'}}) \
            .params(search_type="count")

        day_aggregation = A('date_histogram', field='@timestamp', interval='hour', format='yyyy-MM-dd')
        search.aggs.bucket('per_day', day_aggregation)

        raw_buckets = search.execute().aggregations['per_day']['buckets']

        data = {}
        for bucket in raw_buckets:
            data[bucket['key']] = bucket['doc_count']

        return data

api.add_resource(Referrers, '/referrers')
api.add_resource(Geo, '/geo')
api.add_resource(Bots, '/bots')
api.add_resource(Path, '/path')
api.add_resource(Pages, '/pages')
api.add_resource(AggregationTestResource, '/aggtest')

if __name__ == '__main__':
    app.run(host='0.0.0.0',debug=True)
