from collections import defaultdict
from collections import Counter
from elasticsearch import Elasticsearch
from elasticsearch_dsl import Search, Q
from flask import Flask
from flask_restful import Resource, Api
from robot_detection import is_robot
import time
from urlparse import urlparse

app = Flask(__name__)
api = Api(app)

client = Elasticsearch(host='search-pagecloud-legacy-decode-2016-oijvlfnyaac4p6h2kimvltek24.us-east-1.es.amazonaws.com',
                       port=443,
                       use_ssl=True,
                       verify_certs=False)

requests = []
for hit in Search(using=client, index='production-logs-*')\
                 .fields(['referrer', 'agent', 'geoip.country_code3'])\
                 .query('match_all')\
                 .scan():
    requests.append(hit.to_dict())

class Referrers(Resource):
    def get(self):
        counts = Counter()
        results = []

        for hit in requests:
            url = urlparse(hit.get('referrer', [''])[0].replace('"', '')).netloc

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

        for hit in requests:
            c = hit.get('geoip.country_code3', [''])[0]
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
        results = []
        agents = Counter()
        total = 0

        for req in requests:
            total += 1
            agent = req.get('agent', ['-'])[0].replace('"', '')

            if is_robot(agent) == False:
                continue

            agents[agent] += 1

        for agent in agents.keys():
            results.append({
                'name': agent,
                'count': agents[agent]
            })

        return {
            'data': {
                'bots': {
                    'count': len(results),
                    'data': results
                },
                'users': {
                    'count': total - len(results)
                }
            }
        }


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


# The most popular/visited pages on the website
class Pages(Resource):
# TODO: exclude bots, only check for page visits on UNIQUE visitors
# list of user agents -> https://github.com/monperrus/crawler-user-agents/blob/master/crawler-user-agents.json
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

        response = s.execute().to_dict()
        pages = Counter()

        for hit in response['hits']['hits']:
            pages[hit['fields']['request'][0]] +=1

        pages = pages.most_common(None)

        for entry in pages:
            page, count = entry
            results['data']['pages'].append(
                {
                    'name': page,
                    'hits': count,
                    'lastModified': 'IMPLEMENT ME' # how can we get page's last modified date?
                })

        return results

api.add_resource(Referrers, '/referrers')
api.add_resource(Geo, '/geo')
api.add_resource(Bots, '/bots')
api.add_resource(Path, '/path')
api.add_resource(Pages, '/pages')

if __name__ == '__main__':
    app.run(host='0.0.0.0',debug=True)
