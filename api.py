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

class Unique(Resource):
    def get(self):
    
        results = {
            'data':{
                'unique':[],
                'nonunique':[]
            }
        }

        # HOUR
        hourSearch = Search(using=client, index='production-logs-*')\
        .fields(['clientip','timestamp'])\
        .query('match_all')\
        #.filter("range", **{'@timestamp': {'gte': 'now-1h'}})
        
        hourDict = hourSearch.exclude().to_dict()['hits']['hits']
        results['data']['nonunique'].append({
            'datetime': 'hour',
            'count': len(hourDict)
        })
        
        hourCounter = Counter()
        for i in hourDict:
            hourCounter[i['fields']['clientip'][0]]+=1
        
        results['data']['unique'].append({
            'datetime': 'hour',
            'count': len(hourCounter)
        })
        
         # DAY
        daySearch = Search(using=client, index='production-logs-*')\
        .fields(['clientip','timestamp'])\
        .query('match_all')\
        #.filter("range", **{'@timestamp': {'gte': 'now-1d/d'}})
        
        dayDict = daySearch.exclude().to_dict()['hits']['hits']
        results['data']['nonunique'].append({
            'datetime': 'days',
            'count': len(dayDict)
        })
        
        dayCounter = Counter()
        for i in dayDict:
            dayCounter[i['fields']['clientip'][0]]+=1
        
        results['data']['unique'].append({
            'datetime': 'days',
            'count': len(dayCounter)
        })
        
        # WEEK 
        weekSearch = Search(using=client, index='production-logs-*')\
        .fields(['clientip','timestamp'])\
        .query('match_all')\
        #.filter("range", **{'@timestamp': {'gte': 'now-7d/d'}})
        
        weekDict = weekSearch.exclude().to_dict()['hits']['hits']
        results['data']['nonunique'].append({
            'datetime': 'week',
            'count': len(weekDict)
        })
        
        weekCounter = Counter()
        for i in weekDict:
            weekCounter[i['fields']['clientip'][0]]+=1
        
        results['data']['unique'].append({
            'datetime': 'week',
            'count': len(weekCounter)
        })

        return results
        
api.add_resource(Referrers, '/referrers')
api.add_resource(Geo, '/geo')
api.add_resource(Unique, '/unique')
api.add_resource(Bots, '/bots')
api.add_resource(Path, '/path')
api.add_resource(Pages, '/pages')

if __name__ == '__main__':
    app.run(host='0.0.0.0',debug=True)
