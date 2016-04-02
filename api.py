from collections import defaultdict
from collections import Counter
from elasticsearch import Elasticsearch
from elasticsearch_dsl import Search, Q, A
from flask import Flask
from flask_restful import Resource, Api
import user_agents
import time
from urlparse import urlparse
from robot_detection import is_robot
import requests
import datetime
import re

app = Flask(__name__)
api = Api(app)

client = Elasticsearch(host='search-pagecloud-legacy-decode-2016-oijvlfnyaac4p6h2kimvltek24.us-east-1.es.amazonaws.com',
                       port=443,
                       use_ssl=True,
                       verify_certs=False)

_requests = []
for hit in Search(using=client, index='production-logs-*')\
                 .fields(['referrer', 'agent', 'geoip.country_code3', 'clientip'])\
                 .query('match_all')\
                 .scan():
    _requests.append(hit.to_dict())


class Referrers(Resource):
    def get(self):
        counts = Counter()
        results = []

        for hit in _requests:
            url = urlparse(hit.get('referrer', [''])[0].replace('"', '')).netloc

            if url[:4] == 'www.':
                url =  url[4:]

            counts[url.lower()] += 1

        for key, val in counts.iteritems():
            results.append({
                'name': key,
                'count': val
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

        for hit in _requests:
            c = hit.get('geoip.country_code3', [''])[0]
            countries[c.upper()] += 1

        for key, val in countries.iteritems():
            results.append({
                'country': key,
                'count': val
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
        categories = Counter()
        total = 0

        for req in _requests:
            total += 1
            agent = user_agents.parse(req.get('agent', ['-'])[0].replace('"', ''))
            agents[agent.browser.family] += 1

            if agent.is_mobile:
                categories['mobile'] += 1
            elif agent.is_tablet:
                categories['tablet'] += 1
            elif agent.is_pc:
                categories['pc'] += 1
            elif agent.is_bot:
                categories['bot'] += 1

        for key, val in agents.iteritems():
            results.append({
                'name': key,
                'count': val
            })

        return {
            'data': {
                'count': sum(categories.values()),
                'categories': categories,
                'agents': results
            }
        }


class Path(Resource):

    def commonPath(list2d):
        result = []
        
        firstList = list2d[0]
        result.append(firstList)        
        find = firstList[1]
        
        for i in range(1, len(list2d):
            if find == list2d[i][0]:
                result.append(list2d[i][0])
                find = list2d[i][0]
        
        for i in list2d:
            if find == i[0]:
                result.append(i)
                
        return results
                
    def get(self):
        results = []
        clients = Counter()

        for req in _requests:
            ip = req.get('clientip', ['-'])[0]
            clients[ip] += 1
        path =[]

        for visitor in clients.keys()[:100]:
            pages =[""]
            s = Search(using=client, index='production-logs-*') \
                 .fields(['clientip', 'request']) \
                 .query('match', clientip=visitor)

            for page in s.scan():
                page = page.to_dict().get('request', [''])[0]
                print page
                if not ((page.find('.') > -1) or (page == pages[len(pages) - 1])):
                    print "true"
                    pages.append(page)

            if len(pages) > 2:
                path.append(pages)

        freqPath = Counter()

        for elem in path:
            string = ' '.join(elem)
            freqPath[string] += 1

        data = []
        for elem in freqPath.keys():
            data.append({
                'nodes': elem.split(" ")[1:-1],
                'count': freqPath[elem]
            })

        return {
            'data': {
                'path': data
            }
        }

# The most popular/visited pages on the website
class Pages(Resource):
# TODO: exclude bots, only check for page visits on UNIQUE visitors
    def get(self):
        pages = Counter()
        results = []

        # GET A LIST OF ALL THE WEBSITE'S PAGES AND THEIR LAST MODIFIED DATE
        all_pages = {}
        url = "http://pagecloud.com/"
        manifest = requests.get(url + 'manifest.json')
        manifest = manifest.json()

        for i in range(len(manifest['pages'])):
            all_pages[manifest['pages'][i]['name']] = manifest['pages'][i]['lastModified']
        
        # e.g. www.domain.com/page/ <-- 'request' provides you with '/page'
        s = Search(using=client, index='production-logs-*') \
            .fields(['request']) \
            .query('match_all')

        for hit in s.scan():
            response = hit.to_dict()
            p = response.get('request', [''])[0]

            # Sanitize page name format
            if re.search('\?', p) != None:
            	match = re.search('(.*)\?', p)
            	p = match.group(1)

            pages[p] += 1

        for page in pages.keys():
            
            # Sanitize page name format (remove all parameters after '?') to find modifiedDate
            cleanPage = page
            if re.search('\?', page) != None:
                match = re.search('(.*)\?', page)
                cleanPage = match.group(1)

            if cleanPage[1:] in all_pages.keys():
                lm = all_pages[cleanPage[1:]]
            elif cleanPage == '':
                lm = all_pages['home']
            else:
                lm = 0 # page could not be found in manifest list (might be referrer link!)
           
            if lm > 0:
                lm = datetime.datetime.fromtimestamp(lm / 1000).strftime("%Y-%m-%d")#T%H:%M:%S")

            results.append({
                'name': page,
                'hits': pages[page],
                'lastModified': lm
            })

        return {
            'data': {
                'pages': results
            }
        }


class Unique(Resource):
    def get(self):
        more_data = {
        'data': {
            'nonunique': [],
             'unique': []
            }
        }

        index = 'production-logs-*'

        search = Search(using=client, index=index) \
            .fields(['referrer', 'geoip.ip', 'http_host' ]) \
            .query("match", http_host='decode-2016.pagecloud.io') \
            .filter("range", **{'@timestamp': {'gte': 'now-10d'}}) \
            .params(search_type="count")

        day_aggregation = A('date_histogram',
                            field='@timestamp',
                            interval='day',
                            format='yyyy-MM-dd')

        search.aggs.bucket('group_by_geoip', 'terms', field='geoip.ip', size=0)
        search.aggs['group_by_geoip'].bucket('per_day', day_aggregation)

        raw_buckets = search.execute().aggregations['group_by_geoip']['buckets']

        data = {}
        for bucket in raw_buckets:
            per_day = bucket['per_day']['buckets']
            per_day_data = {}
            for val in per_day:
                per_day_data['key'] = val['key_as_string']
                per_day_data['count'] = val['doc_count']

            data[bucket['key']] = {
                 'count': bucket['doc_count'],
                 'per_day': per_day_data
             }

        unique = {}
        for k, v in data.iteritems():
            if v['per_day']['key'] in unique:
                unique[v['per_day']['key']] += 1
            else:
                unique[v['per_day']['key']] = 1

        for k, v in unique.iteritems():
            more_data['data']['unique'].append({
                'datetime' : k,
                'count' : v
            })

        nonunique = {}
        for k, v in data.iteritems():
            if v['per_day']['key'] in nonunique:
                nonunique[v['per_day']['key']] += v['count']
            else:
                nonunique[v['per_day']['key']] = v['count']

        for k, v in nonunique.iteritems():
            more_data['data']['nonunique'].append({
                'datetime' : k,
                'count' : v
            })

        return more_data

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
api.add_resource(Unique, '/unique')
api.add_resource(Bots, '/bots')
api.add_resource(Path, '/path')
api.add_resource(Pages, '/pages')
api.add_resource(AggregationTestResource, '/aggtest')

if __name__ == '__main__':
    app.run(host='0.0.0.0', debug=True)
