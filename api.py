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
                'count': valagent
            })

        return {
            'data': {
                'count': sum(categories.values()),
                'categories': categories,
                'agents': results
            }
        }


class Path(Resource):
    def commonPath(self, list2d):
        result = [[],[],[],[],[]]
        for depth in range(5):
            firstList = list2d[depth]
            result[depth].append(firstList[0])
            find = firstList[1]
            for i in range(0, len(list2d)):
                if find == list2d[i][0]:
                    result[depth].append(list2d[i][0])
                    find = list2d[i][1]
            for i in list2d:
                if find == i[0]:
                    result[depth].append(i[0])

        return result

    def get(self):
        results = []
        clients = Counter()

        for req in _requests:
            ip = req.get('clientip', ['-'])[0]
            clients[ip] += 1

        freqPath = Counter()
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
                    if page == "":
                        page = "/"
                    pages.append(page)

            if len(pages) > 2:
                for x in range(0, len(pages)-1):
                    freqPath[str(pages[x]+ " "+ pages[x+1])]+=1

        sorted = freqPath.most_common()
        commonTrace =[]
        for elem in sorted:
            x,y = elem
            commonTrace.append(x.split(" "))

        soln = self.commonPath(commonTrace)
        rank = 1
        data = []
        for elem in soln:
            data.append({
                'nodes': elem,
                'count': rank
            })
            rank+=1

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
        url = "http://decode-2016.pagecloud.io/manifest.json"
        manifest = requests.get(url)
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

            if page[1:] in all_pages.keys():
                lm = all_pages[page[1:]]
            elif page == '':
                lm = all_pages['home']
            else:
                continue
           
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
