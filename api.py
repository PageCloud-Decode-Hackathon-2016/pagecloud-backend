from collections import defaultdict
from collections import Counter
from elasticsearch import Elasticsearch
from elasticsearch_dsl import Search, Q, A
from flask import Flask
from flask_restful import Resource, Api
import user_agents
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
                 .fields(['referrer', 'agent', 'geoip.country_code3', 'clientip'])\
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
        categories = Counter()
        total = 0

        for req in requests:
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

        for agent in agents.keys():
            results.append({
                'name': agent,
                'count': agents[agent]
            })

        return {
            'data': {
                'count': sum(categories.values()),
                'categories': categories,
                'agents': results
            }
        }

class Path(Resource):
    def commonPath(self, trace):
    

        return trace

    def get(self):
        results = []
        clients = Counter()

        for req in requests:
            ip = req.get('clientip', ['-'])[0]
            clients[ip] += 1

        freqPath = Counter()
        for visitor in clients.keys()[:100]:
            pages =[""]
            s = Search(using=client, index='production-logs-*')\
                 .fields(['clientip', 'request'])\
                 .query('match', clientip=visitor)

            for page in s.scan():
                page = page.to_dict().get('request', [''])[0]
                print page
                if not ((page.find('.') > -1) or (page == pages[len(pages) - 1])):
                    print "true"
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
            
        day_aggregation = A('date_histogram', field='@timestamp', interval='day', format='yyyy-MM-dd')
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
        # UNIQUE
        for k, v in data.iteritems():
            if v['per_day']['key'] in unique:
                unique[v['per_day']['key']] += 1
            else:
                unique[v['per_day']['key']] = 1

        for k,v in unique.iteritems():       
            more_data['data']['unique'].append({
                'datetime' : k,
                'count' : v
            })
        
        nonunique = {}    
        # NONUNIQUE   
        for k,v in data.iteritems():
            if v['per_day']['key'] in nonunique:
                nonunique[v['per_day']['key']] += v['count']
            else:
                nonunique[v['per_day']['key']] = v['count']
        
        for k,v in unique.iteritems():       
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
