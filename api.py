from collections import Counter
from flask import Flask
from flask_restful import Resource, Api
from elasticsearch import Elasticsearch
from elasticsearch_dsl import Search, Q, A
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

        #raw_buckets = search.execute().aggregations['per_day']['buckets']
        raw_buckets2 = search.execute().aggregations['group_by_geoip']['buckets']

        data = {}
        #for bucket in raw_buckets:
            #data[bucket['key']] = bucket['doc_count']

        data2 = {}
        for bucket in raw_buckets2:
            per_day = bucket['per_day']['buckets']
            per_day_data = {}
            for val in per_day:
                per_day_data['key'] = val['key_as_string']
                per_day_data['count'] = val['doc_count']
            data2[bucket['key']] = {
                 'count': bucket['doc_count'],
                 'per_day': per_day_data
             }
        #return data2
        
        unique = {}
        # UNIQUE
        for k, v in data2.iteritems():
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
        for k,v in data2.iteritems():
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
        
        """
        # HOUR
        hourSearch = Search(using=client, index='production-logs-*')\
        .fields(['clientip','timestamp'])\
        .query('match_all')\
        .filter("range", **{'@timestamp': {'gte': 'now-1h'}})
        
        index = 0
        hourList = []
        for i in hourSearch.scan():
            if i.to_dict().get('clientip'):
                hourList.append(i.to_dict().get('clientip')[0])
                index += 1

        results['data']['nonunique'].append({
            'datetime': 'hour',
            'count': index
        })
        
        hourCounter = Counter()
        for i in hourList:
            hourCounter[i]+=1
        
        results['data']['unique'].append({
            'datetime': 'hour',
            'count': len(hourCounter)
        })
        
        # DAY
        daySearch = Search(using=client, index='production-logs-*')\
        .fields(['clientip','timestamp'])\
        .query('match_all')\
        .filter("range", **{'@timestamp': {'gte': 'now-1d/d'}})
        
        index = 0
        dayList = []
        for i in daySearch.scan():    
            if i.to_dict().get('clientip'):        
                dayList.append(i.to_dict().get('clientip')[0])
                index += 1
            
        results['data']['nonunique'].append({
            'datetime': 'day',
            'count': index
        })
        
        dayCounter = Counter()
        for i in dayList:
            dayCounter[i]+=1
        
        
        results['data']['unique'].append({
            'datetime': 'days',
            'count': len(dayCounter)
        })
        
        # WEEK 
        weekSearch = Search(using=client, index='production-logs-*')\
        .fields(['clientip','timestamp'])\
        .query('match_all')\
        .filter("range", **{'@timestamp': {'gte': 'now-7d/d'}})
        
        index = 0
        weekList = []
        for i in weekSearch.scan():
            if i.to_dict().get('clientip'):
                weekList.append(i.to_dict().get('clientip')[0])
                index += 1

        results['data']['nonunique'].append({
            'datetime': 'week',
            'count': index
        })
               
        weekCounter = Counter()
        for i in weekList:
            weekCounter[i]+=1
        
        results['data']['unique'].append({
            'datetime': 'week',
            'count': len(weekCounter)
        })
        
        return results
        """
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
    app.run(host='0.0.0.0',debug=True)
