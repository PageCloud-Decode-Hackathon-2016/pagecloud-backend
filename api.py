from collections import Counter
from flask import Flask
from flask_restful import Resource, Api
from elasticsearch import Elasticsearch
from elasticsearch_dsl import Search, Q
import json



app = Flask(__name__)
api = Api(app)

client = Elasticsearch(host='search-pagecloud-legacy-decode-2016-oijvlfnyaac4p6h2kimvltek24.us-east-1.es.amazonaws.com',
                       port=443,
                       use_ssl=True,
                       verify_certs=False)

class Referrers(Resource):
    def get(self):
        results = {
        	'data': {
        		'referrers': []
        	}
        }

        # s = Search(using=client, index='production-logs-*')\
        #     .fields(['agent', 'clientip', 'referrer', 'timestamp'])\
        #     .query('match_all')

        s = Search(using=client, index='production-logs-*')\
            .fields(['referrer'])\
            .query('match_all')

        response = s.execute().to_dict()

        for hit in response['hits']['hits']:
        	results['data']['referrers'].append(
        		{
        			'name': hit['fields']['referrer'][0].replace('"', ''),
        			'count': 21
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

        cntry = uniqueCounter()

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
        .filter("range", **{'@timestamp': {'gte': 'now-1h'}})
        
        hourDict = hourSearch.scan().to_dict()['hits']['hits']
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
        .filter("range", **{'@timestamp': {'gte': 'now-1d/d'}})
        
        dayDict = daySearch.scan().to_dict()['hits']['hits']
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
        .filter("range", **{'@timestamp': {'gte': 'now-7d/d'}})
        
        weekDict = weekSearch.scan().to_dict()['hits']['hits']
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

if __name__ == '__main__':
    app.run(host='0.0.0.0',debug=True)
