from flask import Flask
from flask_restful import Resource, Api
from elasticsearch import Elasticsearch

app = Flask(__name__)
api = Api(app)
es = Elasticsearch(host='search-pagecloud-legacy-decode-2016-oijvlfnyaac4p6h2kimvltek24.us-east-1.es.amazonaws.com',
                   port=443,
                   use_ssl=True,
                   verify_certs=False)

results = es.search(
    index="production-logs-*",
    body={
        "size": 500,
        "sort": {
            "@timestamp": "desc"
        },
        "query": {
            "filtered": {
                "query": {
                    "query_string": {
                        "analyze_wildcard": True,
                        "query": "*"
                    }
                },
                "filter": {
                    "bool": {
                        "must": [{
                            "range": {
                                "@timestamp": {
                                    "gte": 1458835200000,
                                    "lte": 1458878400000
                                }
                            }
                        }],
                        "must_not": []
                    }}}},
        "highlight": {
            "pre_tags": ["@kibana-highlighted-field@"],
            "post_tags": ["@/kibana-highlighted-field@"],
            "fields": {
                "*":{}
            },
            "fragment_size": 2147483647
        },
        "aggs":{
            "2":{
                "date_histogram": {
                    "field": "@timestamp",
                    "interval": "10m",
                    "pre_zone": "-04:00",
                    "pre_zone_adjust_large_interval": True,
                    "min_doc_count": 0,
                    "extended_bounds": {
                        "min": 1458835200000,
                        "max": 1458878400000
                    }}}},
        "fields": ["*", "_source"],
        "script_fields": {},
        "fielddata_fields": ["@timestamp"]
    })

class Users(Resource):
    def get(self):
        return {'hello': 'world'}

class Pages(Resource):
    def get(self):
        return {'hello': 'world'}

api.add_resource(Users, '/users')
api.add_resource(Pages, '/pages')

if __name__ == '__main__':
    app.run(debug=True)
