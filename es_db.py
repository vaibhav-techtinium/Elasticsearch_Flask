from elasticsearch import Elasticsearch
import os
from dotenv import load_dotenv

load_dotenv()

def connect_elasticsearch():
    _es_obj = Elasticsearch(hosts=[os.getenv("ES_HOST")], timeout=10)
    if _es_obj.ping():
        print("ES Contected....")
    else:
        print("Something went wrong :(")
    
    return _es_obj

es = connect_elasticsearch()