import os
from flask import Flask
from elasticsearch import Elasticsearch
import json
from sentence_transformers import SentenceTransformer


app = Flask(__name__)

# read settings from json
CONFIG = {}
with open("config.json") as f:
    CONFIG = json.load(f)
with open("../scripts/config.json") as f2:
    CONFIG2 = json.load(f2)    
if CONFIG['semantic_search'].get('ollama_embedding_model')!=CONFIG2['semantic_model']['ollama_embedding_model']:
    exit("Error: Mismatched ollama_embedding_model settings between website/config.json and scripts/config.json")
curdir = os.getcwd()
drive, path = os.path.splitdrive(curdir)
#"exe": "\\..\\fscrawler\\bin\\fscrawler.bat",
#"config_dir": "\\..\\fsjobs",
#"defaults": "\\..\\fsjobs\\_defaults.yaml"

# Connect to your Elasticsearch cluster
EsClient = Elasticsearch(CONFIG["elasticsearch_url"])
# load some dynamic defaults on the CONFIG
PROJECT_PARENT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
print(PROJECT_PARENT)
if CONFIG["fscrawler"]["exe"] == "None":
    CONFIG["fscrawler"]["exe"] = os.path.join(PROJECT_PARENT, "fscrawler", "bin", "fscrawler.bat")
if CONFIG["fscrawler"]["config_dir"] == "None":
    CONFIG["fscrawler"]["config_dir"] = os.path.join(PROJECT_PARENT, "fsjobs")
if CONFIG["fscrawler"]["defaults"] == "None":
    CONFIG["fscrawler"]["defaults"] = os.path.join(CONFIG["fscrawler"]["config_dir"], "_defaults.yaml")

# load model 
EmbeddingModel = SentenceTransformer(CONFIG["semantic_search"]["model_path"])




