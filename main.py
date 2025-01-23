from flask import Flask, request, jsonify
from elasticsearch import ApiError
from es_db import es
import os
from dotenv import load_dotenv

load_dotenv()

app = Flask(__name__)

@app.route("/")
def index():
    return "server is running"

@app.route("/create-index", methods=['POST'])
def create_index():
    try:
        index_name = request.json['index_name']
        es.indices.create(index=index_name)
        return jsonify({'message': 'index created'}), 200
    except ApiError as e:
        return jsonify(e.info), e.status_code
    except Exception as e:
        return jsonify({"error": str(e)}), 500

@app.route("/list-indexes", methods=['GET'])
def list_indexes():
    try:
        indexes = list(es.indices.get_alias().keys())
        return jsonify({'indexes': indexes}), 200
    except ApiError as e:
        return jsonify(e.info), e.status_code
    except Exception as e:
        return jsonify({"error": str(e)}), 500


@app.route("/delete-index/<index>", methods=['DELETE'])
def delete_indexes(index):
    try:
        es.indices.delete(index=index)
        return jsonify({'message': 'index deleted'}), 200
    except ApiError as e:
        return jsonify(e.info), e.status_code
    except Exception as e:
        return jsonify({"error": str(e)}), 500

@app.route("/add-single-doc/<index>", methods=['POST'])
def add_single_doc(index):
    try:
        doc = request.json
        es.index(index=index, body=doc)
        return jsonify({'message': 'doc added'}), 200
    except ApiError as e:
        return jsonify(e.info), e.status_code
    except Exception as e:
        return jsonify({"error": str(e)}), 500


@app.route("/add-multiple-docs/<index>", methods=['POST'])
def add_multiple_docs(index):
    try:
        docs = request.json
        for doc in docs:
            es.index(index=index, body=doc)
        return jsonify({'message': 'docs added'}), 200
    except ApiError as e:
        return jsonify(e.info), e.status_code
    except Exception as e:
        return jsonify({"error": str(e)}), 500

@app.route("/update-doc", methods=['PUT'])
def update_doc():
    try:
        index = request.json['_index']
        id = request.json['_id']
        doc = request.json['_doc']
        es.update(index=index, id=id, body={"doc":doc})
        return jsonify({'message': 'doc updated'}), 200
    except ApiError as e:
        return jsonify(e.info), e.status_code
    except Exception as e:
        return jsonify({"error": str(e)}), 500


@app.route("/get-docs/<index>", methods=["GET"])
def get_docs(index):
    try:
        res = es.search(index=index)
        return jsonify(res['hits']['hits']), 200
    except ApiError as e:
        return jsonify(e.info), e.status_code
    except Exception as e:
        return jsonify({"error": str(e)}), 500

@app.route("/delete-doc/<index>/<id>", methods=["DELETE"])
def delete_docs(index, id):
    try:
        es.delete(index=index, id=id)
        return jsonify({'message': 'doc deleted'}), 200
    except ApiError as e:
        return jsonify(e.info), e.status_code
    except Exception as e:
        return jsonify({"error": str(e)}), 500

@app.route("/search", methods=['POST'])
def search():
    try:
        query = request.json['query']
        index = request.json['index']
        res = es.search(index=index, body={"query": query})
        return jsonify(res['hits']['hits']), 200
    except ApiError as e:
        return jsonify(e.info), e.status_code
    except Exception as e:
        return jsonify({"error": str(e)}), 500

if __name__ == '__main__':
    app.run(debug=os.getenv("DEBUG_MODE")=="True", host=os.getenv("HOST"), port=os.getenv("PORT"))  