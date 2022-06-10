from flask import Flask, render_template, request, flash, jsonify, Response
from elasticsearch import Elasticsearch

app = Flask(__name__)
# api = Api(app)
es = Elasticsearch('http://localhost:9200/')

query_for_like = {"size": 3000,
                  "query": {
                      "wildcard": {"product.keyword": {"value": "Su*"}
                                   }
                  }, "_source": ["product", "price"]
                  }


# Task 1
@app.route('/', methods=['GET'])
def home():
    return jsonify({'message': 'RESTful APIs for assignment',
                    'GET': [{
                        "Task1": ['/products/<product_id>',
                                  '/products/get_all',
                                  'products/delete_all'],
                        "Task2": ['/products/groupby_business_category',
                                  '/products/unique_brands_count',
                                  'products/product_subset']}],
                    'POST': {
                        "Task1": ['/products/insert_many',
                                  '/products/update/<product_id>']
                    }}
                   )


@app.route('/products/<product_id>', methods=['GET'])
def get_products(product_id):
    try:
        return jsonify(es.get(index='mock_data', id=product_id)['_source'])
    except Exception as e:
        print(e)
        return jsonify({'error': f'Exception occurred in the API response {e}',
                        'possible_cause': f' maybe the requested ID was not found ..'})


@app.route('/products/update/<product_id>', methods=['POST'])
def update_product(product_id):
    existing_doc = es.get(index='mock_data', id=product_id)['_source']
    new_document = request.get_json()
    data_keys = list(new_document.keys())
    if all(x in data_keys for x in ['id', 'product', 'price']):
        es.index(index='mock_data', id=existing_doc['id'], document=new_document)
        return jsonify({'message': f'Updated the record of ID {product_id} in the database'})
    else:
        return jsonify({'message': "All the elements should be present in the data ['id', 'product', 'price'] "})


@app.route('/products/get_all', methods=['GET'])
def get_all():
    # it will return 10000 records at once and give response in JSON
    result = es.search(index="mock_data", body={
        'size': 10000,
        "query": {"match_all": {}}})['hits']['hits']
    return jsonify(result)


@app.route('/products/insert_many/', methods=['POST'])
def insert_many():
    failure_list = []
    json_data_from_request = request.get_json()
    if not isinstance(json_data_from_request, list):
        return jsonify({
            'message': 'Please give correct input for insert_many sample input [{..}, {..}, {..}]'}
        )
    for i in json_data_from_request:
        data_keys = list(i.keys())
        if all(x in data_keys for x in ['id', 'product', 'price']):
            es.index(index='mock_data', document=i, id=i['id'])
        else:
            failure_list.append({'message': f"One or more keys are missing in the data ['id', 'price', 'product']"})

    if failure_list:
        return Response(f'Inserting unsuccessful for {len(failure_list)} <br> {failure_list}')
    else:
        return Response('Inserting success for the given data .. ')


@app.route('/products/delete_all', methods=['GET'])
def delete_all():
    es.delete_by_query(index='mock_data', body={"query": {"match_all": {}}})
    return jsonify({'message': 'Deleted all the records from index'})


#  Task 2
@app.route('/products/groupby_business_category', methods=['GET'])
def groupby_categories():
    data = es.search(index='mock_data', body={
        "size": 0,
        "aggs": {
            "unique_categories": {
                "terms": {"field": "business_category.keyword", "size": 3000},
            }
        }
    })
    return jsonify({'business_categories_with_count': data['aggregations']['unique_categories']['buckets']})


@app.route('/products/unique_brands_count', methods=['GET'])
def unique_brands_count():
    data = es.search(index='mock_data', body={
        "size": 0,
        "aggs": {
            "unique_brands": {
                "terms": {"field": "brand.keyword", "size": 3000}
            }
        }
    }
                     )
    return jsonify({'brands_with_count': data['aggregations']['unique_brands']['buckets']})


@app.route('/products/product_subset', methods=['GET'])
def product_subset():
    data = es.search(index='mock_data', body=query_for_like)
    print(data['hits'])
    return jsonify({'brands_with_count': data['hits']})


if __name__ == '__main__':
    app.config['JSONIFY_PRETTYPRINT_REGULAR'] = True
    app.run()
