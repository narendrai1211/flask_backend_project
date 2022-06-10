from elasticsearch import Elasticsearch
import pandas as pd
import os


def pre_process(df):
    df['price'] = df['price'].str.replace('₹', '')
    df['price'] = df['price'].str.split(' ').str[-1]
    df['currency'] = 'INR'
    df['price'] = df['price'].fillna(0.0)
    print('pre-processing done ..')
    return df


def insert_records_to_es(idx_name, df):
    print(f'Inserting records to es index .. {idx_name}')
    dicts = df.to_dict(orient='records')
    id_ = 1
    for i in dicts:
        i['id'] = id_
        try:
            es.index(index=idx_name, id=i['id'], document=i)
        except Exception as e:
            print(e)
            print('Exception in inserting record', i, e)
        id_ += 1
    print('Inserting done .. ')


def make_data_directory():
    try:
        os.mkdir('data')
    except Exception as e:
        print(e)


if __name__ == '__main__':
    make_data_directory()
    df = pd.read_csv('./data/MOCK_DATA_BE.csv')
    es = Elasticsearch('http://localhost:9200/')
    index_name = 'mock_data'
    df_final = pre_process(df)
    insert_records_to_es('mock_data', df_final)
