import json
import requests
from datetime import datetime, timedelta
import os
from google.cloud import storage
from six import string_types
import pandas as pd
from urllib.parse import urlencode, urlunparse


ACCESS_TOKEN = "xxx"
PATH = "/open_api/v1.3/report/integrated/get/"

def build_url(path, query=""):
    """
    Build request URL
    :param path: Request path
    :param query: Querystring
    :return: Request URL
    """
    scheme, netloc = "https", "business-api.tiktok.com"
    return urlunparse((scheme, netloc, path, "", query, ""))

def get(json_str):
    """
    Send GET request
    :param json_str: Args in JSON format
    :return: Response in JSON format
    """
    args = json.loads(json_str)
    query_string = urlencode({k: v if isinstance(v, str) else json.dumps(v) for k, v in args.items()})
    url = build_url(PATH, query_string)
    headers = {
        "Access-Token": ACCESS_TOKEN,
    }
    rsp = requests.get(url, headers=headers)
    return rsp.json()

hoje = datetime.now().date()
intraday = hoje.strftime('%Y-%m-%d')

if __name__ == '__main__':
    metrics_list = ['spend', 'impressions', 'campaign_name']
    metrics = json.dumps(metrics_list)
    data_level = 'AUCTION_CAMPAIGN'
    end_date = intraday
    start_date = intraday
    service_type = 'AUCTION'
    report_type = 'BASIC'
    dimensions_list = ['campaign_id', 'stat_time_hour']
    dimensions = json.dumps(dimensions_list)
    page_size = 1000
    advertiser_ids = ['7361810628245422097', '6986700771614015489']
    dfs = []
    
    for advertiser_id in advertiser_ids:
        my_args = {
            "metrics": metrics,
            "data_level": data_level,
            "end_date": end_date,
            "page_size": page_size,
            "start_date": start_date,
            "advertiser_id": advertiser_id,
            "service_type": service_type,
            "report_type": report_type,
            "dimensions": dimensions
        }
        dados = get(json.dumps(my_args))
        
        
        if 'data' in dados and 'list' in dados['data']:
            data = dados['data']['list']
            stat_time_hours = [item['dimensions']['stat_time_hour'] for item in data]
            impressions = [int(item['metrics']['impressions']) for item in data]
            spend = [float(item['metrics']['spend']) for item in data]
            campaign_names = [item['metrics']['campaign_name'] for item in data]
            df = pd.DataFrame({
                'stat_time_hour': stat_time_hours,
                'impressions': impressions,
                'spend': spend,
                'campaign_name': campaign_names
            })
            df = df[((df['impressions'] != 0) | (df['spend'] != 0))]
            df['stat_time_hour'] = pd.to_datetime(df['stat_time_hour']) 
            df['date'] = df['stat_time_hour'].dt.strftime('%Y-%m-%d')
            df['hour'] = df['stat_time_hour'].dt.strftime('%H')
            dfs.append(df[['date', 'hour', 'impressions', 'spend', 'campaign_name']])
        else:
            print(f"No data returned for advertiser ID: {advertiser_id}")
    
    if dfs:
        
        df_final = pd.concat(dfs, ignore_index=True)
        df_final['hour'] = df_final['hour'].astype(int)
        df_final['date'] = pd.to_datetime(df_final['date'])
        df_final['impressions'] = df_final['impressions'].astype(int)
        df_final['spend'] = df_final['spend'].astype(float)
        df_final.to_csv('custo_tiktok_intraday.csv', index=False, columns=['impressions', 'spend', 'campaign_name', 'date', 'hour'])
        
        
        
        os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = r'.json'
        bucket_name = 'epoca-bi'
        destination_blob_name = 'base-bi/custo_tiktok_intraday.csv'
        storage_client = storage.Client()
        bucket = storage_client.bucket(bucket_name)
        blob = bucket.blob(destination_blob_name)
        blob.upload_from_filename('custo_tiktok_intraday.csv')
        
        

    else:
        print("No data to export.")
