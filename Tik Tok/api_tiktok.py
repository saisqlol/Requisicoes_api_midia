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
    # type: (str, str) -> str
    """
    Build request URL
    :param path: Request path
    :param query: Querystring
    :return: Request URL
    """
    scheme, netloc = "https", "business-api.tiktok.com"
    return urlunparse((scheme, netloc, path, "", query, ""))

def get(json_str):
    # type: (str) -> dict
    """
    Send GET request
    :param json_str: Args in JSON format
    :return: Response in JSON format
    """
    args = json.loads(json_str)
    query_string = urlencode({k: v if isinstance(v, string_types) else json.dumps(v) for k, v in args.items()})
    url = build_url(PATH, query_string)
    headers = {
        "Access-Token": ACCESS_TOKEN,
    }
    rsp = requests.get(url, headers=headers)
    return rsp.json()

hoje = datetime.now().date()
ontem = hoje - timedelta(days=1)
dia_custo = ontem.strftime('%Y-%m-%d')

def fetch_data_for_advertiser(advertiser_id):
    metrics_list = ['spend', 'impressions', 'campaign_name']
    metrics = json.dumps(metrics_list)
    data_level = 'AUCTION_CAMPAIGN'
    end_date = dia_custo
    start_date = dia_custo
    service_type = 'AUCTION'
    report_type = 'BASIC'
    dimensions_list = ['campaign_id', 'stat_time_hour']
    dimensions = json.dumps(dimensions_list)
    page_size = 1000

    # Args in JSON format
    my_args = json.dumps({
        "metrics": metrics_list,
        "data_level": data_level,
        "end_date": end_date,
        "page_size": page_size,
        "start_date": start_date,
        "advertiser_id": advertiser_id,
        "service_type": service_type,
        "report_type": report_type,
        "dimensions": dimensions_list
    })
    
    dados = get(my_args)
    return dados['data']['list']

if __name__ == '__main__':
    advertiser_ids = ['7361810628245422097', '6986700771614015489']
    all_data = []

    for advertiser_id in advertiser_ids:
        data = fetch_data_for_advertiser(advertiser_id)
        all_data.extend(data)

    stat_time_hours = [item['dimensions']['stat_time_hour'] for item in all_data]
    impressions = [int(item['metrics']['impressions']) for item in all_data]
    spend = [float(item['metrics']['spend']) for item in all_data]
    campaign_names = [item['metrics']['campaign_name'] for item in all_data]

    df = pd.DataFrame({
        'stat_time_hour': stat_time_hours,
        'impressions': impressions,
        'spend': spend,
        'campaign_name': campaign_names
    })

    df = df[((df['impressions'] != 0) | (df['spend'] != 0))]
    df[['date', 'hour']] = df['stat_time_hour'].str.split(' ', expand=True)

    # Convert 'date' and 'hour' columns to datetime objects
    df['date'] = pd.to_datetime(df['date'])
    df['hour'] = df['hour'].str[:2]
    df.drop(columns=['stat_time_hour'], inplace=True)

    os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = r'.json'

    # Set your Google Cloud Storage bucket and object details
    bucket_name = 'epoca-bi'
    destination_blob_name = 'base-bi/custo_tiktok.csv'

    # Authenticate to Google Cloud Storage
    storage_client = storage.Client()

    # Assuming you have a DataFrame called 'df'
    # Save the DataFrame to a CSV file
    df.to_csv('temp_dataframe.csv', index=False)

    # Upload the saved file to Google Cloud Storage
    bucket = storage_client.bucket(bucket_name)
    blob = bucket.blob(destination_blob_name)
    blob.upload_from_filename('temp_dataframe.csv')

    # Delete the temporary local file
    os.remove('temp_dataframe.csv')
