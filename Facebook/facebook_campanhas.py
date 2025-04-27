import pandas as pd
import requests 
import urllib3
import webbrowser
import facebook as fb
import json
urllib3.disable_warnings()
from google.cloud import storage
from google.cloud import bigquery_datatransfer_v1
from google.protobuf.timestamp_pb2 import Timestamp
import os
import time
import datetime

hoje = datetime.date.today()
ontem = hoje - datetime.timedelta(days=1)
since_date = ontem.strftime("%Y-%m-%d")
until_date = hoje.strftime("%Y-%m-%d")


#Essa token dura pelo menos 60 dias, e deve ser alterada sempre pelo aplicativo api marketing do facebook 

token_api_marketing = "xxx"
class GraphAPI:
    def __init__(self, fb_api):
        self.base_url = "https://graph.facebook.com/v22.0/"
#Campos que estão vindo no dicionário se desejar um novo coloque na ordem que você quer
        self.api_fields = ["account_id","account_name","adset_id","adset_name", "campaign_id", "campaign_name",
                           "spend", "clicks", "impressions"]
        self.token = "&access_token=" + fb_api
#Parâmetros da API se quiser alterar ou consultar novos parâmetros de insights: https://developers.facebook.com/docs/marketing-api/reference/ad-campaign/insights/
    def get_insights(self, ad_acc, level="adset", since=None, until=None):
        url = self.base_url + "act_" + str(ad_acc)
        url += "/insights?breakdowns=hourly_stats_aggregated_by_advertiser_time_zone&level=" + level
        url += "&fields=" + ",".join(self.api_fields)

        if since and until:
            url += "&time_range={'since':'" + since + "','until':'" + until + "'}&time_increment=1"

        all_data = []
#Se ele encontrar "paging" vai verificar se tem o "next" no dicionário e se for true vai fazer um loop e substituir a url da req api para obter o novo dicionário
        loop_count = 1

        while True:
            data = requests.get(url + self.token, verify=False)
            data = json.loads(data.content.decode("utf-8"))
            all_data.extend(data["data"])
            if "paging" in data:
                if "next" in data["paging"]:
#o print url para fazer checkagem na ferramenta do graph api
                    print(url)
                    print(f"Loop = {loop_count} - data:")
                    print(data)
                    loop_count += 1
                    url = data["paging"]["next"]
                else:
                    break
            else:
                break

        return all_data

if __name__ == "__main__":
    fb_api = token_api_marketing
    ad_acc = "inserir o código da conta"
    since_date = since_date
    until_date = until_date

    graph_api = GraphAPI(fb_api)
    insights_data = graph_api.get_insights(ad_acc, since=since_date, until=until_date)
    df_epoca = pd.DataFrame(insights_data)

### Requisição Epoca Fornecedor

import datetime

hoje = datetime.date.today().strftime("%Y-%m-%d")



token_api_marketing = "xxx"

class GraphAPI:
    def __init__(self, fb_api):
        self.base_url = "https://graph.facebook.com/v22.0/"
#Campos que estão vindo no dicionário se desejar um novo coloque na ordem que você quer
        self.api_fields = ["account_id","account_name","adset_id","adset_name", "campaign_id", "campaign_name",
                           "spend", "clicks", "impressions"]
        self.token = "&access_token=" + fb_api
#Parâmetros da API se quiser alterar ou consultar novos parâmetros de insights: https://developers.facebook.com/docs/marketing-api/reference/ad-campaign/insights/
    def get_insights(self, ad_acc, level="adset", since=None, until=None):
        url = self.base_url + "act_" + str(ad_acc)
        url += "/insights?breakdowns=hourly_stats_aggregated_by_advertiser_time_zone&level=" + level
        url += "&fields=" + ",".join(self.api_fields)

        if since and until:
            url += "&time_range={'since':'" + since + "','until':'" + until + "'}&time_increment=1"

        all_data = []
#Se ele encontrar "paging" vai verificar se tem o "next" no dicionário e se for true vai fazer um loop e substituir a url da req api para obter o novo dicionário
        loop_count = 1

        while True:
            data = requests.get(url + self.token, verify=False)
            data = json.loads(data.content.decode("utf-8"))
            all_data.extend(data["data"])
            if "paging" in data:
                if "next" in data["paging"]:
#o print url para fazer checkagem na ferramenta do graph api
                    print(url)
                    print(f"Loop = {loop_count} - data:")
                    print(data)
                    loop_count += 1
                    url = data["paging"]["next"]
                else:
                    break
            else:
                break

        return all_data

if __name__ == "__main__":
    fb_api = token_api_marketing
    ad_acc = "inserir o código da conta"
    since_date = hoje
    until_date = hoje

    graph_api = GraphAPI(fb_api)
    insights_data = graph_api.get_insights(ad_acc, since=since_date, until=until_date)
    df_fornecedor = pd.DataFrame(insights_data)
df_fornecedor.to_excel('ext.xlsx',index=False)

df_epoca['_epoca_synced'] = datetime.date.today()
df_epoca['_epoca_id'] = ''
df_epoca['spend'] = df_epoca['spend'].astype(float)
df_epoca['clicks'] = df_epoca['clicks'].astype(int)
df_epoca['impressions'] = df_epoca['impressions'].astype(int)
df_epoca = df_epoca[['campaign_id','date_start','hourly_stats_aggregated_by_advertiser_time_zone','_epoca_synced','account_id','account_name','campaign_name','clicks','impressions','spend','adset_id','adset_name','_epoca_id']]

df_fornecedor['_epoca_synced'] = datetime.date.today()
df_fornecedor['_epoca_id'] = ''
df_fornecedor['spend'] = df_fornecedor['spend'].astype(float)
df_fornecedor['clicks'] = df_fornecedor['clicks'].astype(int)
df_fornecedor['impressions'] = df_fornecedor['impressions'].astype(int)
df_fornecedor = df_fornecedor[['campaign_id','date_start','hourly_stats_aggregated_by_advertiser_time_zone','_epoca_synced','account_id','account_name','campaign_name','clicks','impressions','spend','adset_id','adset_name','_epoca_id']]

df_consolidado = pd.concat([df_epoca,df_fornecedor],ignore_index=True)

df_consolidado.to_csv('Facebook_Intraday.csv',index=False)

os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = r'chave.json'


bucket_name = 'storage'
destination_blob_name1 = 'Midia_Ads_API/Facebook_Intraday.csv'
storage_client = storage.Client()
bucket = storage_client.bucket(bucket_name)
blob = bucket.blob(destination_blob_name1)
blob.upload_from_filename('Facebook_Intraday.csv')

 # Função para executar o Data Transfer no BigQuery
def start_data_transfer(transfer_config_name):
    client = bigquery_datatransfer_v1.DataTransferServiceClient()

    now = datetime.datetime.utcnow()
    seconds = int(now.timestamp())
    
    # Definindo o período de execução manual
    run_time = Timestamp(seconds=seconds)

    # Disparar o job de transferência
    request = bigquery_datatransfer_v1.StartManualTransferRunsRequest(
        parent=transfer_config_name,
        requested_run_time=run_time,
    )
    
    response = client.start_manual_transfer_runs(request=request)
    print(f"Data Transfer {transfer_config_name} iniciado com sucesso.")


transfer_config_1 = 'projects/epoca-230913/locations/us/transferConfigs/'
transfer_config_2 = 'projects/epoca-230913/locations/us/transferConfigs/'

# Iniciar os Data Transfers 1
start_data_transfer(transfer_config_1)
print("O Data Transfers 1 - Epoca API Facebook no BigQuery foi iniciado com sucesso.")


# Aguarda 4 minutos antes de iniciar o segundo Data Transfer (Faço isso para garantir que o fluxo que alimenta a base completa só rode após o fluxo intraday rode)
time.sleep(240)

# Iniciar o Data Transfer 2
start_data_transfer(transfer_config_2)
print("O Data Transfer 2 - Base Campanhas Facebook no BigQuery foi iniciado com sucesso.")