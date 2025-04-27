import pandas as pd
from datetime import datetime, timedelta
from google.cloud import storage
from google.cloud import bigquery_datatransfer_v1
from google.protobuf.timestamp_pb2 import Timestamp
import time
import os


def process_bing_ads_reports(today_file, yesterday_file, output_file, required_columns):
    # Lê os arquivos de hoje e ontem, ignorando cabeçalhos e rodapés
    df_today = pd.read_csv(today_file, header=9, skipfooter=2, engine='python')
    df_yesterday = pd.read_csv(yesterday_file, header=9, skipfooter=2, engine='python')
    
    # Filtra as colunas especificadas
    df_today = df_today[required_columns]
    df_yesterday = df_yesterday[required_columns]

    # Combina os DataFrames
    combined_df = pd.concat([df_today, df_yesterday], ignore_index=True)

    # Salva o arquivo consolidado
    combined_df.to_csv(output_file, index=False)
    print(f"Arquivo consolidado salvo em: {output_file}")

# Especifica as colunas desejadas
required_columns = [
    "TimePeriod", "AccountId", "AccountName", "AccountStatus", "CampaignId",
    "CampaignName", "CampaignStatus", "CampaignType", "DeliveredMatchType",
    "AdDistribution", "QualityScore", "Impressions", "Clicks", "Ctr",
    "AverageCpc", "Spend", "ImpressionSharePercent", "TopImpressionRatePercent",
    "AbsoluteTopImpressionRatePercent", "Conversions", "ConversionRate",
    "CostPerConversion", "TrackingTemplate", "CustomParameters", "DeviceType",
    "Network", "Revenue", "ViewThroughConversions", "PhoneImpressions",
    "PhoneCalls", "ImpressionLostToRankAggPercent", "ImpressionLostToBudgetPercent",
    "AveragePosition"
]

# Exemplo de uso
if __name__ == "__main__":
    # Caminhos dos arquivos gerados pelo Bing Ads
    today_file = "C:/Users/bi_epoca/Documents/Bing Ads API/venv/Bing Ads Request/Dados de campanha/hoje.csv"  # Caminho do arquivo de hoje
    yesterday_file = "C:/Users/bi_epoca/Documents/Bing Ads API/venv/Bing Ads Request/Dados de campanha/ontem.csv"  # Substitua pelo caminho correto do arquivo de ontem
    output_file = "C:/Users/bi_epoca/Documents/Bing Ads API/venv/Bing Ads Request/Dados de campanha/bing_ads_intraday.csv"  # Nome do arquivo consolidado
    
    # Processa e combina os relatórios
    process_bing_ads_reports(today_file, yesterday_file, output_file, required_columns)
    
    
# Enviando dados para o GCP e rodando os data transfers

os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = r'G:/Shared drives/Bases BI/epoca-230913-b478a9a0dd4c.json'

bucket_name = 'epoca-storage'
destination_blob_name1 = 'Midia_Ads_API/bing_ads_intraday.csv'
storage_client = storage.Client()
bucket = storage_client.bucket(bucket_name)
blob = bucket.blob(destination_blob_name1)
blob.upload_from_filename('C:/Users/bi_epoca/Documents/Bing Ads API/venv/Bing Ads Request/Dados de campanha/bing_ads_intraday.csv')

time.sleep(50)

# Função para executar o Data Transfer no BigQuery
def start_data_transfer(transfer_config_name):
    client = bigquery_datatransfer_v1.DataTransferServiceClient()
    now = datetime.utcnow() - timedelta(hours=3)
    seconds = int(now.timestamp())
    # Definindo o período de execução manual
    run_time = Timestamp(seconds=seconds)
    # Disparar o job de transferência
    request = bigquery_datatransfer_v1.StartManualTransferRunsRequest(
        parent=transfer_config_name,
        requested_run_time=run_time,
    )
    response = client.start_manual_transfer_runs(request=request)


transfer_config_1 = 'projects/epoca-230913/locations/us/transferConfigs/675531c4-0000-2212-be26-30fd38151e78'

# Iniciar os Data Transfers 1
start_data_transfer(transfer_config_1)
print("O Data Transfers 1 - Bing Ads Api  no BigQuery foi iniciado com sucesso.")
