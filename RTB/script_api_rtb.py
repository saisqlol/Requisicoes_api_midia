from datetime import date, timedelta
from operator import attrgetter
import pandas as pd

from rtbhouse_sdk.client import BasicAuth, Client
from rtbhouse_sdk.schema import CountConvention, StatsGroupBy, StatsMetric

if __name__ == "__main__":
    with Client(auth=BasicAuth("Login", "Senha")) as api:
        advertisers = api.get_advertisers()
        day_to = date.today()
        day_from = '2023-07-16'
        group_by = [StatsGroupBy.DAY]
        metrics = [
            StatsMetric.IMPS_COUNT,
            StatsMetric.CLICKS_COUNT,
            StatsMetric.CAMPAIGN_COST,
            StatsMetric.CONVERSIONS_COUNT,
            StatsMetric.CTR
        ]
        data_frames = []
        
        for advertiser in advertisers:
            stats = api.get_rtb_stats(
                advertiser.hash,
                day_from,
                day_to,
                group_by,
                metrics,
                count_convention=CountConvention.ATTRIBUTED_POST_CLICK,
            )
            
            columns = group_by + metrics
            data_frame = [
                [getattr(row, c.name.lower()) for c in columns]
                for row in reversed(sorted(stats, key=attrgetter("day")))
            ]
            columns = ['Data', 'Impressoes', 'Cliques', 'Custo', 'Conversao', 'CTR']
            
            df = pd.DataFrame(data_frame, columns=columns)
            df['Custo'] = df['Custo'].astype(float)
            df['Conta'] = advertiser.name
            data_frames.append(df)
        
        merged_df = pd.concat(data_frames)
        path = r'G:/Shared drives/Bases BI/dados_rtb.csv'
        merged_df.to_csv(path, index=False)
