import pandas as pd
from datetime import datetime
from api_cows_utils import ApiConfiguration
from deltalake import DeltaTable, write_deltalake
from logging_utils import logging_enrich
from datetime import datetime
from reporting_utils import ReportingConfig
from logging_utils import logging_enrich
import logging

logger= logging.getLogger(__name__)
logging.basicConfig(format='%(asctime)s %(message)s',level=logging.DEBUG)
class ReportingWeightCows(ReportingConfig):
    def __init__(self):
        self.api_config=ApiConfiguration()
    
    @logging_enrich
    def generate_weight_aggretaion_report(self):
        
        df_weight=DeltaTable(self.api_config.measures_weight_delta_table_name).to_pandas()
        df_weigth_ordered=df_weight.sort_values(by=['cow_id','date'])
        df_weigth_ordered['date']=pd.to_datetime(df_weight["date"], format="%Y-%m-%d")
        df_weigth_final =  df_weigth_ordered.groupby(['cow_id']).apply(lambda  df_weigth_ordered:  df_weigth_ordered[(df_weigth_ordered['date'] >=(df_weigth_ordered['date'].max() - pd.to_timedelta(30, unit='d')))].agg({'value': 'mean'}))
        html=df_weigth_final.to_html()

        

        return html
    
    @logging_enrich
    def input_date(self):

        print("Introduce the date you what to generate the report. If date is empty all days well be considered in the report")
        
        self.html_agg_weight=self.generate_weight_aggretaion_report()
        #print (self.html_agg_milk)
       
        text_file = open(self.get_date_for_file("weight"), "w")
        text_file.write( self.html_agg_weight)
        text_file.close()



reporting=ReportingWeightCows()
reporting.input_date()
