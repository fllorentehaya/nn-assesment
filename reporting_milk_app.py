
import pandas as pd
from reporting_utils import ReportingConfig
from api_cows_utils import ApiConfiguration
from deltalake import DeltaTable, write_deltalake
import logging 
from logging_utils import logging_enrich

logger= logging.getLogger(__name__)
logging.basicConfig(format='%(asctime)s %(message)s',level=logging.DEBUG)



class ReportingMilkCows(ReportingConfig):
    
    def __init__(self):
        #logger = logging.getLogger(__name__)
        #logging.basicConfig(level=logging.DEBUG)
        #logging.basicConfig(format='%(asctime)s %(message)s',level=logging.DEBUG)
        self.api_config=ApiConfiguration()
     
    @logging_enrich
    def generate_milk_aggretaion_report(self,date_filter):
        self.df_milk=DeltaTable(self.api_config.measures_milk_delta_table_name).to_pandas()
        if (len(date_filter)>0):
            self.df_milk=self.df_milk[self.df_milk["date"]==date_filter]

            
       
        
        self.df_agg_milk=self.df_milk.groupby(["cow_id","date"]).agg(total_milk=('value','sum'))
        return self.df_agg_milk.to_html()
    
    @logging_enrich
    def input_date(self):

        print("Introduce the date you what to generate the report. If date is empty all days well be considered in the report")
        self.date_filter=reporting_date=input()
        self.html_agg_milk=self.generate_milk_aggretaion_report(self.date_filter)
        
        text_file = open(self.get_date_for_file("milk"), "w")
        text_file.write( self.html_agg_milk)
        text_file.close()



reporting=ReportingMilkCows()
reporting.input_date()
