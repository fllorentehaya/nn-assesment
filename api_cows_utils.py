import json
import pandas as pd
from deltalake import DeltaTable, write_deltalake
from pydantic import BaseModel 
from datetime import datetime
import logging


'''
class to Enforce type hints at runtime and data validation (cow entity)

'''
class Cow(BaseModel):
    id: str
    name: str
    birthday: str
'''
class to Enforce type hints at runtime and data validation (Meaure entity)

'''
class Measure (BaseModel):
   id:str
   cow_id:str
   value:float
   date:datetime
'''
class with configuration elements for API configurarion

'''
class ApiConfiguration():
    def __init__(self):
      self.cows_delta_table_name="db/cows"
      self.measures_milk_delta_table_name="db/measures_milk"
      self.measures_weight_delta_table_name="db/measures_weight"
    def get_cow_json(self,dataframe):
        resul_cow_data=f"'id':{dataframe['id']},'name':{dataframe['name']}"
        return resul_cow_data
      
'''
Class with the persistence implementation based on pandas and delta lake just for this example. No use for production environments :-)
'''
class ApiPersistence():

    
    '''
        Constructor of the class
    '''
    def __init__(self):
        logging.info(self.__class__.__name__)
        self.api_config=ApiConfiguration()
    '''
        Method to create the ingestion of a new milk measure of a cow 
        Param:
            sensor_id:
            cow_id: id of the cow
            value: Litres of milk
            date: the day of the measure
    '''
    def create_measure_milk(self,*,sensor_id,cow_id,value,date):
       try:
         
            invalid_measure = Measure(id=sensor_id, cow_id=cow_id,value=value,date=datetime.strptime(date, '%Y-%m-%d'))

            df = pd.DataFrame({"sensor_id": [sensor_id],"cow_id": [cow_id],"value":[float(value)],"date":[datetime.strptime(date, '%Y-%m-%d')]})
            write_deltalake(self.api_config.measures_milk_delta_table_name, df, mode="append")
            return 1
       except:
        raise Exception("Persistence Error. Maybe some data has not the correct type")
    
    '''
        Method to create the ingestion of a new weight measure of a cow 
        Param:
            sensor_id:
            cow_id: id of the cow
            value: kg of cow
            date: the day of the measure
    '''
    
    def create_measure_weight(self,*,sensor_id,cow_id,value,date):
       try:
         
            invalid_measure = Measure(id=sensor_id, cow_id=cow_id,value=value,date=date)

            df = pd.DataFrame({"sensor_id": [sensor_id],"cow_id": [cow_id],"value":[float(value)],"date":[date]})
            write_deltalake(self.api_config.measures_weight_delta_table_name, df, mode="append")
            return 1
       except:
        raise Exception("Persistence Error. Maybe some data has not the correct type")
    
    '''
        Method to get info of a cow
        Param:
            cow_id: id of the cow
    '''
    def get_cow(self,*,id):
       logging.info(__name__)
       df=DeltaTable(self.api_config.cows_delta_table_name).to_pandas()
       df_filtered=df[df["id"]==id]
       return df_filtered.to_json(orient='records')
    '''
        Method to create a new cow register 
        Param:
            
            cow_id: unique identifier of the cow
            name: name of the cow
            birthday: date of birth
    '''    
    def create_cow(self,*,id,name,birthday):
      try:
         
        invalid_cow = Cow(id=id, name=name,birthday=birthday)

        df = pd.DataFrame({"id": [id],"name": [name],"birthday":[birthday]})
        write_deltalake(self.api_config.cows_delta_table_name, df, mode="append")
        return 1
      except:
        raise Exception("Persistence Error. Maybe some data has not the correct type")
