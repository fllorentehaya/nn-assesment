import unittest
from deltalake import DeltaTable, write_deltalake

import requests
import json
from api_cows_utils import ApiPersistence
from datetime import  datetime

'''
def create_mock_cow(*,id,name,birthday):
    body="{" + f"\"name\":\"{name}\",\"birthday\":\"{birthday}\"" + "}"
    print(body)
    ret=requests.post(f"http://127.0.0.1:5000/cows/{id}",json=json.loads(body))
def get_mock_cow(*,id):
    api_persistence =ApiPersistence()
    return api_persistence.get_cow(id=id)
'''    

class Api_Test(unittest.TestCase):
    
    measures_milk_delta_table_name="db/measures_milk"
    measures_weight_delta_table_name="db/measures_weight"
    '''
        delete the synthentic data
    '''
    def delete_synthetic_weight(self,id):
       self.cows_delta_table_name="db/measures_weight"
       df_cow_to_delete=DeltaTable(self.cows_delta_table_name)
       df_cow_to_delete.delete(f"sensor_id =='{id}'")
    def delete_systhentic_cow(self,*,id):
       self.cows_delta_table_name="db/cows"
       df_cow_to_delete=DeltaTable(self.cows_delta_table_name)
       df_cow_to_delete.delete(f"id =='{id}'")
        
    def create_synthetic_measure_weight(self):
        date_now=datetime.now()
        date_now_formatted=date_now.strftime("%m%d%Y%H%M%S")
        sensor_id=f"sensor_id_{date_now_formatted}"
        cow_id=f"cow_id_{date_now_formatted}"
        value=0
        date=f"{date_now_formatted}"
        return sensor_id,cow_id,value,date
    def create_synthetic_cow(self):
        
        date_now=datetime.now()
        date_now_formatted=date_now.strftime("%m%d%Y%H%M%S")
        cow_id=f"cow_id_{date_now_formatted}"
        cow_name=f"cow_name_{date_now_formatted}"
        cow_birthday=f"birth_{date_now_formatted}"
        return cow_id,cow_name,cow_birthday
    
    def create_mock_measure_weight(self,*,sensor_id,cow_id,value,date):
        body="{" + f"\"cow_id\":\"{cow_id}\",\"value\":{value},\"date\":\"{date}\""  + "}"
        print(sensor_id + " " + body)
        ret=requests.post(f"http://127.0.0.1:5000//measure/sensor/weight/{sensor_id}",json=json.loads(body))
    def create_mock_cow(self,*,id,name,birthday):
        body="{" + f"\"name\":\"{name}\",\"birthday\":\"{birthday}\"" + "}"
        #print(body)
        ret=requests.post(f"http://127.0.0.1:5000/cows/{id}",json=json.loads(body))
    
    def test_create_measure_weigh(self):
        try:
            api_persistence =ApiPersistence()
            #create a mock cow with sysnthetic data
            sensor_id,cow_id,value,date=self.create_synthetic_measure_weight()
           
            #create the cow in the database
            self.create_mock_measure_weight(sensor_id=sensor_id,cow_id=cow_id,value=value,date=date)
            #request real data generated in the test (mock data)
            #transfor to json
            #create list of data generated to te text and the real data obtained 
            self.delete_systhentic_cow(id=sensor_id)
            self.assertTrue(True,"Everything works well to create and delete mock data")
        except:
            self.assertTrue(False, "Something was wrong")
       
   
    '''
        function test for getcow
    '''
    def test_getcow(self):
        try:
            api_persistence =ApiPersistence()
            #create a mock cow with sysnthetic data
            cow_id,cow_name,cow_birth=self.create_synthetic_cow()
            #create the cow in the database
            self.create_mock_cow(id=cow_id,name=cow_name,birthday=cow_birth)
            #request real data generated in the test (mock data)
            cow_mock_data=api_persistence.get_cow(id=cow_id)
            #transfor to json
            cow_mock_data_json=json.loads (cow_mock_data)
            #create list of data generated to te text and the real data obtained 
            list_cow_data_test=[cow_id,cow_name,cow_birth]
            list_cow_data_real=[cow_mock_data_json[0]['id'],cow_mock_data_json[0]['name'],cow_mock_data_json[0]['birthday']]
            
            self.assertListEqual(list_cow_data_real,list_cow_data_test,"All data is correct")
            
            # delete synthentic data
            self.delete_systhentic_cow(id=cow_id)
        except:
            self.assertFalse(True,"Some error has happended")
    '''
        function test for getcow
    '''
    def test_createcow(self):
        try:
                
            self.api_persistence =ApiPersistence()
            #create a mock cow with sysnthetic data
            cow_id,cow_name,cow_birth=self.create_synthetic_cow()
            #create the cow in the database
            self.create_mock_cow(id=cow_id,name=cow_name,birthday=cow_birth)
            #request real data generated in the test (mock data)
            cow_mock_data=self.api_persistence.get_cow(id=cow_id)
            #transfor to json
            cow_mock_data_json=json.loads (cow_mock_data)
            #create list of data generated to te text and the real data obtained 
            list_cow_data_test=[cow_id,cow_name,cow_birth]
            list_cow_data_real=[cow_mock_data_json[0]['id'],cow_mock_data_json[0]['name'],cow_mock_data_json[0]['birthday']]
            
            self.assertListEqual(list_cow_data_real,list_cow_data_test,"All data is correct")
            
            # delete synthentic data
            self.delete_systhentic_cow(id=cow_id)
        except:
            self.assertFalse(True,"Some error has happended")
            
           
    
if __name__ == '__main__':
    unittest.main()
