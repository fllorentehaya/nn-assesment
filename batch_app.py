import requests
import pandas as pd
import json
'''
    Decorator function to implement the requests to API

'''
def requestor(func):
    def wrapper(**kwargs):
        
        url, id,body= func(**kwargs)
        print (f"http://127.0.0.1:5000/{url}/{id}")
        ret=requests.post(f"http://127.0.0.1:5000/{url}/{id}",json=json.loads(body))
        print(ret)
        print(body)
    return wrapper

@requestor
def load(*,uri,**kwargs):
    json_result="{"
    for k, val in kwargs.items():
        if (k=="id"):
            cow_id_result=val
        else:
            json_result+=f"\"{k}\":\"{val}\","
    json_result_enrich=json_result[:-1]+"}"
    print(json_result_enrich)
    return uri , cow_id_result, json_result_enrich
'''


'''
def load_data():
    df_sens=pd.read_parquet('data/cow_data/sensors.parquet', engine='pyarrow')
    df_meas=pd.read_parquet('data/cow_data/measurements.parquet', engine='pyarrow')
    
    # nulls are not considered in the process lifecycle in this approach
    
    df_meas_clean=df_meas.dropna()
    df_sens["sensor_id"]=df_sens["id"]
    df_result = pd.merge(df_meas_clean, df_sens, on=['sensor_id'], how='left')   
    df_result["date"]=pd.to_datetime(df_result['timestamp'], unit='s')
    df_result['date2'] = df_result['date'].dt.strftime('%Y-%m-%d')
    
    return df_result

'''
    Load measures related to weight
'''
def load_measures_weight(data):
    df_result_milk=data[data["unit"]=='kg']
    for index, row in df_result_milk.iterrows():
        load(uri="measure/sensor/weight",id=row["sensor_id"],cow_id=row["cow_id"],value=row["value"],date=row["date2"])
        
    return df_result_milk
'''
    Load measures related to milk
'''
def load_measures_milk(data):
    
    df_result_milk=data[data["unit"]=='L']
    for index, row in df_result_milk.iterrows():
        load(uri="measure/sensor/milk",id=row["sensor_id"],cow_id=row["cow_id"],value=row["value"],date=row["date2"])
        
    return df_result_milk
'''
    Load cows data fron the origin to pass to the decorator in order to make requests
'''
def load_cows():
    
    df_cows=pd.read_parquet('data/cow_data/cows.parquet', engine='pyarrow')
    for index, row in df_cows.iterrows():
        load(uri="cows",id=row["id"],name=row["name"],birthday=row["birthdate"])

'''
    Chain of the complete load.

'''   
def load_process():
    
    load_cows()
    measure_no_filtered=load_data()

    load_measures_milk(measure_no_filtered)
    load_measures_weight(measure_no_filtered)



    
'''
    Start the process of load entities and sensor data

'''    
load_process()