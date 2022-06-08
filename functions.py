import read_db
import time
import pgeocode
import urllib.request
import json
import numpy as np
import pandas as pd
from tqdm import tqdm

class Functions:

    def __init__(self):
        self.extract = read_db.ExtractData()

    def get_distance(self,df):
        dist = pgeocode.GeoDistance('NL')
        distance=dist.query_postal_code(df['ZipCode_x'].apply(lambda x: x.split(" ")[0]).to_list(), df['ZipCode_y'].apply(lambda x: x.split(" ")[0]).to_list())
        return pd.DataFrame(distance).set_index(df.index)

    def get_zipcode(self):
        # adres_employee=self.extract.get_adres('Employees')
        employee_adres=self.extract.join_addresses(target="Employees")
        print(employee_adres[(employee_adres['Id']=='125497')])
        relation_adres=self.extract.join_addresses(target="Relations")
        print(relation_adres[relation_adres['ZipCode']=='2265 BL'])
        employee_adres.dropna(inplace=True),relation_adres.dropna(inplace=True)
        df=pd.merge(employee_adres, relation_adres, on='Id',how='inner').set_index('Id')
        return self.get_distance(df)

    def distance_costumer(self,):
        distances=[]   
        a=self.get_zipcode()
        print(a)

        

if __name__=="__main__":
    dist = pgeocode.GeoDistance('NL')
    print(dist.query_postal_code('2595','2265'))
    functions=Functions()
    functions.distance_costumer()


