import read_db
import time
import pgeocode
import urllib.request
import json
import numpy as np
import pandas as pd
from tqdm import tqdm
import matplotlib.pyplot as plt

class Distance:

    def __init__(self):
        self.extract = read_db.ExtractData()

    def get_distance(self,df):
        dist = pgeocode.GeoDistance('NL')
        distance=dist.query_postal_code(df['ZipCode_x'].apply(lambda x: "".join(x.split(" "))[:4]).to_list(),
                                        df['ZipCode_y'].apply(lambda x: "".join(x.split(" "))[:4]).to_list())
        return pd.DataFrame(distance).set_index(df.index)

    def get_zipcode(self):
        employee_adres=self.extract.join_addresses(target="Employees").dropna()
        relation_adres=self.extract.join_addresses(target="Relations").dropna()
        df=pd.merge(employee_adres, relation_adres, on='Id',how='inner').set_index('Id')
        return self.get_distance(df)

    def get_distance_timeslots(self,):
        return self.get_zipcode()

class TimeSlotDetails:

    def __init__(self):
        pass



if __name__=="__main__":
    dist = pgeocode.GeoDistance('NL')
    dist=Distance()
    # dist_plot=dist.get_distance_timeslots().boxplot()
    # dist_plot.plot()
    # plt.show()


