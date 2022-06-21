import numpy as np
import pandas as pd
import seaborn as sns
import matplotlib.pyplot as plt
import torch
import torch.nn as nn
import torch.optim as optim
from torch.utils.data import Dataset, DataLoader
from sklearn.preprocessing import StandardScaler,RobustScaler,MinMaxScaler  
from sklearn.model_selection import train_test_split
from sklearn.metrics import confusion_matrix, classification_report
import time
import seaborn as sns
import math
import os
import sys

CURRENT_PATH=os.path.dirname(os.path.abspath(__file__))

def get_dataframe(PATH):
    df=pd.read_csv(PATH,index_col=0)
    df = df.replace([np.inf, -np.inf], np.nan)
    with pd.option_context('mode.use_inf_as_null', True):
            df = df.dropna()
    return df

def get_model_variables(PATH):
    df=get_dataframe(PATH)
    scale = StandardScaler()
    X=scale.fit_transform(df.loc[:, df.columns != 'Label'])
    y=df['Label']
    return train_test_split(X, y, test_size=0.2)

def get_train_test_loader(X_train, X_test, y_train, y_test):
    train_loader=Data(torch.FloatTensor(X_train),torch.FloatTensor(y_train.values))
    test_loader=test_data = Data(torch.FloatTensor(X_test),torch.FloatTensor(y_test.values))
    return train_loader,test_loader

def binary_acc(y_pred, y_test):
    y_pred_tag = torch.round(torch.sigmoid(y_pred))
    correct_results_sum = (y_pred_tag == y_test).sum().float()
    acc = correct_results_sum/y_test.shape[0]
    acc = torch.round(acc * 100)
    return acc

def confusion(prediction, truth):
    probabilities=torch.sigmoid(prediction)
    prediction=torch.round(probabilities)
    index_false=(torch.eq(prediction,truth)==False).nonzero(as_tuple=True)[0]
    confusion_vector = prediction / truth
    # Element-wise division of the 2 tensors returns a new tensor which holds a
    # unique value for each case:
    #   1     where prediction and truth are 1 (True Positive)
    #   inf   where prediction is 1 and truth is 0 (False Positive)
    #   nan   where prediction and truth are 0 (True Negative)
    #   0     where prediction is 0 and truth is 1 (False Negative)
    """ Returns the confusion matrix for the values in the `prediction` and `truth`
        tensors, i.e. the amount of positions where the values of `prediction`
        and `truth` are
        - 1 and 1 (True Positive)
        - 1 and 0 (False Positive)
        - 0 and 0 (True Negative)
        - 0 and 1 (False Negative)
        """
    true_positives = torch.sum(confusion_vector == 1).item()
    false_positives = torch.sum(confusion_vector == float('inf')).item()
    true_negatives = torch.sum(torch.isnan(confusion_vector)).item()
    false_negatives = torch.sum(confusion_vector == 0).item()

    return true_positives, false_positives, true_negatives, false_negatives,index_false,probabilities


class Data(Dataset):
    
    def __init__(self, X_data,y_data):
        self.X_data = X_data
        self.y_data = y_data
        
    def __getitem__(self, index):
        return self.X_data[index], self.y_data[index]
        
    def __len__ (self):
        return len(self.X_data)