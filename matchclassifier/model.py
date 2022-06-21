import torch.nn as nn
import seaborn as sns
import os

CURRENT_PATH=os.path.dirname(os.path.abspath(__file__))

class BinaryClassification(nn.Module):
    def __init__(self,input_size,output_size):
        super(BinaryClassification, self).__init__()
        self.layer_1 = nn.Linear(input_size, 128) 
        self.layer_2 = nn.Linear(128, 128)
        self.layer_3 = nn.Linear(128, 64)
        self.layer_out = nn.Linear(64, output_size) 
        
        self.relu = nn.ReLU()
        self.dropout1 = nn.Dropout(p=0.2)
        self.dropout2 = nn.Dropout(p=0.2)
        self.dropout3 = nn.Dropout(p=0.2)
        self.batchnorm1 = nn.BatchNorm1d(128)
        self.batchnorm2 = nn.BatchNorm1d(128)
        self.batchnorm3 = nn.BatchNorm1d(64)
        
    def forward(self, inputs):
        x = self.relu(self.layer_1(inputs))
        x = self.batchnorm1(x)
        x = self.dropout1(x)
        x = self.relu(self.layer_2(x))
        x = self.batchnorm2(x)
        x = self.dropout2(x)
        x = self.relu(self.layer_3(x))
        x = self.batchnorm3(x)
        x = self.dropout3(x)
        x = self.layer_out(x)
        
        return x

