import torch
import torch.nn as nn
import torch.optim as optim
from tqdm import tqdm
from matchclassifier.modelutils import binary_acc, confusion
import pandas as pd
import numpy as np
from torch.utils.data import Dataset, DataLoader

class trainer:

    def __init__(self,model,train_data,test_data,epochs=50,batch_size=256,learning_rate=0.005,weight_decay=1e-4):
        self.model = model
        self.epochs=epochs
        self.train_loader=DataLoader(dataset=train_data, batch_size=batch_size, shuffle=True)
        self.test_loader=DataLoader(dataset=test_data, batch_size=batch_size, shuffle=True)
        self.device = torch.device("cuda:0" if torch.cuda.is_available() else "cpu")
        self.model.to(self.device)
        self.criterion = nn.BCEWithLogitsLoss()
        self.optimizer = optim.Adam(model.parameters(), lr=learning_rate,weight_decay=weight_decay)

    def train(self,save=None):
        self.model.train()
        pbar = tqdm(range(1,  self.epochs+1))
        for e in pbar:
            epoch_loss = 0
            epoch_acc = 0
            for X_batch, y_batch in self.train_loader:
                X_batch, y_batch = X_batch.to(self.device), y_batch.to(self.device)
                
                y_pred = self.model(X_batch)
                loss = self.criterion(y_pred, y_batch.unsqueeze(1))
                # +l1_regularizer(model, lambda_l1=lambda_l1)\
                # +orth_regularizer(model, lambda_orth=lambda_orth)   
                acc = binary_acc(y_pred, y_batch.unsqueeze(1))
                
                self.optimizer.zero_grad()
                loss.backward()
                self.optimizer.step()
                
                epoch_loss += loss.item()
                epoch_acc += acc.item()
            pbar.set_description(f'Epoch {e+0:03}: | Loss: {epoch_loss/len(self.train_loader):.5f} | Acc: {epoch_acc/len(self.train_loader):.3f}')
        if save is not None:
            torch.save(self.model.state_dict(), save)
    
    def eval(self):
        self.model.eval()
        test_loss=0
        test_acc=0
        true_positives, false_positives, true_negatives, false_negatives=0,0,0,0
        with torch.no_grad():
            for X_batch,y_batch in self.test_loader:
                X_batch, y_batch = X_batch.to(self.device), y_batch.to(self.device)
                y_pred = self.model(X_batch)
                loss = self.criterion(y_pred, y_batch.unsqueeze(1))
                acc = binary_acc(y_pred, y_batch.unsqueeze(1))
                tp,fp,tn,fn,index_false,probs=confusion(y_pred,y_batch.unsqueeze(1))
                true_positives, false_positives, true_negatives, false_negatives=true_positives+tp, false_positives+fp, true_negatives+tn, false_negatives+fn
                test_loss += loss.item()
                test_acc += acc.item()
            print(f'Loss: {test_loss/len(self.test_loader):.5f} | Acc: {test_acc/len(self.test_loader):.3f}')
            print(f'True Positives: {true_positives}')
            print(f'False Positives: {false_positives}')
            print(f'True Negatives: {true_negatives}')
            print(f'False Negatives: {false_negatives}')
