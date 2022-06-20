import os
import sys
import argparse
from matchclassifier.train import trainer
from matchclassifier.model import BinaryClassification
from matchclassifier.modelutils import get_model_variables,get_train_test_loader
import torch
from dataloading.computeDataframe import ComputeDataframe
# from matchclassifier import train

CURRENT_DIR=os.path.dirname(os.path.abspath(__file__))

'''
def trainer(args):
    print(args.timeframe,args.initialize,args.epochs,args.learn_rate)

function_redirect={'trainer':trainer}

parser = argparse.ArgumentParser(description='A automated tool for planning in the home-health care')
parser.add_argument('-s','--source', help='The source of the database"',metavar='',default="mssql://SA:Assist2022@localhost:1401/qpz-florein-prod_bu_20220414-ANONYMOUS")
subparser = parser.add_subparsers(dest='cmd')
trainer = subparser.add_parser('trainer')
trainer.set_defaults(func=trainer)
trainer.add_argument('-t','--timeframe', help='The prediction timeframe, date in format Y-M-D H:M:S',metavar='',default="all")
trainer.add_argument('-i','--initialize', help='either "start-over" or "pre-initialized"',metavar='', default='pre-initialized')
trainer.add_argument('-e','--epochs', help='The amount of epochs to train',type=int,metavar='', default=50)
trainer.add_argument('-lr','--learn_rate', help='The learning rate of the model',metavar='',type=float, default=0.005)
predicter = subparser.add_parser('predict')
predicter.add_argument('-t','--timeframe', help='The prediction timeframe',metavar='',default="all")
args = parser.parse_args()
function_redirect[args.cmd](args)



if trainer.purpose
    if os.path.exists(CURRENT_DIR+'/Data Loading/train_df.csv'):
        if os.path.exists(CURRENT_DIR+'/Data Loading/checkpoint.pth'):
            new_train=train(epochs=)
            print('Loading Dataframe and Model')

else:
    print('does not exist')
'''
def main(purpose='train',from_file='/dataloading/checkpoint.pth'):
    if os.path.exists(CURRENT_DIR+'/dataloading/train_df.csv'):
            X_train, X_test, y_train, y_test=get_model_variables(CURRENT_DIR+'/dataloading/train_df.csv')
            train_data,test_data=get_train_test_loader(X_train, X_test, y_train, y_test)
            model = BinaryClassification(X_train.shape[1],1)
            if os.path.exists(CURRENT_DIR+from_file):
                model.load_state_dict(torch.load(CURRENT_DIR+'/dataloading/checkpoint.pth',map_location=torch.device('cpu')))
                _train=trainer(model,train_data,test_data)
                if purpose=='train':
                    _train.train()
                if purpose=='test':
                    _train.eval()
            else:
                if purpose=='train':
                    _train.train(save=CURRENT_DIR+'/dataloading/checkpoint.pth')
                if purpose=='test':
                    print("No pre-trained model found, we need to train first!")
                    _train.train(save=CURRENT_DIR+'/dataloading/checkpoint.pth')
                    _train.eval()
    else:
        print("No dataframe found, we need to load first!")
        cdf=ComputeDataframe(source="mssql://SA:Assist2022@localhost:1401/qpz-florein-prod_bu_20220414-ANONYMOUS")
        cdf.main(CURRENT_DIR+'/dataloading/train_df.csv')
        main(purpose,from_file)

if __name__=="__main__":
    main('test')