from Game import Game
import pandas as pd
import os

train = pd.read_csv('train.csv')

data_path = "C:\\Users\\u172951\\Documents\\data"



train['Game_ID'] = train['Game_ID'].apply(lambda x: os.path.join(data_path, x))

files = [os.path.join(data_path, file) for file in os.listdir(data_path)]

print(files)

train = train[train['Game_ID'].isin(files)]

train = train.reset_index(drop=True)


for index,row in train.iterrows():
    print("Game ID: {}, Event Index: {}, Frame Number: {} {}/{}".format(row.Game_ID,row.Event_Number,row.Frame_Number,index,len(train)))
    game = Game(path_to_json=row.Game_ID, event_index=row.Event_Number, frame=row.Frame_Number)
    game.read_json()
    features = game.start()
    if features is None:
        continue
    for i in range(len(features)):
        str_col = 'f' + str(i)
        train.at[index,str_col] = features[i]
    
train = train.dropna(axis=0)
train.to_csv('train_fin.csv',index=False)

print('finished')
