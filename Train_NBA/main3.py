from Game import Game
import pandas as pd
import os
import multiprocessing

train = pd.read_csv('train.csv')

data_path = "C:\\Users\\u172951\\Documents\\data"



train['Game_ID'] = train['Game_ID'].apply(lambda x: os.path.join(data_path, x))

files = [os.path.join(data_path, file) for file in os.listdir(data_path)]

print(files)

train = train[train['Game_ID'].isin(files)]

train = train.reset_index(drop=True)


def process_row(index, row, train):
    print("Game ID: {}, Event Index: {}, Frame Number: {} {}/{}".format(row.Game_ID,row.Event_Number,row.Frame_Number,index,len(train)))
    game = Game(path_to_json=row.Game_ID, event_index=row.Event_Number, frame=row.Frame_Number)
    game.read_json()
    features = game.start()
    if features is None:
        return None
    for i in range(len(features)):
        str_col = 'f' + str(i)
        train.at[index,str_col] = features[i]

if __name__ == '__main__':
    pool = multiprocessing.Pool()
    results = []
    for index, row in train.iterrows():
        result = pool.apply_async(process_row, (index, row, train))
        results.append(result)
    pool.close()
    pool.join()

    for result in results:
        result.get()
    
train = train.dropna(axis=0)
train.to_csv('train_fin.csv',index=False)

print('finished')