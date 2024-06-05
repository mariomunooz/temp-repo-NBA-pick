from Game import Game
import pandas as pd
import multiprocessing


def get_game_id(file_path):
    file_name = file_path.split('\\')[-1]
    game_id = file_name.split('.')[0]
    return game_id

train = pd.read_csv('train.csv')

game_ids = train['Game_ID'].unique()

train_dfs = {}

iterations_done = 0

total_number_of_iterations = len(train)

for game_id in game_ids:
    game_train = train[train['Game_ID'] == game_id]
    train_dfs[get_game_id(game_id)] = game_train
    

def process_game(arguments):
    game_id, iterations_done, total_number_of_iterations = arguments
    game_train = train[train['Game_ID'] == game_id]
    for index, row in game_train.iterrows():
        iterations_done += 1
        print("Game ID: {}, Event Index: {}, Frame Number: {} {}/{} ({}%)".format(row.Game_ID, row.Event_Number, row.Frame_Number, iterations_done, total_number_of_iterations, round((iterations_done/total_number_of_iterations)*100, 2)))
        game = Game(path_to_json=row.Game_ID, event_index=row.Event_Number, frame=row.Frame_Number)
        game.read_json()
        features = game.start()
        if features is None:
            continue
        for i in range(len(features)):
            str_col = 'f' + str(i)
            game_train.at[index, str_col] = features[i]
    game_train = game_train.dropna(axis=0)
    game_train.to_csv('train_fin_{}.csv'.format(game_id), index=False)

if __name__ == '__main__':
    pool = multiprocessing.Pool()
    args = [(game_id, iterations_done, total_number_of_iterations) for game_id in game_ids]
    pool.map(process_game, args)
    pool.close()
    pool.join()