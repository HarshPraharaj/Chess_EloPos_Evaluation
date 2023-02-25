import pandas as pd
import numpy as np
# import matplotlib.pyplot as plt
import requests
import lichess.api
import lichess.pgn
import lichess.format
import chess
import chess.pgn
import io
import os
import json
from user_definition import *
from google.cloud import storage


def get_user_games(username):
    r = requests.get(f"https://lichess.org/api/games/user/{username}",
                     params={'max': 100, 'analysed': True, 'pgnInJson': True, 'moves': True, 'evals': True,
                             'opening': True},
                     headers={"Accept": "application/x-ndjson"})  # headers = headers, stream = True)
    return r


def user_games_to_pgn(r):
    games_list = r.text.splitlines()
    games_list_dictified = []
    for game in games_list:
        game_dictified = json.loads(game)
        games_list_dictified.append(game_dictified)
    return games_list_dictified


def extract_evaluations(string):
    substrings = []
    start = 0
    for i in range(len(string)):
        if string[i] == "[":
            start = i
        if string[i] == "]":
            substrings.append(string[start + 1:i])
    substrings.append(string[-3:])
    return substrings


def make_dataframe(user_games):
    games_df = pd.DataFrame(columns=['notation', 'eval'])
    for i, user_game in enumerate(user_games):
        pgn_io = io.StringIO(user_game['pgn'])
        game = chess.pgn.read_game(pgn_io)
        evals = extract_evaluations(user_game['pgn'].split('\n')[-4])
        # if not os.path.isdir(f"{data_dir}/{username}"):
        #     os.mkdir(f'{data_dir}/{username}')
        j = 0
        while game:
            board = game.board()
            pos = board.fen(en_passant='fen')  # grab fen string of current position
            #         print(board)
            # print(pos)
            # f = open(f"{data_dir}/{username}/game_{i+1}.txt", "a")
            if j < len(evals) - 1:
                # f.write(f'{pos} | {evals[j].split(" ")[1]} \n')
                games_df = games_df.append(
                    {'notation': pos, 'eval': evals[j].split(" ")[1], 'game_id': str(user_game['id']) + str(j)},
                    ignore_index=True)
                # print(pos)
                # print(evals[j].split(" ")[1])
            else:
                break
            j += 1
            game = game.next()

        # break
    return games_df


def push_df_gcp(games_df, username, bucket_name, service_account_key_file):
    storage_client = storage.Client.from_service_account_json(service_account_key_file)
    bucket = storage_client.bucket(bucket_name)
    blob = bucket.blob(f'data/{username}.csv')

    with blob.open("w") as f:
        games_df.to_csv(f, index=False)


def push_elo_to_gcp(elo_dir, bucket_name, service_account_key_file):
    elo_df = pd.read_csv(elo_dir)
    storage_client = storage.Client.from_service_account_json(service_account_key_file)
    bucket = storage_client.bucket(bucket_name)
    blob = bucket.blob(f'elo_data/elo_data.csv')

    with blob.open("w") as f:
        elo_df.to_csv(f, index=False)
