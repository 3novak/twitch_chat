# read in the raw text pulled from twitch streams

import pandas as pd
import pickle as p
import os
import re

def make_dfs(streams):
    for stream in streams:
        print('\nworking on ' + stream + '...')
        print('reading file...')

        # specify the appropriate datetime format
        print('changing the time format...')
        df = pd.read_csv(stream + '.txt', names=['date', 'time', 'name', 'msg'], engine='python', delimiter='\`\|\=')
        df['datetime'] = pd.to_datetime(df['date'].astype(str) + ' ' + df['time'], infer_datetime_format=True)
        df.drop(['date', 'time'], axis=1, inplace=True)

        # include the stream name
        print('adding stream name...')
        df['stream'] = stream

        print('writing the pickle...')
        p.dump(df, open('pickled_' + stream + '.p', 'wb'))


def aggregate_dfs(df_names='default', aggregate_dest='test.p'):
    if df_names == 'default':
        files = [f for f in os.listdir() if re.match(r'pickled_', f)]
    else:
        files = df_names
    print(files)
    dfs = [p.load(open(f, 'rb')) for f in files]
    tmp = pd.concat(dfs)
    p.dump(tmp, open(aggregate_dest, 'wb'))


if __name__ == '__main__':
    make_dfs(['cowsep', 'eulcs1', 'imaqtpie', 'lck1', 'loltyler1', 'nalcs1', 'trick2g', 'tsm_doublelift', 'tsm_dyrus', 'voyboy'])
    print('first round of aggregations...')
    aggregate_dfs(df_names=['pickled_cowsep.p', 'pickled_eulcs1.p', 'pickled_imaqtpie.p', 'pickled_lck1.p', 'pickled_loltyler1.p'], aggregate_dest='all_streams1.p')
    print('round 2...')
    aggregate_dfs(df_names=['pickled_nalcs1.p', 'pickled_trick2g.p', 'pickled_tsm_doublelift.p', 'pickled_tsm_dyrus.p', 'pickled_voyboy.p'], aggregate_dest='all_streams2.p')
