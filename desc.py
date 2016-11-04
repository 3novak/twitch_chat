# volume of messages over time by streamer

import pandas as pd
import numpy as np
import pickle as p
import seaborn
import matplotlib.pyplot as plt

def time_inquiry(df=None, bystream=False, datetime='datetime', num_vals=5):
    if bystream == False:
        tyme_diff = df[datetime].max() - df[datetime].min()
        print('time difference:', tyme_diff)
        return tyme_diff
    else:
        grouped = df.groupby(['stream'])
        print('max:', grouped[datetime].max(), '\n')
        print('min:', grouped[datetime].min(), '\n')
        print('duration:', grouped[datetime].max() - grouped[datetime].min(), '\n')
        print('value_counts:', grouped[datetime].value_counts()[:num_vals])


def users_by_stream(df, users=None, streams=None, nrows=None, sort_on=None):
    # the users and streams selections default to all users and all streams
    # a list of users or streams can be passed in to filter the rows of the
    # data frame
    # nrows defines the size of the returned table. the default is to include
    # all users, but the first ten rows may be the most relevant, for instance.
    # the default is to sort on the first stream in the data.
    if users != None:
        df = df.loc[df['name'].isin(users)]
    if streams != None:
        df = df.loc[df['stream'].isin(streams)]
    df['ones'] = 1
    if nrows == None:
        nrows = df['name'].unique().shape[0]
    if sort_on == None:
        sort_on = df['stream'].unique()[0]

    name_freq = pd.pivot_table(df[['name', 'stream', 'ones']],
                               index=['name'], columns=['stream'],
                               aggfunc='count', fill_value=0)

    name_freq.columns = name_freq.columns.get_level_values(1)
    name_freq.sort_values(by=sort_on, ascending=False, inplace=True)

    return name_freq[:nrows]


def tyme_series(data, interval):
    # interval governs the size of each interval
    # the interval for each row is computed as its distance from the first
    # timestamp that appears in the data
    data['diff'] = data['datetime'] - data['datetime'].min()
    data['diff'] = data['diff'].dt.seconds
    data['interval'] = (data['diff']/interval).astype(int)
    grp_intervals = data.groupby(['interval', 'stream'])['ones'].sum()

    return grp_intervals


if __name__ == '__main__':
    df = p.load(open('twitch_sample.p', 'rb'))

    time_inquiry(df=df, bystream=True, num_vals=3, datetime='datetime')
    print('\n\n\n')

    print(users_by_stream(df=df, nrows=3, sort_on=['trick2g', 'loltyler1', 'cowsep']))
    print('\n\n\n')

    ts = tyme_series(data=df, interval=3600)
    ts = pd.DataFrame(ts)
    ts.reset_index(inplace=True)
    seaborn.pointplot(x='interval', y='ones', data=ts, hue='stream')
    plt.show()
