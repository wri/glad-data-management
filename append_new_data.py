import sqlite3
import os
import json
import argparse

import pandas as pd


parser = argparse.ArgumentParser(description='Update pre-calculated tile stats with new data')
parser.add_argument('--current-db', '-c', help='the current DB', required=True)
parser.add_argument('--new-db', '-n', help='the DB with updated data', required=True)
parser.add_argument('--years', '-y', nargs='+', help='the years to replace in the current DB', required=True)

args = parser.parse_args()


def db_to_df(db):

    if not os.path.exists(db):
        raise ValueError('{} not found'.format(db))

    conn = sqlite3.connect(db)
    df = pd.read_sql('SELECT * FROM tile_summary_stats_z12', conn)

    # load JSON into dict
    df['alert_dict'] = df.apply(lambda row: json.loads(row['alert_dict']), axis=1)

    unpacked_df = pd.DataFrame([item for sublist in 
                 [[[row.x, row.y, row.z, k, v] for 
                 (k, v) in row.alert_dict.items()] for 
                 _, row in df.iterrows()] 
                 for item in sublist])

    # name rows and grab year from date
    unpacked_df.columns = ['x', 'y', 'z', 'alert_key', 'alert_count']
    unpacked_df['year'] = unpacked_df.apply(lambda row: row['alert_key'].split('-')[0], axis=1)

    return conn, unpacked_df


if __name__ == '__main__':

    current_conn, current_df = db_to_df(args.current_db)
    _, new_df = db_to_df(args.new_db)

    current_df.drop(current_df[current_df.year.isin(args.years)].index, inplace=True)

    df = pd.concat([current_df, new_df])

    # now get back to our serialized format
    # build alert_dict column
    df['alert_dict'] = [{k: v} for k, v in zip(df.alert_key, df.alert_count)]

    # group by tile
    grouped = df.groupby(['x', 'y', 'z'])['alert_dict'].apply(lambda x: x.tolist()).reset_index()

    # combine list of dicts per tile to one dict as JSON
    grouped['alert_dict'] = grouped.apply(lambda row: json.dumps({ k: v for d in row['alert_dict'] for k, v in d.items() }), axis=1)

    # write back to "current" database
    grouped.set_index(['x', 'y', 'z'], inplace=True)
    grouped.to_sql('tile_summary_stats_z12', current_conn, if_exists='replace')

