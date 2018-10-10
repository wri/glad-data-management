import os
import errno
import sys

import pandas as pd

input_csv = sys.argv[1]
adm_level = sys.argv[2]
csv_name = os.path.splitext(os.path.basename(input_csv))[0]


def main():

    if adm_level not in ['iso', 'adm1', 'adm2']:
        raise ValueError('second arg must be one of iso, adm1 or adm2')

    # initial output from hadoop doesn't have headers
    cols = None

    if adm_level == 'iso': 
        cols = ['long', 'lat', 'confidence', 'year', 'julian_day', 'country_iso', 'state_id', 'dist_id', 'confidence_text']
        out_dir = os.path.join('iso')
        group_col = 'country_iso'

    elif adm_level == 'adm1':
        iso = csv_name
        out_dir = os.path.join('adm1', iso)
        group_col = 'state_id'
    
    else:
        iso, adm1 = csv_name.split('_')
        out_dir = os.path.join('adm2', iso, adm1)
        group_col = 'dist_id'
    
    if not os.path.exists(out_dir):
        mkdir_p(out_dir)
    
    if cols:
        df = pd.read_csv(sys.argv[1], header=None, names=cols)
    else:
        df = pd.read_csv(sys.argv[1])
    
    for i, g in df.groupby(group_col):
        print i
    
        if adm_level == 'iso':
            out_csv = '{}.csv'.format(i)
        elif adm_level == 'adm1':
            out_csv = '{}_{}.csv'.format(iso, i)
        else:
            out_csv = '{}_{}_{}.csv'.format(iso, adm1, i)
       
        g.to_csv(os.path.join(out_dir, out_csv), index=False)
    

def mkdir_p(dirname):
    # https://stackoverflow.com/a/21349806/
    try:
        os.makedirs(dirname)
    except OSError, e:
        if e.errno != errno.EEXIST:
            raise


if __name__ == '__main__':
    main()

