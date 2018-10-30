import os
import subprocess
import argparse
import time
import sqlite3
import datetime
import shutil

import boto3


# Parse commandline arguments
parser = argparse.ArgumentParser(description='Prep input raster data GLAD or Terra I to point workflows.')
parser.add_argument('--region', '-r', nargs='+', help='list of regions to process', required=True)
parser.add_argument('--years', '-y', nargs='+', help='list of years to process', required=True)
parser.add_argument('--staging', dest='staging', action='store_true')
args = parser.parse_args()

glad_update_dir = os.path.dirname(os.path.abspath(__file__))
root_dir = os.path.dirname(glad_update_dir)


def main():

    # first generate tiles
    cmd = ['python', 'generate-tiles.py', '-l', 'glad', '-y'] + args.years + ['--threads', '20', '--world', '-r'] + args.region

    if args.staging:
        cmd += ['--staging']

    cwd = os.path.join(root_dir, 'mapnik-forest-change-tiles')
    subprocess.check_call(cmd, cwd=cwd)
    
    # clear cloudfront cache so that our new tiles are visible
    client = boto3.client('cloudfront')
    cloudfront_config = {'DistributionId': 'E3363DM0PQ56GN', 
                         'InvalidationBatch': {'CallerReference': str(time.time()),
                                               'Paths': {'Items': ['/glad_prod/tiles/*'], 'Quantity': 1}}}
   
    client.create_invalidation(**cloudfront_config)
    
    # then write GLAD data to point and upload to s3
    cmd = ['python', 'processing/utilities/weekly_updates.py', '-l', 'glad', '-r'] + args.region + ['-y'] + args.years

    if args.staging:
        cmd += ['--staging']

    cwd = os.path.join(root_dir, 'raster-vector-to-tsv')
    subprocess.check_call(cmd, cwd=cwd)
           
    # kick off hadoop process to attach iso/adm1/adm2 to each GLAD point
    cmd = ['python', 'update_country_stats.py', '-d', 'umd_landsat_alerts', '-e', 'prod']
    cwd = '/home/ubuntu/gfw-country-pages-analysis-2'
    hadoop_background_process = subprocess.Popen(cmd, cwd=cwd)

    # while we're waiting for that to finish
    # we can update our z/x/y tile stats
    if args.years != ['2018']:
        raise ValueError('Need to build out multiple year handling for this process- not done yet')

    # clean up temp files before we start downloading and writing locally
    clean_up_temp_files(glad_update_dir)
    
    # download our stats.db to update the z / x / y / date / count database table
    cmd = ['aws', 's3', 'cp', 's3://palm-risk-poc/data/mvt/stats.db', '.']
    subprocess.check_call(cmd, cwd=glad_update_dir)
    
    # delete old data
    stats_db = os.path.join(glad_update_dir, 'stats.db')
    conn = sqlite3.connect(stats_db)
    cursor = conn.cursor()
    
    # NB: need to update this when we get 2019 data
    cursor.execute("DELETE FROM tile_alert_stats WHERE alert_date >= '2018-01-01';")
    conn.commit()
    
    glad_folder = 'glad-staging' if args.staging else 'glad'
    
    # process updates by region
    for region in args.region:
    
        csv_name = '{}_{}.csv'.format(region, args.years[0])
    
        # download the source CSV locally
        # this was just created from the raster-vector-to-tsv process
        src_csv = 's3://gfw2-data/alerts-tsv/{}/{}'.format(glad_folder, csv_name)
        cmd = ['aws', 's3', 'cp', src_csv, '.']
        subprocess.check_call(cmd, cwd=glad_update_dir)
    
        # call tippecanoe to convert to mbtiles
        mbtile_db = os.path.join(glad_update_dir, '{}.mbtiles'.format(region))
        cmd = ['tippecanoe', '-o', mbtile_db, '-z12', '-Z12', '-b', '0', csv_name]
        subprocess.check_call(cmd, cwd=glad_update_dir)
        
        # unpack those vector tiles to our z / x / y / date / alert_count format
        cmd = ['python', 'aggregate_tiles_up.py', '-m', mbtile_db, '-s', stats_db]
        subprocess.check_call(cmd, cwd=glad_update_dir)
        
    
    # now that we've added in our new tile data, rebuild the index
    cursor.execute('REINDEX tile_alert_stats;')
    
    # and update our latest alert table
    cursor.execute('UPDATE latest SET alert_date = (SELECT max(alert_date) FROM tile_alert_stats);')
    
    # clean up a little
    cursor.execute('VACUUM;')
    
    # commit and close
    conn.commit()
    conn.close()
    
    # copy back up to S3
    cmd = ['aws', 's3', 'cp', 'stats.db', 's3://palm-risk-poc/data/mvt/stats.db']
    subprocess.check_call(cmd, cwd=glad_update_dir)
    
    # now we need to wait until the hadoop / country pages process has finished
    # this process writes a giant CSV of all GLAD alerts by iso/adm1/adm2 to S3
    wait_for_hadoop(hadoop_background_process)

    hadoop_s3_path = get_current_hadoop_output()
    hadoop_output_csv = 'hadoop_output.csv'
    cmd = ['aws', 's3', 'cp', hadoop_s3_path, hadoop_output_csv]
    subprocess.check_call(cmd, cwd=glad_update_dir)

    # now that we have that data locally, create our iso/adm1/adm2 CSVs
    cmd = ['python', 'split_glad.py', hadoop_output_csv, 'iso']
    subprocess.check_call(cmd, cwd=glad_update_dir)

    # now call the iso --> adm1 and adm1 --> adm2 using parallel processing
    cmd = 'for i in iso/*; do echo python split_glad.py $i adm1; done | parallel --jobs 35'
    subprocess.check_call(cmd, shell=True, cwd=glad_update_dir)

    cmd = 'for i in adm1/*/*; do echo python split_glad.py $i adm2; done | parallel --jobs 35'
    subprocess.check_call(cmd, shell=True, cwd=glad_update_dir)
    
    # copy this data up to S3
    base_cmd = ['aws', 's3', 'cp', '--recursive']
    base_dir = 's3://gfw2-data/alerts-tsv/glad-download/'

    adm_list = ['iso', 'adm1', 'adm2']
    for adm_level in adm_list:
        cmd = base_cmd + ['{}/'.format(adm_level), '{}{}/'.format(base_dir, adm_level)]
        subprocess.check_call(cmd, cwd=glad_update_dir)

    clean_up_temp_files(glad_update_dir)

    # future work:
    # redeploy with jenkins
    # check /latest to make sure data has updated, then hit webhook!


def clean_up_temp_files(glad_update_dir):

    # clean up
    file_list = [x for x in os.listdir('.') if os.path.splitext(x)[1] in ['.csv', '.mbtiles']]
    for f in file_list:
        os.remove(os.path.join(glad_update_dir, f))

    for d in ['iso', 'adm1', 'adm2']:
        try:
            shutil.rmtree(os.path.join(glad_update_dir, d))
        except OSError:
            pass


def get_current_hadoop_output():

    today = datetime.datetime.today()
    yesterday = (today + datetime.timedelta(days=1))

    # Given that this often runs overnight, datestamp may be today or "tomorrow"
    # compared to when the script started
    if check_s3(today):
        date_str = today.strftime('%Y%m%d')

    elif check_s3(yesterday):
        date_str = yesterday.strftime('%Y%m%d')

    else:
        raise ValueError('Hadoop output not found on S3')

    return r's3://gfw2-data/alerts-tsv/temp/output-glad-summary-{}/part-'.format(date_str)


def wait_for_hadoop(process_handle):
    # first we'll reconnect to the process
    hadoop_process_done = False
    
    for i in range(0, 120):
        poll = process_handle.poll()
    
        # still running
        if poll is None:
            pass
    
        elif poll == 0:
            hadoop_process_done = True
            break
    
        else:
            raise ValueError('Hadoop process errored with code {}'.format(poll))

        time.sleep(60)

    
    # if it hasn't finished, kill it and then raise an error
    if not hadoop_process_done:
        process_handle.kill()
        raise ValueError('Hadoop process took longer than 2 hours to finish')


def check_s3(date_val):

    date_str = date_val.strftime('%Y%m%d')

    s3 = boto3.resource('s3')
    bucket = s3.Bucket('gfw2-data')
    key = 'alerts-tsv/temp/output-glad-summary-{}/part-'.format(date_str)
    objs = list(bucket.objects.filter(Prefix=key))

    return len(objs) > 0 and objs[0].key == key


if __name__ == '__main__':
    main()

