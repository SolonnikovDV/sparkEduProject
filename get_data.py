from kaggle.api.kaggle_api_extended import KaggleApi
import os
from kaggle.api.kaggle_api_extended import KaggleApi
from zipfile import ZipFile
from pathlib import Path
import datetime

# local import
import config as cfg


DATA_SET = 'AnalyzeBoston/crimes-in-boston'

DATA_TIME = datetime.datetime.now()


def api_kaggle_connect():
    os.environ['KAGGLE_USERNAME'] = cfg.KAGGLE_USERNAME
    os.environ['KAGGLE_KEY'] = cfg.KAGGLE_KEY
    api = KaggleApi()
    return api


def download_crime_df():
    try:
        api = api_kaggle_connect()
        api.authenticate()
        api.dataset_download_file(DATA_SET, cfg.CSV_API_CRIME)
    except Exception as e:
        print(f'[INFO] : {DATA_TIME} >> Error API connection : ', e)

    if Path(cfg.CSV_CRIME + '.zip').is_file():
        with ZipFile('csv_data/crime.csv.zip', 'r') as unzip_data:
            unzip_data.extractall()
            print(f'[INFO] : {DATA_TIME} >> zip file was successful unpackage')
        return 'crime.csv'
    else:
        print(f'[INFO] : {DATA_TIME} >> there are no zip files needs to unpackage')
        pass


def download_offence_codes_df():
    api = api_kaggle_connect()
    api.authenticate()
    api.dataset_download_file(DATA_SET, cfg.CSV_API_OFFENSE_CODE, cfg.CSV_OFFENSE_CODES)


download_offence_codes_df()

if __name__ == '__main__':
    print('')