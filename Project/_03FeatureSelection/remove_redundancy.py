import pandas as pd
import numpy as np

from Project._03FeatureSelection.NZERTF_dataframe_redundancy_functions import create_redundancy_dataframes
from Project.Database import Db


def remove_redundancy(dataframe, redundancy_dataframe):
    pass


if __name__ == '__main__':
    NZERTF_redundancy = create_redundancy_dataframes()
    for year in [1, 2]:
        NZERTF, NZERTF_meta = Db.load_data(hourly=False, meta=True, year=year)
        status_columns = NZERTF_redundancy[f'Year{year}'].columns[1:].tolist()
        consumer_columns = NZERTF_meta.loc[status_columns, 'Consumer_Match'].tolist()
        # print(NZERTF[status_columns].sum(1).sum())
        # print(NZERTF[consumer_columns].sum(1).sum())
        # NZERTF = NZERTF.where(NZERTF_redundancy[f'Year{year}'][status_columns] == 0).fillna(0)
        for status_att, consumer in NZERTF_meta.loc[status_columns, 'Consumer_Match'].iteritems():
            NZERTF.loc[lambda self: self[status_att] == 0, consumer] = 0

        # print(NZERTF[status_columns].sum(1).sum())
        print(NZERTF[consumer_columns].sum(1).sum())
