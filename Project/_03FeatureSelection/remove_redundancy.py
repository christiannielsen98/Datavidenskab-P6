import pandas as pd

from Project._03FeatureSelection.NZERTF_dataframe_redundancy_functions import create_redundancy_dataframes, \
    find_average_power_consumption
from Project.Database import Db


def remove_redundant_power_consumption_from_data():
    power_consumption = find_average_power_consumption()
    NZERTF_redundancy = create_redundancy_dataframes()
    for year in [1, 2]:
        NZERTF, NZERTF_meta = Db.load_data(hourly=False, meta=True, year=year)
        status_columns = NZERTF_redundancy[f'Year{year}'].columns[1:].tolist()
        NZERTF[status_columns] = NZERTF.where(NZERTF_redundancy[f'Year{year}'][status_columns] == 0)[
            status_columns].fillna(0)
        for status_att, meta_row in NZERTF_meta.loc[status_columns].iterrows():
            for index, row in NZERTF.loc[NZERTF_redundancy[f'Year{year}'][status_att] == 1].iterrows():
                NZERTF.loc[index, meta_row['Consumer_Match']] = max(meta_row['Standby_Power'],
                                                                    NZERTF.loc[index, meta_row['Consumer_Match']] -
                                                                    power_consumption[status_att])
        Db.pickle_dataframe(NZERTF, f"All-Subsystems-minute-year{year}_no_redundancy.pkl")


if __name__ == '__main__':
    remove_redundant_power_consumption_from_data()
