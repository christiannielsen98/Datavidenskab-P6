import pandas as pd
import numpy as np

from Project.Database import Db

for hourly in [True, False]:
    for year in [1, 2]:
        time_base = "hour" if hourly else "minute"
        house, meta = Db.load_data(hourly=hourly, meta=True, year=year)

        # meta.set_index("Unnamed: 0", inplace=True)
        # meta = meta.loc[~((~pd.isnull(house)).sum(0) <= (house.shape[0] / 2))]
        #
        # status_condition = (lambda self: self["Units"] == "Binary Status")
        # status_columns = meta.loc[status_condition].index.tolist()
        #
        # house[(house[status_columns] != 0) & (house[status_columns] != 1)] = np.NaN
        # house = house.ffill()
        #
        # meta = meta.loc[(house != house.shift(1)).sum(0) > 1]
        # house = house[meta.index]

        house["Timestamp"] = pd.to_datetime(house["Timestamp"], format="%Y-%m-%d %H:%M:%S%z")

        Db.pickle_dataframe(dataframe=meta, filename=f"Metadata-{time_base}-year{year}.pkl")
        Db.pickle_dataframe(dataframe=house, filename=f"All-Subsystems-{time_base}-year{year}.pkl")
