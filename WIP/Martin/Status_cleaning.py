import pandas as pd
import numpy as np

from Project.Database import Db

from difflib import SequenceMatcher

for hourly in [True, False]:
    for year in [1, 2]:
        house, meta = Db.load_data(hourly=hourly, meta=True, year=year)

        meta.set_index("Unnamed: 0", inplace=True)
        meta = meta.loc[~((~pd.isnull(house)).sum(0) <= (house.shape[0] / 2))]

        status_condition = (lambda self: self["Units"] == "Binary Status")
        status_columns = meta.loc[status_condition].index.tolist()

        house[(house[status_columns] != 0) & (house[status_columns] != 1)] = np.NaN
        # house[status_columns] = house[status_columns].ffill()
        house = house.ffill()

        meta = meta.loc[(house != house.shift(1)).sum(0) > 1]
        house = house[meta.index]

        status_columns = meta.loc[status_condition].index.tolist()
        power_columns = meta.loc[lambda self: self["Units"] == "W"].index.tolist()

        status_switch_dict = {}
        current_change_dict = {}

        for col in status_columns + power_columns:
            condition_value = (house[col].max() - house[col].min()) / 1000
            if col in status_columns:
                status_switch_dict[col] = house.loc[house[col] != house[col].shift(1), col]
            elif 0 < (house[col] != house[col].shift(1)).sum() / house.shape[0] <= 0.7:
                current_change_dict[col] = house.loc[house[col] != house[col].shift(1), col]

        for key_1, value_1 in status_switch_dict.items():
            for key_2, value_2 in current_change_dict.items():
                # if meta.loc[key_1, "Measurement_Location"] == meta.loc[key_2, "Measurement_Location"]:
                #     test = 0
                #     test_1 = 0
                if len(set(value_1.index) - set(value_2.index)) < len(value_1) / 10 and \
                    round(len(set(value_1.index)) / len(set(value_2.index)), 3) > 0.2:
                    print(key_1)
                    print(key_2)
                    print("    Status switched count          :",
                          len(value_1))
                    print("    Unexplained status switch count:",
                          len(set(value_1.index) - set(value_2.index)))
                    print("        Unexplained proportion     :",
                          round(len(set(value_1.index) - set(value_2.index)) / len(set(value_1.index)), 3))
                    print("    Proportion of Elec changes     :",
                          round(len(set(value_1.index)) / len(set(value_2.index)), 3))
                    print("")
                    # for index in value_1.index:
                    #     test += 1
                    #     if index in value_2.index:
                    #         test_1 += 1
                    # if test_1/test == 1:
                    #     print(key_1, key_2)
                    #     print(test_1/test)
                    # for index in value_2.index:
                    #     test += 1
                    #     if index in value_1.index:
                    #         test_1 += 1
                    #     # if value_1.index.tolist() == value_2.index.tolist():
                    # # if test_1/test >= 0.5:
                    # print(key_2, key_1)
                    # print(test_1/test)
        break
    break
