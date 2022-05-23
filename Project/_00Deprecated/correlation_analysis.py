import numpy as np
import pandas as pd

from Project.Database import Db

pd.options.mode.chained_assignment = None

year1_minute, meta = Db.load_data(hourly=False, meta=True, year=1)

consumption = meta.loc[((meta['Parameter'] == "Power_Electrical") | (meta['Parameter'] == "Power_Thermal")) & (
    meta["Description"].str.contains("power consumption" or "used"))].index.tolist()

correlation_df = year1_minute[consumption].corr()
np.fill_diagonal(correlation_df.values, np.NaN)
condition = (correlation_df[(correlation_df > 0)].sum(1) > 0)
correlation_df = correlation_df.loc[condition, condition]

explained_dict = {}
correlation_dict = {}
columns = set(correlation_df.columns.tolist())
explained_columns = set()

for threshold in np.arange(1, 0.5, -0.025):
    condition = (correlation_df[(correlation_df > threshold)].sum(1) > 0)
    df = correlation_df.loc[list(columns), list(columns)].loc[condition, condition]
    for col in df.columns:
        correlated_columns = df.loc[df[col] >= threshold, col].index.to_list()
        df2 = year1_minute[correlated_columns + [col]]
        df2["sum_of_correlated_attributes"] = df2[correlated_columns].sum(1)
        sum_correlation = df2[["sum_of_correlated_attributes", col]].corr().loc[
            "sum_of_correlated_attributes", col]
        if sum_correlation > 0:
            if sum_correlation == 1:
                explained_dict[col] = correlated_columns.copy()
                explained_columns.add(col)
                explained_columns.update(correlated_columns)
            elif col not in explained_columns:
                if col in correlation_dict.keys():
                    if correlation_dict[col]["corr"] < sum_correlation:
                        correlation_dict[col] = {"corr": sum_correlation,
                                                 "attr": correlated_columns.copy()}
                else:
                    correlation_dict[col] = {"corr": sum_correlation,
                                             "attr": correlated_columns.copy()}

    columns -= explained_columns


def find_uniques(dictionary):
    keys = set()
    temp_dict = {}
    for key, value in dictionary.items():
        if key not in explained_columns:
            keys.add(key)
            if len(value["attr"]) > 1:
                condition = True
                for col in value["attr"]:
                    condition = condition and col not in explained_columns
                if condition:
                    temp_dict.update({key: value})
            elif not (value["attr"][0] in keys or value["attr"][0] in explained_columns):
                temp_dict.update({key: value})

    return temp_dict


correlation_dict = find_uniques(correlation_dict)
standby_cols = {}
for key, value in correlation_dict.items():
    if "standby" in key.lower():
        for col in value["attr"]:
            standby_cols.update({col: key})
    else:
        for col in value["attr"]:
            if "standby" in col.lower():
                standby_cols.update({key: col})

print(explained_dict)
print(correlation_dict)
print(standby_cols)
