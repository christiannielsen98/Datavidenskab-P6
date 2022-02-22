import datetime

import pandas as pd
import numpy as np
import plotly.express as px
import plotly.graph_objects as go

from Project.Database import Db

year1_minute, meta = Db.load_data(hourly=False, meta=True, year=1)

consumption = meta.loc[((meta['Parameter'] == "Power_Electrical") | (meta['Parameter'] == "Power_Thermal")) & (
    meta["Description"].str.contains("power consumption" or "used"))]["Unnamed: 0"].tolist()

corr = year1_minute[consumption].corr()

pd.options.mode.chained_assignment = None

i=0

explained_dict = {}
correlation_dict = {}
columns = corr.columns.tolist()

for threshold in np.arange(1, 0.5, -0.05):
    for col in columns:
        if col not in [val for liste in explained_dict.values() for val in liste]:
            if corr[col].sum() > 1:
                i+=1
                a = corr.loc[corr[col] >= threshold, col].index.to_list()
                df2 = year1_minute[a]
                a.remove(col)
                df2["sum_of_correlated_attributes"] = df2[a].sum(1)
                sum_correlation = df2[["sum_of_correlated_attributes",col]].corr().loc["sum_of_correlated_attributes",col]
                if sum_correlation >= corr.loc[a, col].max():
                    if sum_correlation == 1:
                        explained_dict[col] = a.copy()
                        if col in correlation_dict.keys():
                            del correlation_dict[col]
                    elif col in correlation_dict.keys():
                        if correlation_dict[col]["corr"] < sum_correlation:
                            correlation_dict[col] = {"corr": sum_correlation,
                                                     "attr": a.copy()}
                    else: #if col not in explained_dict.keys():
                        correlation_dict[col] = {"corr": sum_correlation,
                                                     "attr": a.copy()}

    for key, value in explained_dict.items():
        if key in columns:
            columns.remove(key)
        for col in value:
            if col in columns:
                columns.remove(col)
