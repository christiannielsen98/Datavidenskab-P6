import pandas as pd
import plotly.express as px
import re
import datetime
from concurrent.futures import ThreadPoolExecutor, as_completed
import plotly.graph_objects as go

from Project.Database import Db

# year1_hourly, meta = Db.load_data(hourly=True, meta=True, year=1)
# year2_hourly, meta = Db.load_data(hourly=True, meta=True, year=2)
#
# consumption = meta.loc[((meta['Parameter'] == "Power_Electrical") | (meta['Parameter'] == "Power_Thermal")) & (
#     meta["Description"].str.contains("power consumption" or "used"))].index.tolist()
#
# # Forces Timestamp to the type of datetime, to extract the hour of Timestamp.
# year1_hourly["Timestamp"] = pd.to_datetime(year1_hourly["Timestamp"], format="%Y-%m-%d %H:%M:%S%z",
#                                            utc=True) - pd.to_timedelta(unit="h", arg=(
# year1_hourly["Timestamp"].str.split(pat="-", expand=True)[lambda self: self.columns[-1]].str[1]).astype(int))
#
# # Extracts hour of Timestamp.
# year1_hourly["HourOfTimestamp"] = year1_hourly.Timestamp.dt.hour
#
# # Replaces values to be binary 0 or 1.
# for i in consumption:
#     year1_hourly.loc[year1_hourly[i] < 0, i] = 0
#     year1_hourly.loc[year1_hourly[i] > 0, i] = 1
#
# # Creates a dataframe with counts of "StatusOffOn" = 1, grouped by HourOfTimestamp for all attributes in year1_hours
# hourON_all = year1_hourly.groupby(["HourOfTimestamp"])[consumption].sum()


def normalise_dataframe(df):
    '''
    Normalises values of all attributes in a dataframe, with minmax method.
    :param df:
    :type df:
    :return:
    :rtype:
    '''
    normalised_df = df.copy()

    for column in normalised_df.columns:
        if normalised_df[column].max() != 0:
            normalised_df[column] = (normalised_df[column] - normalised_df[column].min()) / (
                    normalised_df[column].max() - normalised_df[column].min())
        else:
            normalised_df[column] = normalised_df[column]

    return normalised_df


# # set up data to heatmap
# columns = hourON_all.columns[1:].tolist()
# hourON_all[columns] = normalise_dataframe(hourON_all[columns])
# hourON_all["HourOfTimestamp"] = hourON_all.index
# z_data = []
# for i in hourON_all[columns]:
#     data_list = hourON_all[i].round(2).values.tolist()
#     z_data.append(data_list)
#
# # Annotate z_values
# annotations = go.Annotations()
# for n, row in enumerate(z_data):
#     for m, val in enumerate(row):
#         annotations.append(go.Annotation(text="" if z_data[n][m] == 0 else str(z_data[n][m]),
#                                          x=hourON_all["HourOfTimestamp"][m], y=columns[n], xref='x1', yref='y1',
#                                          showarrow=False))
#
# # Create heatmap
# fig = go.Figure(data=go.Heatmap(
#     z=z_data,
#     x=hourON_all["HourOfTimestamp"],
#     y=columns,
#     colorscale='Teal')
# )
#
# fig.update_layout(
#     margin=dict(
#         l=30,
#         r=30,
#         b=30,
#         t=50, ),
#     xaxis_nticks=30,
#     annotations=annotations)
#
# fig.update_xaxes(title="Hour of day")
# fig.update_yaxes(title="Appliance", showticklabels=True)
#
# # fig.write_html(Db.get_save_file_directory(f"Appliance.html"))
# fig.show()
