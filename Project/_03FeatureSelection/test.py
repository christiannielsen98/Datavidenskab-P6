import re

import pandas as pd
import plotly.express as px

from Project.Database import Db

year1_minutes, meta = Db.load_data(hourly=False, meta=True)
# year1_minutes = year1_minutes.loc[:1439]


consumers = meta.loc[(lambda self: (
    (self["Units"] == "W") &
    (self["Subsystem"] != "DHW") &
    (~self.index.str.contains("Child")) &
    (~self.index.str.contains("Parent"))
))].index.tolist()

status_cols = meta.loc[(lambda self: (
        (self["Parameter"] == "Status_OnOff") &
        (self["Subsystem"] == "Loads") &
        (~self.index.str.contains("Child")) &
        (~self.index.str.contains("Prnt"))
))].index.tolist()

year1_minutes = year1_minutes[["Timestamp"] + consumers + status_cols].copy()

consumer_dict = {}
for consumer in consumers:
    # interval = (year1_minutes[consumer].max() - year1_minutes[consumer].min()) * 0.00002
    # interval = year1_minutes[consumer].std() * 0.05
    # consumer_dict[consumer] = year1_minutes[(year1_minutes[consumer].shift(periods=1, axis=0) - interval > year1_minutes[consumer]) |
    #                                         (year1_minutes[consumer] > year1_minutes[consumer].shift(periods=1, axis=0) + interval)][
    #                        ["Timestamp", consumer]][1:]
    temp = year1_minutes[(year1_minutes[consumer] != year1_minutes[consumer].shift(periods=1, axis=0))][
        ["Timestamp", consumer]][1:]
    if temp.shape[0] < year1_minutes.shape[0] / 3:
        consumer_dict[consumer] = temp

appliance_dict = {}
for appliance in status_cols:
    appliance_switch = year1_minutes[(year1_minutes[appliance] != year1_minutes[appliance].shift(1))][
                           ["Timestamp", appliance]][1:]
    appliance_dict[appliance] = appliance_dict.get(appliance, [])
    for consumer, df in consumer_dict.items():
        if (appliance_switch.index.isin(df.index)).mean() > 0.5:
             appliance_dict[appliance].append(consumer)

print(appliance_dict)

# for i in consumers:
#     fig = px.scatter(year1_minutes, x=year1_minutes["Timestamp"], y=i)
#
#     height = 1
#     for appliance, df in appliance_dict.items():
#         for index in df.index:
#             x = df.loc[index, "Timestamp"].timestamp() * 1000 - (2 + int(str(df.loc[index, "Timestamp"]).split("-")[-1][
#                                                                              1])) * 3_600_000  #Plotly bug work around. Computes unix time with offset
#             fig.add_vline(x=x,
#                           line_width=3,
#                           line_dash="dash",
#                           annotation_text=df.loc[index, "Appliance"],
#                           line_color=df.loc[index, "Color"],
#                           annotation_y=height)
#         height -= 0.02
#
#     fig.update_layout(
#         xaxis={'type': 'date', 'tickformat': '%Y-%m-%d %H:%M:%S'}
#     )
#
#     fig.update_layout(
#         xaxis_tickformatstops=[
#             dict(dtickrange=[None, 60000], value="%H:%M:%S\n%d %b %y"),
#             dict(dtickrange=[60000, 3600000], value="%H:%M\n%d %b %y"),
#             dict(dtickrange=[3600000, 86400000], value="%d %b %Y")
#         ]
#     )
# # fig.write_html(Db.get_save_file_directory("Kitchen_plug_appliances_consumption.html"))
#     fig.show()