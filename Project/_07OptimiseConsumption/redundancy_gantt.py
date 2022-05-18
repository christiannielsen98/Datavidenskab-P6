import re

import pandas as pd
import plotly.express as px

from Project.Database import Db


def create_gantt_vis(year):
    appliance_job_list = []
    NZERTF_meta = Db.load_data(hourly=False, meta=True, year=year, consumption=False)
    calender_year = [2013, 2015][year - 1]

    # Extract appliances from the meta data
    appliance_condition = (lambda self: (
            (self["Parameter"] == "Status_OnOff") &
            (self["Subsystem"] == "Loads") &
            (~self.index.str.contains("SensHeat"))
    ))
    appliance_location = pd.DataFrame(
        NZERTF_meta.loc[appliance_condition, "Measurement_Floor"].sort_values(ascending=True))
    appliance_order = NZERTF_meta.loc[appliance_condition, ["Measurement_Floor", "Measurement_Location"]]
    appliance_order["Name"] = [
        " ".join(re.findall('[A-Z][^A-Z]*', "".join(''.join(app.split("PlugLoad")).split("Appliance")).split("Load_Status")[-1])) for app in
        appliance_order.index]
    appliance_order = appliance_order.sort_values(by=["Measurement_Floor", "Measurement_Location", "Name"])["Name"]

    for with_redundancy in [True, False]:

        NZERTF = Db.load_data(hourly=False, year=year, with_redundancy=with_redundancy).loc[lambda self: (
            self["Timestamp"].dt.strftime('%Y-%m-%d').between(f'{calender_year}-07-18', f'{calender_year}-07-21')
        )]

        # Combine minute data with the extracted appliances into a dataframe
        minute_appliances_status = NZERTF[["Timestamp"] + appliance_location.index.tolist()].copy()

        for appliance, appliance_row in appliance_location.iterrows():
            name = appliance_order[appliance]
            appliance_switch = minute_appliances_status[(
                        minute_appliances_status[appliance] != minute_appliances_status[appliance].shift(1))][
                                   ["Timestamp", appliance]][1:]
            appliance_switch.reset_index(inplace=True, drop=True)
            for index, row in appliance_switch.iterrows():
                if row[appliance]:
                    try:
                        appliance_job_list.append({"appliance": name, "start": row["Timestamp"],
                                                   "end": appliance_switch.loc[index + 1, "Timestamp"],
                                                   "location": {'1stFloor': "First floor", '2ndFloor': 'Second floor'}[
                                                       appliance_row["Measurement_Floor"]]})
                        if not with_redundancy:
                            appliance_job_list.append({"appliance": name, "start": row["Timestamp"],
                                                       "end": appliance_switch.loc[index + 1, "Timestamp"],
                                                       "location":
                                                           {'1stFloor': "First floor", '2ndFloor': 'Second floor'}[
                                                               appliance_row["Measurement_Floor"]]})
                    except:
                        continue

        residens_condition = (lambda self: (
                (self["Subsystem"] == "Loads") &
                (self["Parameter"] == "Status_OnOff") &
                (self.index.str.contains("Sens"))
        ))

        residens_location = NZERTF_meta.loc[residens_condition].index.tolist()
        residens_location.sort()

        # Combine minute data with the extracted residens into a dataframe
        residens_location_status = NZERTF[["Timestamp"] + residens_location].copy()
        person_status_list = []

        for col in residens_location:
            name = "".join(re.findall("[PC][a-z]+[AB]", col))
            person_location = residens_location_status[lambda self: (self[col] != self[col].shift(1))][
                                  ["Timestamp", col]][1:]

            df_indices = person_location.index.tolist()
            for index, df_index in enumerate(df_indices):
                if person_location.loc[df_index, col]:
                    try:
                        person_status_list.append(
                            {"appliance": name, "start": person_location.loc[df_index, "Timestamp"],
                             "end": person_location.loc[df_indices[index + 1], "Timestamp"],
                             "location": {"DOWN": "First floor", "UP": "Second floor"}[
                                 "".join(col.split(name)[-1])]})
                        if not with_redundancy:
                            person_status_list.append(
                                {"appliance": name, "start": person_location.loc[df_index, "Timestamp"],
                                 "end": person_location.loc[df_indices[index + 1], "Timestamp"],
                                 "location": {"DOWN": "First floor", "UP": "Second floor"}[
                                     "".join(col.split(name)[-1])]})
                    except:
                        continue
        appliance_job_list = appliance_job_list + person_status_list

    fig = px.timeline(appliance_job_list, x_start="start", x_end="end", y="appliance", color="location", opacity=0.34,
                      category_orders={"appliance": appliance_order.tolist() + ["PrntA", "PrntB", "ChildA", "ChildB"]})
    fig.update_layout(template='plotly')
    fig.write_html(Db.get_save_file_directory(f"Gantt/Person_appliance_status_gantt_year_{year}_group_floor.html"))


if __name__ == '__main__':
    for year in [1, 2]:
        create_gantt_vis(year)
