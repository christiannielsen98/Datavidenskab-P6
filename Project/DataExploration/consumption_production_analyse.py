import pandas as pd
import plotly.graph_objects as go

from Project.Database import Db

year1_hourly, meta = Db.load_data(hourly=True, meta=True, year=1)


def heatmap_generator(df, meta_data, x="HourOfTimestamp", y="MonthOfTimestamp"):

    consumption_condition = (lambda self: (
            (self["Units"] == "W") &
            self["Description"].str.contains("power consumption" or "used") &
            ~(self["Unnamed: 0"] == "Elec_PowerMicrowave") &
            ~(self["Unnamed: 0"] == "Elec_PowerRefrigerator") &
            ~(self["Unnamed: 0"] == "Elec_PowerClothesWasher") &
            ~self["Unnamed: 0"].str.contains("Elec_[\w]+HVAC") &
            (~self["Unnamed: 0"].str.contains("DHW_") |
             self["Unnamed: 0"].str.contains("DHW_[\w]+Total")) &
            (~self["Unnamed: 0"].str.contains("Load_") |
             ~self["Unnamed: 0"].str.contains("Load_Microwave") &
             self["Unnamed: 0"].str.contains("Load_[\w]+Standby"))))

    consumption = meta_data.loc[consumption_condition]["Unnamed: 0"].tolist()

    production = meta_data.loc[(meta_data['Parameter'] == "Power_Electrical") & (
        meta_data["Description"].str.contains("produced"))]["Unnamed: 0"].tolist()

    year1_consumption_production = df[["Timestamp"] + consumption + production].copy()

    year1_consumption_production["Mean_consumption"] = year1_consumption_production[consumption].sum(1)
    year1_consumption_production["Mean_production"] = year1_consumption_production[production].sum(1)

    agg_consum_prod = year1_consumption_production[["Timestamp", "Mean_consumption", "Mean_production"]].copy()

    # Forces Timestamp to the type of datetime, to extract the hour of Timestamp.
    agg_consum_prod['Timestamp'] = pd.to_datetime(agg_consum_prod['Timestamp'], errors="coerce", utc=True,
                                                  format="%Y-%m-%d %H:%M:%S%z")

    # Extracts hour of Timestamp.
    agg_consum_prod["MonthOfTimestamp"] = agg_consum_prod.Timestamp.dt.month
    agg_consum_prod["WeekdayOfTimestamp"] = agg_consum_prod.Timestamp.dt.dayofweek
    agg_consum_prod["HourOfTimestamp"] = agg_consum_prod.Timestamp.dt.hour

    agg_consum_prod["Proportion_of_consumption_production"] = agg_consum_prod["Mean_production"] - agg_consum_prod[
        "Mean_consumption"]
    hourON = agg_consum_prod.groupby([y, x]).mean()
    hourON.reset_index(inplace=True)

    # Create heatmap
    fig = go.Figure(data=go.Heatmap(
        z=hourON["Proportion_of_consumption_production"],
        x=hourON[x],
        y=hourON[y],
        colorscale="RdBu",
        zmid=0
    )
    )

    fig.update_layout(margin=dict(
        l=30,
        r=30,
        b=30,
        t=50,
    ), xaxis_nticks=30,
        yaxis_nticks=20
        # annotations=annotations,
    )

    fig.show()


# heatmap_generator(year1_hourly, meta, x="HourOfTimestamp", y="WeekdayOfTimestamp")


def average_consumption_production_line(df, meta_data):
    consumption_condition = (lambda self: (
            (self["Units"] == "W") &
            self["Description"].str.contains("power consumption" or "used") &
            ~(self["Unnamed: 0"] == "Elec_PowerMicrowave") &
            ~(self["Unnamed: 0"] == "Elec_PowerRefrigerator") &
            ~(self["Unnamed: 0"] == "Elec_PowerClothesWasher") &
            ~self["Unnamed: 0"].str.contains("Elec_[\w]+HVAC") &
            (~self["Unnamed: 0"].str.contains("DHW_") |
             self["Unnamed: 0"].str.contains("DHW_[\w]+Total")) &
            (~self["Unnamed: 0"].str.contains("Load_") |
             ~self["Unnamed: 0"].str.contains("Load_Microwave") &
             self["Unnamed: 0"].str.contains("Load_[\w]+Standby"))))

    consumption = meta_data.loc[consumption_condition]["Unnamed: 0"].tolist()

    production = meta_data.loc[(meta_data['Parameter'] == "Power_Electrical") & (
        meta_data["Description"].str.contains("produced"))]["Unnamed: 0"].tolist()

    year1_consumption_production = df[["Timestamp"] + consumption + production].copy()

    year1_consumption_production["Mean_consumption"] = year1_consumption_production[consumption].sum(1)
    year1_consumption_production["Mean_production"] = year1_consumption_production[production].sum(1)

    agg_consum_prod = year1_consumption_production[["Timestamp", "Mean_consumption", "Mean_production"]].copy()

    # Forces Timestamp to the type of datetime, to extract the hour of Timestamp.
    agg_consum_prod['Timestamp'] = pd.to_datetime(agg_consum_prod['Timestamp'], errors="coerce", utc=True,
                                                  format="%Y-%m-%d %H:%M:%S%z")

    # Extracts hour of Timestamp.
    agg_consum_prod["MonthOfTimestamp"] = agg_consum_prod.Timestamp.dt.month
    agg_consum_prod["WeekdayOfTimestamp"] = agg_consum_prod.Timestamp.dt.dayofweek
    agg_consum_prod["HourOfTimestamp"] = agg_consum_prod.Timestamp.dt.hour

    agg_consum_prod["Proportion_of_consumption_production"] = agg_consum_prod["Mean_production"] - agg_consum_prod[
        "Mean_consumption"]
    hourON = agg_consum_prod.groupby(["HourOfTimestamp"]).mean()
    hourON.reset_index(inplace=True)


    fig = go.Figure(data=[
        go.Line(name='Mean_consumption', y=(hourON["Mean_consumption"])),
        go.Line(name='Mean_production', y=hourON["Mean_production"]),
        go.Line(name='Mean_energy_balance', y=(hourON["Mean_production"] - hourON["Mean_consumption"]))
    ]
    )

    fig.update_layout(margin=dict(
        l=30,
        r=30,
        b=30,
        t=50,
    ), xaxis_nticks=30,
        yaxis_nticks=20
        # annotations=annotations,
    )

    fig.show()

average_consumption_production_line(df=year1_hourly, meta_data=meta)