import pandas as pd
import plotly.graph_objects as go

from Project.Database import Db

year1_hourly, meta = Db.load_data(hourly=True, meta=True, year=1)
year2_hourly, meta = Db.load_data(hourly=True, meta=True, year=2)


def heatmap_generator(df, meta_data, x="HourOfDay", y="MonthOfYear"):
    consumption_condition = (lambda self: (
            (self["Units"] == "W") &
            self["Description"].str.contains("power consumption" or "used") &
            ~(self.index == "Elec_PowerMicrowave") &
            ~(self.index == "Elec_PowerRefrigerator") &
            ~(self.index == "Elec_PowerClothesWasher") &
            ~self.index.str.contains("Elec_[\w]+HVAC") &
            (~self.index.str.contains("DHW_") |
             self.index.str.contains("DHW_[\w]+Total")) &
            (~self.index.str.contains("Load_") |
             ~self.index.str.contains("Load_Microwave") &
             self.index.str.contains("Load_[\w]+Standby"))))

    consumption = meta_data.loc[consumption_condition].index.tolist()

    production = meta_data.loc[(meta_data['Parameter'] == "Power_Electrical") & (
        meta_data["Description"].str.contains("produced"))].index.tolist()

    year1_consumption_production = df[["Timestamp"] + consumption + production].copy()

    year1_consumption_production["Mean_consumption"] = year1_consumption_production[consumption].sum(1)
    year1_consumption_production["Mean_production"] = year1_consumption_production[production].sum(1)

    agg_consum_prod = year1_consumption_production[["Timestamp", "Mean_consumption", "Mean_production"]].copy()

    # Forces Timestamp to the type of datetime, to extract the hour of Timestamp.
    agg_consum_prod['Timestamp'] = pd.to_datetime(agg_consum_prod['Timestamp'], errors="coerce", utc=True,
                                                  format="%Y-%m-%d %H:%M:%S%z")

    # Extracts hour of Timestamp.
    agg_consum_prod["MonthOfYear"] = agg_consum_prod.Timestamp.dt.month
    months = {1: 'January', 2: 'February', 3: 'March', 4: 'April', 5: 'May', 6: 'June', 7: 'Juli', 8: 'August',
              9: 'September', 10: 'October', 11: 'November', 12: 'December'}
    agg_consum_prod["MonthOfYear"] = agg_consum_prod["MonthOfYear"].map(months)
    agg_consum_prod["DayOfWeek"] = agg_consum_prod.Timestamp.dt.dayofweek
    days = {0: 'Monday', 1: 'Tuesday', 2: 'Wednesday', 3: 'Thursday', 4: 'Friday', 5: 'Saturday', 6: 'Sunday'}
    agg_consum_prod["DayOfWeek"] = agg_consum_prod["DayOfWeek"].map(days)
    agg_consum_prod["HourOfDay"] = agg_consum_prod.Timestamp.dt.hour

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
        zmid=0,
    )
    )
    if x == 'DayOfWeek' or y == 'DayOfWeek':
        fig.update_layout(margin=dict(
            l=30,
            r=30,
            b=30,
            t=50,
        ), xaxis_nticks=30,
            yaxis_nticks=10,
            yaxis={'categoryarray': ['Monday', 'Tuesday', 'Wednesday', 'Thursday', 'Friday', 'Saturday', 'Sunday']},
            font=dict(
                size=20
            ),
        )
    else:
        fig.update_layout(
            margin=dict(
                l=30,
                r=30,
                b=30,
                t=50,
            ),
            xaxis_nticks=30,
            yaxis_nticks=20,
            yaxis={'categoryarray': ['January', 'February', 'March', 'April', 'May', 'June', 'Juli', 'August',
                                     'September', 'October', 'November', 'December']},
            font=dict(
                size=20
            ),
        )

    fig.update_xaxes(title_text=x)
    fig.update_yaxes(title_text=y)

    fig.write_html(Db.get_save_file_directory(f"{x}-over-{y}.html"))
    # fig.show()


# heatmap_generator(year1_hourly, meta, x="HourOfDay", y="DayOfWeek")
# heatmap_generator(year1_hourly, meta, x="HourOfDay", y="MonthOfYear")


# heatmap_generator(year1_hourly, meta, x="WeekdayOfTimestamp", y="MonthOfYear")


def average_consumption_production_line(df, meta_data):
    consumption_condition = (lambda self: (
            (self["Units"] == "W") &
            self["Description"].str.contains("power consumption" or "used") &
            ~(self.index == "Elec_PowerMicrowave") &
            ~(self.index == "Elec_PowerRefrigerator") &
            ~(self.index == "Elec_PowerClothesWasher") &
            ~self.index.str.contains("Elec_[\w]+HVAC") &
            (~self.index.str.contains("DHW_") |
             self.index.str.contains("DHW_[\w]+Total")) &
            (~self.index.str.contains("Load_") |
             ~self.index.str.contains("Load_Microwave") &
             self.index.str.contains("Load_[\w]+Standby"))))

    consumption = meta_data.loc[consumption_condition].index.tolist()

    production = meta_data.loc[(meta_data['Parameter'] == "Power_Electrical") & (
        meta_data["Description"].str.contains("produced"))].index.tolist()

    year1_consumption_production = df[["Timestamp"] + consumption + production].copy()

    year1_consumption_production["Mean_consumption"] = year1_consumption_production[consumption].sum(1)
    year1_consumption_production["Mean_production"] = year1_consumption_production[production].sum(1)

    agg_consum_prod = year1_consumption_production[["Timestamp", "Mean_consumption", "Mean_production"]].copy()

    # Forces Timestamp to the type of datetime, to extract the hour of Timestamp.
    agg_consum_prod['Timestamp'] = pd.to_datetime(agg_consum_prod['Timestamp'], errors="coerce", utc=True,
                                                  format="%Y-%m-%d %H:%M:%S%z")

    # Extracts hour of Timestamp.
    agg_consum_prod["MonthOfYear"] = agg_consum_prod.Timestamp.dt.month
    agg_consum_prod["WeekdayOfTimestamp"] = agg_consum_prod.Timestamp.dt.dayofweek
    agg_consum_prod["HourOfDay"] = agg_consum_prod.Timestamp.dt.hour

    agg_consum_prod["Proportion_of_consumption_production"] = agg_consum_prod["Mean_production"] - agg_consum_prod[
        "Mean_consumption"]
    hourON = agg_consum_prod.groupby(["HourOfDay"]).mean()
    hourON.reset_index(inplace=True)

    fig = go.Figure(data=[
        go.Line(name='Mean_consumption', y=(hourON["Mean_consumption"])),
        go.Line(name='Mean_production', y=hourON["Mean_production"]),
        go.Line(name='Mean_energy_balance', y=(hourON["Mean_production"] - hourON["Mean_consumption"]))
    ]
    )

    fig.update_layout(
        # margin=dict(
        # l=30,
        # r=30,
        # b=30,
        # t=50,
        # ),
        xaxis_nticks=30,
        yaxis_nticks=20,
        font=dict(
            size=16
        ),
    )
    fig.update_xaxes(title_text="Hour of day")
    fig.update_yaxes(title_text="Energy (wH)")

    # fig.write_html(Db.get_save_file_directory(f"average_consumption_production_line.html"))
    fig.show()


average_consumption_production_line(df=year1_hourly, meta_data=meta)
average_consumption_production_line(df=year2_hourly, meta_data=meta)
