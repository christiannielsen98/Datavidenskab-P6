import pandas as pd
import plotly.graph_objects as go

from Project.Database import Db


def aggregate_conum_prod_sum(df: pd.DataFrame, meta_data: pd.DataFrame):
    consumption_condition = (lambda self: (~self['Consumer_Match'].isna()))

    consumption = meta_data.loc[consumption_condition].index.tolist()

    production = meta_data.loc[(meta_data['Parameter'] == "Power_Electrical") & (
        meta_data["Description"].str.contains("produced"))].index.tolist()

    df = df[["Timestamp"] + consumption + production].copy()

    df["Mean_consumption"] = df[consumption].sum(1)
    df["Mean_production"] = df[production].sum(1)

    return df[["Timestamp", "Mean_consumption", "Mean_production"]].copy()


def heatmap_generator(agg_consum_prod, x: str = "HourOfDay", y: str = "MonthOfYear"):
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
            yaxis={'categoryarray': ['Monday',
                                     'Tuesday',
                                     'Wednesday',
                                     'Thursday',
                                     'Friday',
                                     'Saturday',
                                     'Sunday']},
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
            yaxis={'categoryarray': ['January',
                                     'February',
                                     'March',
                                     'April',
                                     'May',
                                     'June',
                                     'Juli',
                                     'August',
                                     'September',
                                     'October',
                                     'November',
                                     'December']},
            font=dict(
                size=20
            ),
        )

    fig.update_xaxes(title_text=x)
    fig.update_yaxes(title_text=y)

    # fig.write_html(Db.get_save_file_directory(f"{x}-over-{y}.html"))
    fig.show()


def average_consumption_production_line(agg_consum_prod):
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


if __name__ == '__main__':
    for year in [1, 2]:
        house_df, meta = Db.load_data(hourly=True, meta=True, year=year)
        aggregate_conum_prod_sum = aggregate_conum_prod_sum(df=house_df, meta_data=meta)
        heatmap_generator(aggregate_conum_prod_sum, x="HourOfDay", y="DayOfWeek")
        heatmap_generator(aggregate_conum_prod_sum, x="HourOfDay", y="MonthOfYear")
        heatmap_generator(aggregate_conum_prod_sum, x="DayOfWeek", y="MonthOfYear")

        average_consumption_production_line(aggregate_conum_prod_sum)
