import os

from pandas import DataFrame, read_csv, concat, period_range, to_datetime

from Database import Db


def create_production_dataframe(dataframe, filename):
    production_df = DataFrame()
    months = period_range(start=dataframe["Timestamp"].min(), end=dataframe["Timestamp"].max(), freq="M").strftime(
        "%Y%m")
    for file in os.listdir(Db.get_data_path("csv/production")):
        if file.split("_")[0] in months:
            production_df = concat(objs=(production_df, read_csv(Db.get_data_path(f"csv/production/{file}"))), axis=0)

    production_df["HOUR"] = to_datetime(arg=production_df["HOUR"], format="%d%b%Y:%H:%M:%S").dt.strftime(
        "%Y-%m-%d %H:%M:%S")
    production_df.sort_values(by="HOUR", inplace=True)
    production_df.reset_index(drop=True, inplace=True)
    transformed_production_df = DataFrame(columns=production_df["FUEL_TYPE"].unique(),
                                          index=production_df["HOUR"].unique()).fillna(0)
    for index in production_df.index:
        transformed_production_df.loc[production_df.loc[index, "HOUR"], production_df.loc[index, "FUEL_TYPE"]] = \
        production_df.loc[index, "PERCENT_MARGINAL"]

    transformed_production_df = transformed_production_df.loc[lambda self: self.index <= dataframe["Timestamp"].max()]

    Db.pickle_dataframe(dataframe=transformed_production_df, filename=filename)


data_dict_list = [{"dataframe": Db.load_data(hourly=True), "filename": "Production_year1.pkl"},
                  {"dataframe": Db.load_data(hourly=True, year=1), "filename": "Production_year2.pkl"}]

for data_dict in data_dict_list:
    create_production_dataframe(**data_dict)
