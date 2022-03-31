import os
from difflib import SequenceMatcher

import plotly.express as px
from pandas import DataFrame, read_csv, read_html, concat, period_range, to_datetime, date_range

from Project.Database import Db


def create_production_dataframe(dataframe, filename):
    production_df = DataFrame()
    months = period_range(start=dataframe["Timestamp"].min(), end=dataframe["Timestamp"].max(), freq="M").strftime(
        "%Y%m")
    for file in os.listdir(Db.get_data_path("csv/production")):
        if file.split("_")[0] in months:
            production_df = concat(objs=(production_df, read_csv(Db.get_data_path(f"csv/production/{file}"))), axis=0)

    production_df["HOUR"] = to_datetime(arg=production_df["HOUR"], format="%d%b%Y:%H:%M:%S").dt.strftime(
        "%Y-%m-%d %H:%M:%S")

    production_df = production_df.loc[lambda self: ~self["FUEL_TYPE"].isin(
        ["Min Gen/Dispatch Reset", "Miscellaneous", "Demand Response", "Missing Data", "Virtual Sale at NY",
         "Virtual Sale at MISO"])]

    production_df.sort_values(by="HOUR", inplace=True)
    production_df.reset_index(drop=True, inplace=True)

    transformed_production_df = DataFrame(columns=production_df["FUEL_TYPE"].unique(),
                                          index=date_range(start=production_df["HOUR"].min(),
                                                           end=production_df["HOUR"].max(),
                                                           freq="H")).fillna(0)

    for index in production_df.index:
        transformed_production_df.loc[production_df.loc[index, "HOUR"], production_df.loc[index, "FUEL_TYPE"]] = \
            production_df.loc[index, "PERCENT_MARGINAL"]

    transformed_production_df = transformed_production_df.loc[lambda self: self.index <= dataframe["Timestamp"].max()]

    unknown_source_columns = {"Min Gen/Dispatch Reset", "Miscellaneous", "Demand Response", "Missing Data",
                              "Virtual Sale at NY", "Virtual Sale at MISO"}

    unknown_source_columns = unknown_source_columns - (unknown_source_columns - set(transformed_production_df.columns))

    for col in unknown_source_columns:
        transformed_production_df.drop(columns=col, inplace=True)

    transformed_production_df = transformed_production_df.loc[:, lambda self: self.max(0) != 0]

    # transformed_production_df = transformed_production_df.T.div(transformed_production_df.sum(1).values,
    #                                                             fill_value=0).T

    transformed_production_df['Coal'] = transformed_production_df['Coal'] + 1 - transformed_production_df.sum(1)

    # print(transformed_production_df)
    # transformed_production_df["Hour"] = transformed_production_df.index.hour
    # print(transformed_production_df.fillna(0).loc[lambda self: self["Hour"] == 1].loc[
    #           lambda self: (self.sum(1) == 0), lambda self: ~self.columns.isin(["Hour"])])

    # for hour in transformed_production_df["Hour"]:
    #     transformed_production_df.loc[lambda self: self["Hour"] == hour].loc[
    #         lambda self: (self.fillna(0).sum(1) == 0), lambda self: ~self.columns.isin(["Hour"])].fillna(
    #         transformed_production_df.loc[lambda self: self["Hour"] == hour].loc[
    #             lambda self: (self.fillna(0).sum(1) != 0), lambda self: ~self.columns.isin(["Hour"])].fillna(0).mean(
    #             0), inplace=True)

    # transformed_production_df.drop("Hour", inplace=True)

    transformed_production_df = find_co2_emissions(transformed_production_df)

    # Db.pickle_dataframe(dataframe=transformed_production_df, filename=filename)


def find_co2_emissions(production):
    production_source_df = DataFrame(columns=["EnergySource", "BTU/kWh", "CO2(Grams)/BTU"])

    production_source_df["EnergySource"] = production.columns.values

    col_dict = {
        "Renewable": {
            "EnergySource": ["Wind", "Solar"],
            "BTU/kWh": 9541  # Source: https://www.eia.gov/totalenergy/data/monthly/pdf/sec12_7.pdf year 2013
        },
        "Nuclear": {
            "EnergySource": ["Uranium"],
            "BTU/kWh": 10449  # Source: https://www.eia.gov/totalenergy/data/monthly/pdf/sec12_7.pdf year 2013
        },
        "Coal": {
            "EnergySource": ["Coal", "Waste Coal"],
            "BTU/kWh": 10459  # Source: https://www.eia.gov/totalenergy/data/monthly/pdf/sec12_7.pdf year 2013
        },
        "Gas": {
            "EnergySource": ["Natural Gas", "Land Fill Gas"],
            "BTU/kWh": 7948  # Source: https://www.eia.gov/totalenergy/data/monthly/pdf/sec12_7.pdf year 2013
        },
        "Petroleum": {
            "EnergySource": ["Light Oil", "Heavy Oil", "Diesel", "Kerosene"],
            "BTU/kWh": 10713  # Source: https://www.eia.gov/totalenergy/data/monthly/pdf/sec12_7.pdf year 2013
        },
        "Waste": {
            "EnergySource": ["Municipal Waste"],
            "BTU/kWh": 17000
            # About 20% waste to energy efficiancy source: https://www.nrel.gov/docs/fy13osti/52829.pdf (page 22, search page: 30)
        }
    }

    co2_cols = ["Coal", "Waste Coal", "Natural Gas", "Land Fill Gas", "Light Oil", "Heavy Oil", "Diesel", "Kerosene",
                "Municipal Waste"]

    for energy_group, values in col_dict.items():
        for energy_source in values["EnergySource"]:
            production_source_df.loc[production_source_df["EnergySource"] == energy_source, "BTU/kWh"] = values[
                "BTU/kWh"]

    CO2_per_BTU = read_html("https://www.eia.gov/environment/emissions/co2_vol_mass.php")[0].loc[1:]

    emission_df = DataFrame(columns=["EnergySource", "CO2(Grams)/BTU"])
    emission_df["EnergySource"] = CO2_per_BTU["Unnamed: 0_level_0"]["Carbon Dioxide (CO2) Factors:"].values
    emission_df["KilogramsCO2PerMillionBtu"] = CO2_per_BTU["Kilograms CO2"]["Per Million Btu"].values
    emission_df = emission_df.loc[lambda self: self["KilogramsCO2PerMillionBtu"].str.contains("[0-9]+\\.[0-9]+")]

    emission_df["CO2(Grams)/BTU"] = emission_df["KilogramsCO2PerMillionBtu"].astype("float") / 1e3
    emission_df.drop("KilogramsCO2PerMillionBtu", axis=1)

    column_matches_dict = {}

    for i in co2_cols:
        column_matches_dict[i] = column_matches_dict.get(i, {})
        for j in i.split(" "):
            column_matches_dict[i][j] = column_matches_dict[i].get(j, set())
            column_matches_dict[i][j].update(
                set(emission_df.loc[emission_df["EnergySource"].str.contains(j)]["EnergySource"].tolist()))
    emission_df.set_index("EnergySource", inplace=True)

    column_translator_dict = {}

    for key, sub_dict in column_matches_dict.items():
        column_translator_dict[key] = column_translator_dict.get(key, {})
        for sub_key, source_set in sub_dict.items():
            for value in source_set:
                column_translator_dict[key][str(SequenceMatcher(a=key, b=value).ratio())] = value
        try:
            column_translator_dict[key] = column_translator_dict[key][max(column_translator_dict[key].keys())]
        except:
            continue

    column_translator_dict.update({
        'Waste Coal': column_translator_dict["Coal"],
        'Light Oil': column_translator_dict["Diesel"],
        'Heavy Oil': column_translator_dict["Diesel"]
    })

    production_source_df["CO2(Grams)/BTU"] = [
        emission_df["CO2(Grams)/BTU"][column_translator_dict[source]] if source in co2_cols else 0 for source in
        production_source_df["EnergySource"]]

    production_source_df["CO2(Grams)/kWh"] = (
            production_source_df["BTU/kWh"] * production_source_df["CO2(Grams)/BTU"]).astype("float").round(2)

    production["CO2(Grams)/kWh"] = production_source_df["CO2(Grams)/kWh"].T.values.dot(production.values.T)

    fig = px.line(production, x=production.index, y="CO2(Grams)/kWh")
    fig.show()

    return production


data_dict_list = [{"dataframe": Db.load_data(hourly=True), "filename": "Production_year1.pkl"},
                  {"dataframe": Db.load_data(hourly=True, year=2), "filename": "Production_year2.pkl"}]

for data_dict in data_dict_list:
    create_production_dataframe(**data_dict)
