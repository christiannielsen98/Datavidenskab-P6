import configparser
import os
from pickle import dump, load
from pandas import read_csv

from project_root import project_path


class __Database:
    def __init__(self):
        """
        This constructor method creates container attributes for directory paths.
        config_path is hard coded.
        database_path is loaded from the Config file.
        """
        self.__config_path = os.path.join(project_path, 'Config')
        self.__onedrive_path = self.__config_load()
        self.__data_path = self.get_onedrive_path("data/netzero-data")
        self.__model_path = os.path.join(project_path, "Project", "Models")
        self.__ensure_dataframes()

    def __config_load(self):
        """
        Loads database path.
        """
        config = configparser.ConfigParser()

        config.read(os.path.join(self.__config_path, os.listdir(self.__config_path)[0]))

        return config['onedrive']['path']

    def __find_new_files(self):
        csv = set()
        pkl = set()
        for file in os.listdir(self.__data_path):
            if file.split('.')[-1] == "csv":
                csv.add(os.path.join(self.__data_path, file.split(".")[0]))
            elif file.split('.')[-1] == "pkl":
                pkl.add(os.path.join(self.__data_path, file.split(".")[0]))

        return csv - pkl

    def __ensure_dataframes(self):
        for new_file in self.__find_new_files():
            with open(file=f"{new_file}.pkl", mode="wb") as pkl_file:
                dump(obj=read_csv(f"{new_file}.csv"), file=pkl_file)

    def get_onedrive_path(self, sub_folder=None):
        """
        Returns the directory path to the database.
        """
        path = self.__onedrive_path

        if sub_folder is None:
            return path

        else:
            if isinstance(sub_folder, (list, tuple)):
                return os.path.join(path, *sub_folder)
            elif "\\" in sub_folder:
                return os.path.join(path, *sub_folder.split("\\"))
            else:
                return os.path.join(path, *sub_folder.split("/"))

    def load_data(self, hourly=True, year=1, meta=False):
        time_base = "hour" if hourly else "minute"
        with open(
                file=os.path.join(self.__data_path, f"All-Subsystems-{time_base}-year{year}.pkl"),
                mode="rb") as pkl_file:
            data = load(pkl_file)
        if meta:
            with open(
                    file=os.path.join(self.__data_path, f"Metadata-minute-year{year}.pkl"), mode="rb") as pkl_file:
                return data, load(pkl_file)

        return data


Db = __Database()
