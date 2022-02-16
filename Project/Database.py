import configparser
import os
import shutil
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
        self.__create_pickled_dataframes()

    def __config_load(self):
        """
        Loads database path.
        """
        config = configparser.ConfigParser()

        config.read(os.path.join(self.__config_path, os.listdir(self.__config_path)[0]))

        return config['onedrive']['path']

    def __move_files(self):
        for file in os.listdir(self.get_data_path()):
            suffix = file.split(".")[-1]
            if not os.path.isdir(self.get_data_path(file)):
                if not os.path.isdir(self.get_data_path(suffix)):
                    os.mkdir(self.get_data_path(suffix))
                shutil.move(self.get_data_path(file),
                            self.get_data_path(f"{suffix}/{file}"))

    def __find_new_files(self):
        csv = set()
        pkl = set()
        if not os.path.isdir(self.get_data_path("pkl")):
            os.mkdir(self.get_data_path("pkl"))

        for file in os.listdir(self.get_data_path("csv")):
            prefix = file.split(".")[0]
            if file.split(".")[-1] == "csv":
                csv.add(prefix)
        for file in os.listdir(self.get_data_path("pkl")):
            prefix = file.split(".")[0]
            if file.split(".")[-1] == "pkl":
                pkl.add(prefix)

        return csv - pkl

    def __create_pickled_dataframes(self):
        self.__move_files()
        for new_file in self.__find_new_files():
            with open(file=self.get_data_path(f"pkl/{new_file}.pkl"), mode="wb") as pkl_file:
                dump(obj=read_csv(self.get_data_path(f"csv/{new_file}.csv")), file=pkl_file)

    @staticmethod
    def split_string(sub_folder):
        if isinstance(sub_folder, (list, tuple)):
            return sub_folder
        elif "\\" in sub_folder:
            return sub_folder.split("\\")
        else:
            return sub_folder.split("/")

    def get_project_path(self, sub_folder=None):
        if sub_folder is None:
            return project_path

        return os.path.join(project_path, *self.split_string(sub_folder))

    def get_onedrive_path(self, sub_folder=None):
        """
        Returns the directory path to the database.
        """
        if sub_folder is None:
            return self.__onedrive_path

        return os.path.join(self.__onedrive_path, *self.split_string(sub_folder))

    def get_data_path(self, sub_folder=None):
        if sub_folder is None:
            return self.get_onedrive_path("data")

        return self.get_onedrive_path(f"data/{sub_folder}")

    def load_data(self, year=1, hourly=True, meta=False):
        time_base = "hour" if hourly else "minute"
        with open(
                file=self.get_data_path(f"pkl/All-Subsystems-{time_base}-year{year}.pkl"),
                mode="rb") as pkl_file:
            data = load(pkl_file)
        if meta:
            with open(
                    file=self.get_data_path(f"pkl/Metadata-minute-year{year}.pkl"), mode="rb") as pkl_file:
                return data, load(pkl_file)

        return data


Db = __Database()
