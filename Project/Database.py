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
        """
        Moves files from the data directory to filetype specific subdirectories.
        """
        for file in os.listdir(self.get_data_path()):
            suffix = file.split(".")[-1]
            if not os.path.isdir(self.get_data_path(file)):
                if not os.path.isdir(self.get_data_path(suffix)):
                    os.mkdir(self.get_data_path(suffix))
                shutil.move(self.get_data_path(file),
                            self.get_data_path(f"{suffix}/{file}"))

    def __find_new_files(self):
        """
        Iterates through the csv and pkl subdirectories to identify new dataset files.
        :return: A set of new dataset files
        """
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
        """
        Creates and pickles dataframe versions of the datasets.
        """
        self.__move_files()
        for new_file in self.__find_new_files():
            with open(file=self.get_data_path(f"pkl/{new_file}.pkl"), mode="wb") as pkl_file:
                dump(obj=read_csv(self.get_data_path(f"csv/{new_file}.csv")), file=pkl_file)

    @staticmethod
    def split_string(text):
        """
        Converts a string to a list of strings.
        :param text: String to convert to list.
        :return: A list of strings.
        """
        if isinstance(text, (list, tuple)):
            return text
        elif "\\" in text:
            return text.split("\\")
        else:
            return text.split("/")

    def get_project_path(self, sub_folder=None):
        """
        Creates a string containing a project directory path.
        :param sub_folder: A list of- or "/" / "\\" divided string of sub-path(s).
        :return: A string containing to a project directory path.
        """
        if sub_folder is None:
            return project_path

        return os.path.join(project_path, *self.split_string(sub_folder))

    def get_onedrive_path(self, sub_folder=None):
        """
        Creates a string containing a OneDrive directory path.
        :param sub_folder: A list of- or "/" / "\\" divided string of sub-path(s).
        :return: A string containing to a OneDrive directory path.
        """
        if sub_folder is None:
            return self.__onedrive_path

        return os.path.join(self.__onedrive_path, *self.split_string(sub_folder))

    def get_data_path(self, sub_folder=None):
        """
        Creates a string containing a dataset directory path.
        :param sub_folder: A list of- or "/" / "\\" divided string of sub-path(s).
        :return: A string containing to a dataset directory path.
        """
        if sub_folder is None:
            return self.get_onedrive_path("data")

        return self.get_onedrive_path(f"data/{sub_folder}")

    def save_file_directory(self, filename):
        """
        Creates a string containing the directory path for a given filename.
        :param filename: Name of the save file.
        :return: A string containing a directory path.
        """
        subdirectory = filename.split(".")[-1]
        return self.get_data_path(f"{subdirectory}/{filename}")

    def load_data(self, year=1, hourly=True, meta=False):
        """
        Loads in the pickled dataframe objects
        :param year: An integer index to request the dataset of a specific year.
        :param hourly: A boolean, requests an hour version dataframe if True it requests a minutes version.
        :param meta: A boolean,
        :return:
        """
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
