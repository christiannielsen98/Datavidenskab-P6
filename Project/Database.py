import configparser
import os
import shutil

from pandas import read_csv, read_pickle

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
            if not os.path.isdir(self.get_data_path(file)):
                if file in os.listdir('/'.join(self.get_save_file_directory(file).split('/')[:-1])):
                    os.remove(self.get_save_file_directory(file))
                    shutil.move(self.get_data_path(file),
                                self.get_save_file_directory(file))

    def __ensure_directory(self, subdirectory):
        """

        :param subdirectory:
        :return:
        """
        if not os.path.isdir(self.get_data_path(subdirectory)):
            os.mkdir(self.get_data_path(subdirectory))

    def __find_new_files(self):
        """
        Iterates through the csv and pkl subdirectories to identify new dataset files.
        :return: A set of new dataset files
        """
        files = {
            "csv": set(),
            "pkl": set()
        }

        for file_type in files.keys():
            self.__ensure_directory(file_type)
            for file in os.listdir(self.get_data_path(file_type)):
                prefix = file.split(".")[0]
                if file.split(".")[-1] == file_type:
                    files[file_type].add(prefix)

        return files["csv"] - files["pkl"]

    def __create_pickled_dataframes(self):
        """
        Creates and pickles dataframe versions of the datasets.
        """
        self.__move_files()
        for new_file in self.__find_new_files():
            read_csv(self.get_save_file_directory(f"{new_file}.csv")).to_pickle(
                self.get_save_file_directory(f"{new_file}.pkl"))

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
        elif "/" in text:
            return text.split("/")
        else:
            return [text]

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

    def get_save_file_directory(self, filename):
        """
        Creates a string containing the directory path for a given filename.
        :param filename: Name of the save file.
        :return: A string containing a directory path.
        """
        file_subdirectory_list = self.split_string(filename)
        subdirectory = [filename.split(".")[-1]] + file_subdirectory_list[:-1]
        self.__ensure_directory("/".join(subdirectory))
        return self.get_data_path("/".join(subdirectory + [file_subdirectory_list[-1]]))

    def load_data(self, year=1, hourly=True, consumption=True, production=False, meta=False):
        """
        Loads in the pickled dataframe objects
        :param year: An integer index to request the dataset of a specific year.
        :param hourly: A boolean, requests an hour version dataframe if True it requests a minutes version.
        :param meta: A boolean,
        :return:
        """
        time_base = "hour" if hourly else "minute"
        try:
            data = []
            if consumption:
                data.append(read_pickle(self.get_save_file_directory(f"All-Subsystems-{time_base}-year{year}.pkl")))
            if meta:
                data.append(
                    read_pickle(self.get_save_file_directory(f"Metadata-{time_base}-year{year}.pkl")))
            if production:
                data.append(read_pickle(self.get_save_file_directory(f"Production_year{year}.pkl")))
        except:
            data = []
            if consumption:
                data.append(read_csv(self.get_save_file_directory(f"All-Subsystems-{time_base}-year{year}.csv")))
            if meta:
                data.append(read_csv(self.get_save_file_directory(f"Metadata-{time_base}-year{year}.csv")))

        if len(data) == 1:
            return data[0]

        return data

    def pickle_dataframe(self, dataframe, filename):
        dataframe.to_pickle(self.get_data_path(filename))
        self.__move_files()


Db = __Database()

if __name__ == "__main__":
    print(Db.load_data(meta=True, consumption=False))
