import configparser
import os

from project_root import project_path


class DatabaseLocator:
    def __init__(self):
        """
        This constructor method creates container attributes for directory paths.
        config_path is hard coded.
        database_path is loaded from the Config file.
        """
        self.__config_path = os.path.join(project_path, 'Config')
        self.__onedrive_path = self.__config_load()
        self.__model_path = os.path.join(project_path, 'Project', 'Models')

    def __config_load(self):
        """
        Loads database path.
        """
        config = configparser.ConfigParser()

        config.read(os.path.join(self.__config_path, os.listdir(self.__config_path)[0]))

        return config['onedrive']['path']

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


if __name__ == '__main__':
    Locator = DatabaseLocator()
    print(Locator.get_onedrive_path(sub_folder=("data", "file")))
