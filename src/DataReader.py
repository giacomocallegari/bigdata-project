#!/usr/bin/python
import sys
import csv
import os
from TaxiType import TaxiType


class DataReader:

    is_a_folder = False
    type = TaxiType.ALL
    yellow_set = list()
    green_set = list()
    fhv_set = list()
    yellow_template_header = []
    fhv_template_header = []
    green_template_header = []

    # Checks if the file is of type yellow.
    def __is_yellow(self, header):
        if len(header) == len(self.yellow_template_header):
            for i in range(len(header)):
                if header[i] != self.yellow_template_header[i]:
                    return False
            return True

    # Checks if the file is of type green.
    def __is_green(self, header):
        if len(header) == len(self.green_template_header):
            for i in range(len(header)):
                if header[i] != self.green_template_header[i]:
                    return False
            return True

    # Checks if the file is of type fhv.
    def __is_fhv(self, header):
        if len(header) == len(self.fhv_template_header):
            for i in range(len(header)):
                if header[i] != self.fhv_template_header[i]:
                    return False
            return True

    # Sets the type of the files.
    def __set_type_param(self, type_param):
        if type_param == "--yellow" or type_param == "-y":
            self.type = TaxiType.YELLOW
        elif type_param == "--green" or type_param == "-g":
            self.type = TaxiType.GREEN
        elif type_param == "--FHV" or type_param == "-f":
            self.type = TaxiType.FHV
        elif type_param == "--all" or type_param == "-a":
            self.type = TaxiType.ALL
        else:
            print("Invalid Type Parameter")
            sys.exit()

    # Checks if the path is valid and if it contains csv files.
    def __filter_csv(self, folder_path):
        try:
            exist_valid_csv = False
            for file in os.listdir(folder_path):
                try:
                    with open(folder_path + file) as current_file:
                        reader = csv.reader(current_file)
                        current_header = next(reader)
                        if self.__is_yellow(current_header):
                            exist_valid_csv = True
                            self.yellow_set.append(folder_path + file)
                            exist_valid_csv = True
                        elif self.__is_fhv(current_header):
                            self.fhv_set.append(folder_path + file)
                            exist_valid_csv = True
                        elif self.__is_green(current_header):
                            exist_valid_csv = True
                            self.green_set.append(folder_path + file)
                except Exception:
                    pass
            if not exist_valid_csv:
                print("The selected folder does not contain any csv file")
                sys.exit()
        except FileNotFoundError:
            print("No such file or directory for the path specified")
            sys.exit()

    # Initializes the header templates for the files.
    def __init__(self):
        with open(os.path.join(os.path.dirname(__file__), "csv-template/yellow.csv")) as yellow_template:
            reader = csv.reader(yellow_template)
            self.yellow_template_header = next(reader)
        with open(os.path.join(os.path.dirname(__file__), "csv-template/fhv.csv")) as fhv_template:
            reader = csv.reader(fhv_template)
            self.fhv_template_header = next(reader)
        with open(os.path.join(os.path.dirname(__file__), "csv-template/green.csv")) as green_template:
            reader = csv.reader(green_template)
            self.green_template_header = next(reader)

    # Reads the input parameters.
    def read_input_params(self):
        try:
            if sys.argv[1] == "-r" and (len(sys.argv) == 3 or len(sys.argv) == 4):  # Read folder
                self.is_a_folder = True
                try:
                    folder_path = sys.argv[2]
                    if not folder_path.endswith("/"):
                        folder_path += "/"
                    self.__filter_csv(folder_path)
                    if len(sys.argv) == 4:
                        self.__set_type_param(sys.argv[3])
                except IndexError:
                    print("No folder path specified")
                    sys.exit()
            elif len(sys.argv) == 2 or len(sys.argv) == 3:  # Read file
                self.is_a_folder = False
                input_path = sys.argv[1]
                try:
                    with open(input_path) as current_file:
                        reader = csv.reader(current_file)
                        header = next(reader)
                        if self.__is_yellow(header):
                            self.yellow_set.append(input_path)
                        elif self.__is_fhv(header):
                            self.fhv_set.append(input_path)
                        elif self.__is_green(header):
                            self.green_set.append(input_path)
                        else:
                            print("Invalid file selected")
                            sys.exit()
                        if len(sys.argv) == 3:
                            self.__set_type_param(sys.argv[2])
                except Exception:
                    print("Invalid file selected")
                    sys.exit()
            else:
                print("Invalid parameters")
                sys.exit()

        except IndexError:
            print("You have to specify a folder/file directory")
            sys.exit()
