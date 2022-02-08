import json
import os
import shutil
import sys
from configparser import ConfigParser
from typing import Dict
import re
from typing import List

class FileIO:
    def __init__(self, filepath: str, create_parent_dir=False):
        self.filepath = filepath
        parent_dir = '/'.join(re.split('\/|\\\\', self.filepath)[:-1])
        if create_parent_dir:
            shutil.rmtree(parent_dir, ignore_errors=True)
            os.makedirs(parent_dir)

        os.makedirs(parent_dir, exist_ok=True)


    def newline_dict_to_file(self, data: Dict):

        with open(self.filepath, 'a') as outfile:
            outfile.write(json.dumps(data))
            outfile.write('\n')

class Directory:
    def __init__(self, dir_path):
        self.dir_path = dir_path

    def collect_files(self, ends_with='') -> List:
        file_list = []
        for dirpath, _, files in os.walk(self.dir_path):
            for filename in files:
                if filename.endswith(ends_with):
                    file_list.append(os.path.join(dirpath, filename))

        return file_list




if __name__ == '__main__':
    pass