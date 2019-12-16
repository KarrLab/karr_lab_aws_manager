import json
from pathlib import Path, PurePath


class AnalyzersUtil:

    def __init__(self):
        self.cwd = Path.cwd()

    def read_analyzer(self, rel_dir):
        """Read in analzyer json file.
        
        Args:
            rel_dir (:obj:`str`): relative directory of the json file.

        Return:
            (:obj:`dict`)
        """
        full_dir = PurePath(str(self.cwd)).joinpath(rel_dir)
        with open(full_dir, 'r') as f:
            return json.load(f)        