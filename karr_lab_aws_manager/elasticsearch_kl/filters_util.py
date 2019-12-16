import json
from pathlib import Path, PurePath


class FiltersUtil:

    def __init__(self):
        self.cwd = Path.cwd()

    def read_filter(self, _dir):
        """Read in analzyer json file.
        
        Args:
            _dir (:obj:`str`): directory of the json file.

        Return:
            (:obj:`dict`)
        """
        full_dir = str(Path(_dir).expanduser())
        with open(full_dir, 'r') as f:
            return json.load(f)        