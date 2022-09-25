import dask.dataframe as dd
import os
import pandas as pd

from typing import Iterable

from earthmover.nodes.source import Source


class RenpyVisualNovel(Source):
    """

    """
    CHUNKSIZE = 1024 * 1024 * 100  # 100 MB

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.mode = 'renpy_visual_novel'

        self.allowed_configs.update(["game_dir", "script_files"])

        self.game_dir = None
        self.script_files = None


    def compile(self):
        """

        :return:
        """
        super().compile()

        # game_dir: REQUIRED
        self.error_handler.assert_key_exists_and_type_is(self.config, 'game_dir', str)
        self.game_dir = self.config['base_dir']

        if not os.path.exists(self.game_dir) or not os.path.isdir(self.game_dir):
            self.error_handler.throw(
                f"Specified visual novel directory does not exist or is not unzipped: `{self.game_dir}`"
            )

        # script_files: REQUIRED
        self.error_handler.assert_key_exists_and_type_is(self.config, 'script_files', list)
        self.script_files = []

        _script_filepaths = map(lambda x: os.path.join(self.base_dir, x), self.config['script_files'])
        for _file in self.iterate_files(_script_filepaths):
            self.script_files.append(_file)


    def execute(self):
        """

        :return:
        """
        super().execute()

        dataframes = []
        for file in self.script_files:
            with open(file, 'r') as fp:
                raw_lines = fp.readlines()

            rows = []
            for line_idx, line in enumerate(raw_lines):
                rows.append( [file, line_idx, line] )

            _dataframe = dd.from_pandas(
                pd.DataFrame(rows, columns=['file', 'line_idx', 'line']),
                chunksize=self.CHUNKSIZE
            )
            dataframes.append(_dataframe)

        self.data = dd.concat(dataframes)
        return self.data


    def iterate_files(self, paths: Iterable[str]):
        """

        :param paths:
        :return:
        """
        for path in paths:
            if os.path.isfile(path):
                yield path
            else:
                paths_to_recurse = map(lambda x: os.path.join(path, x), os.listdir(path))
                self.iterate_files(paths_to_recurse)