import os
import pandas as pd
import textwrap

from earthmover.nodes.destination import Destination

class Screenplay(Destination):
    """

    """
    DEFAULT_LINE_SEP = '\n\n'
    DEFAULT_JUSTIFY = None
    DEFAULT_OFFSET = 0
    DEFAULT_WRAP_OFFSET = 0

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.mode = 'screenplay'

        self.allowed_configs.update([
            "text_col", "screenplay_col", "file_col",
            "line_sep", "justify", "offset", "wrap_offset"
        ])

        self.output_dir = None
        self.text_col = None
        self.screenplay_col = None
        self.file_col = None

        self.line_sep = None
        self.justify = None
        self.offset = None
        self.wrap_offset = None


    def compile(self):
        """

        :return:
        """
        super().compile()

        # output_dir: OPTIONAL
        if _output_dir := self.config.get('output_dir'):
            self.error_handler.assert_key_type_is(self.config, 'output_dir', str)

            if not os.path.isdir(_output_dir):
                self.error_handler.throw(
                    "Selected `output_dir` is a file."
                )
                raise

            self.output_dir = _output_dir

        else:
            self.output_dir = os.path.join(
                self.earthmover.state_configs['output_dir'], self.name
            )

        # text_col: REQUIRED
        self.error_handler.assert_key_exists_and_type_is(self.config, 'text_col', str)
        self.text_col = self.config['text_col']

        # screenplay_col: REQUIRED
        self.error_handler.assert_key_exists_and_type_is(self.config, 'screenplay_col', str)
        self.screenplay_col = self.config['screenplay_col']

        # file_col: REQUIRED
        self.error_handler.assert_key_exists_and_type_is(self.config, 'file_col', str)
        self.file_col = self.config['file_col']

        # line_sep: OPTIONAL
        if 'line_sep' in self.config:
            self.error_handler.assert_key_type_is(self.config, 'line_sep', str)
        self.line_sep = self.config.get('line_sep', self.DEFAULT_LINE_SEP)

        # justify: OPTIONAL
        if 'justify' in self.config:
            self.error_handler.assert_key_type_is(self.config, 'justify', int)
        self.justify = self.config.get('justify', self.DEFAULT_JUSTIFY)

        # offset: OPTIONAL
        if 'offset' in self.config:
            self.error_handler.assert_key_type_is(self.config, 'offset', int)
        self.offset = self.config.get('offset', self.DEFAULT_OFFSET)

        # wrap_offset: OPTIONAL
        if 'wrap_offset' in self.config:
            self.error_handler.assert_key_type_is(self.config, 'wrap_offset', int)
        self.wrap_offset = self.config.get('wrap_offset', self.DEFAULT_WRAP_OFFSET)


    def execute(self):
        """

        :return:
        """
        super().execute()

        # Convert the line into a screenplay.
        self.data[self.screenplay_col] = self.data.apply(
            self._format_screenplay, axis=1,
            meta=pd.Series(dtype='str', name=self.text_col)
        )

        # Write out screenplay col, partitioned by self.file_col.
        distinct_files = self.data[self.file_col].unique().compute()
        grouped = self.data.groupby(self.file_col)

        os.makedirs(self.output_dir, exist_ok=True)
        for filename in distinct_files:
            subset = grouped.get_group(filename)
            subset = subset[self.screenplay_col].compute()

            _output_path = os.path.join(self.output_dir, f"{filename}.txt")
            with open(_output_path, 'w') as fp:
                fp.writelines(subset.to_list())


    def _format_screenplay(self, row, **kwargs) -> str:
        """
        https://stackoverflow.com/questions/1166317

        :param row:
        :return:
        """
        row_dict = row.to_dict()

        # Set defaults if the columns are not present in the row.
        # (We must explicitly compare to none, in case 0 is the column's value.)
        text = row_dict[self.text_col]
        text = text.replace(r'\n', '\n')  # Replace '\n' strings with newline characters.

        line_sep = row_dict.get('line_sep', self.line_sep)
        justify = row_dict.get('justify', self.justify)
        offset = row_dict.get('offset', self.offset)
        wrap_offset = row_dict.get('wrap_offset', self.wrap_offset)

        screenplay_lines = []
        for idx, line in enumerate(text.split('\n')):
            if idx == 0:
                _wrapped = textwrap.wrap(
                    line, justify,
                    break_long_words=False, replace_whitespace=False,
                    initial_indent=(' ' * offset),
                    subsequent_indent=(' ' * wrap_offset)
                )

            else:
                _wrapped = textwrap.wrap(
                    line, justify,
                    break_long_words=False, replace_whitespace=False,
                    initial_indent=(' ' * wrap_offset),
                    subsequent_indent=(' ' * wrap_offset)
                )

            screenplay_lines.extend(_wrapped)

        return '\n'.join(screenplay_lines) + line_sep
