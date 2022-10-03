

from earthmover.nodes.destination import Destination

class Screenplay(Destination):
    """

    """
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.mode = 'screenplay'

        self.allowed_configs.update(["file_col", "screenplay_col", "justify", "line_sep"])

        self.file_col = None
        self.screenplay_col = None
        self.justify = None
        self.line_sep = None


    def compile(self):
        """

        :return:
        """
        super().compile()

        # file_col: REQUIRED
        self.error_handler.assert_key_exists_and_type_is(self.config, 'file_col', str)
        self.file_col = self.config['file_col']

        # screenplay_col: REQUIRED
        self.error_handler.assert_key_exists_and_type_is(self.config, 'screenplay_col', str)
        self.screenplay_col = self.config['screenplay_col']

        # justify: OPTIONAL
        if 'justify' in self.config:
            self.error_handler.assert_key_type_is(self.config, 'justify', int)
        self.justify = self.config.get('justify')

        # line_sep: OPTIONAL
        if 'line_sep' in self.config:
            self.error_handler.assert_key_type_is(self.config, 'line_sep', str)
        self.line_sep = self.config.get('line_sep', '\n\n')



    def execute(self):
        """

        :return:
        """
        super().execute()
        pass

