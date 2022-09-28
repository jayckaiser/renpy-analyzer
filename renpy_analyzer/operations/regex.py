import pandas as pd
import re

from earthmover.nodes.operation import Operation


class RegexReplaceOperation(Operation):
    """

    """
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

        self.allowed_configs.update(['column', 'replaces', 'trim'])

        self.column = None
        self.replaces = None
        self.trim = None


    def compile(self):
        """

        :return:
        """
        super().compile()

        # column: REQUIRED
        self.error_handler.assert_key_exists_and_type_is(self.config, 'column', str)
        self.column = self.config['column']

        # replaces: REQUIRED
        self.error_handler.assert_key_exists_and_type_is(self.config, 'replaces', dict)
        self.replaces = self.config['replaces']

        # trim: OPTIONAL
        if 'trim' in self.config:
            self.error_handler.assert_key_type_is(self.config, 'trim', bool)
        self.trim = self.config.get('trim', False)


    def execute(self):
        """

        :return:
        """
        super().execute()

        if self.trim:
            self.data[self.column] = self.data[self.column].str.strip()

        for expr, replacement in self.replaces.items():
            self.data[self.column] = self.data[self.column].replace(expr, replacement, regex=True)

        return self.data



class RegexMatchGroupOperation(Operation):
    """

    """
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

        self.allowed_configs.update(["column", "classification_col", "group_cols", "regexes"])

        self.column = None
        self.classification_col = None
        self.group_cols = None
        self.regexes = None


    def compile(self):
        """

        :return:
        """
        super().compile()

        # column: REQUIRED
        self.error_handler.assert_key_exists_and_type_is(self.config, 'column', str)
        self.column = self.config['column']

        # classification_col: REQUIRED
        self.error_handler.assert_key_exists_and_type_is(self.config, 'classification_col', str)
        self.classification_col = self.config['classification_col']

        # group_cols: OPTIONAL
        if 'group_cols' in self.config:
            self.error_handler.assert_key_type_is(self.config, 'group_cols', list)
            self.group_cols = self.config['group_cols']
        else:
            self.group_cols = []

        # regexes: REQUIRED
        self.error_handler.assert_key_exists_and_type_is(self.config, 'regexes', dict)
        _regexes = self.config['regexes']

        # for regex_config in self.config['regexes']:
        #     self.error_handler.assert_key_exists_and_type_is(regex_config, 'classification', str)
        #     self.error_handler.assert_key_exists_and_type_is(regex_config, 'regex', str)
        #     self.error_handler.assert_key_exists_and_type_is(regex_config, 'groups', list)
        #
        #     if any(map(lambda x: x not in self.group_cols, regex_config['groups'])):
        #         self.error_handler.throw(
        #             "One or more specified match groups are not found in the specified group columns."
        #         )
        #         raise

        self.regexes = {}
        for classification, regex in _regexes.items():
            if classification == '__line__':
                continue
            self.regexes[classification] = re.compile(regex)


    def verify(self):
        """

        :return:
        """
        super().verify()

        for col in [self.classification_col] + self.group_cols:
            if col not in self.data.columns:
                self.error_handler.throw(
                    f"column {col} is specified, but not defined with a default value"
                )
                raise


    def execute(self):
        """

        :return:
        """
        super().execute()

        new_cols = [self.classification_col] + self.group_cols

        self.data[new_cols] = self.data.apply(
            self.classify_match, axis=1,
            meta=pd.DataFrame(columns=list(range(len(new_cols))), dtype='str'),
            result_type='expand'
        )

        return self.data


    def classify_match(self, row):
        """

        :param row:
        :return:
        """
        for classification, regex in self.regexes.items():
            matched_regex = regex.match(row[self.column])

            if matched_regex:
                output = [classification,]

                for col in self.group_cols:
                    try:
                        _match_group = matched_regex.group(col)
                        output.append(_match_group)
                    except IndexError:
                        output.append(row[col])

                return output

        # Otherwise, default to current row values.
        else:
            return (
                row[self.classification_col],
                *row[self.group_cols]
            )
