import re

from earthmover.nodes.operation import Operation



class RegexReplaceOperation(Operation):
    """

    """
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

        self.allowed_configs.update(['column', 'replaces'])

        self.column = None
        self.replaces = None


    def compile(self):
        """

        :return:
        """
        super().compile()

        self.error_handler.assert_key_exists_and_type_is(self.config, 'column', str)
        self.column = self.config['column']

        self.error_handler.assert_key_exists_and_type_is(self.config, 'replaces', dict)
        self.replaces = self.config['replaces']


    def execute(self):
        """

        :return:
        """
        super().execute()

        for expr, replacement in self.replaces.items():
            self.data[self.column] = self.data[self.column].replace(expr, replacement, regex=True)

        return self.data

#
#
# class RegexMatchGroupOperation(Operation):
#     """
#
#     """
#     def __init__(self, *args, **kwargs):
#         super().__init__(*args, **kwargs)
#
#         self.allowed_configs.update(["column", "classification_col", "regexes"])
#
#         self.column = None
#         self.classification_col = None
#         self.regexes = None
#
#
#     def compile(self):
#         """
#
#         :return:
#         """
#         super().compile()
#
#         # column: REQUIRED
#         self.error_handler.assert_key_exists_and_type_is(self.config, 'column', str)
#         self.column = self.config['column']
#
#         # classification_col: REQUIRED
#         self.error_handler.assert_key_exists_and_type_is(self.config, 'classification_col', str)
#         self.classification_col = self.config['classification_col']
#
#         # regexes: REQUIRED
#         self.error_handler.assert_key_exists_and_type_is(self.config, 'regexes', list)
#         self.regexes = []
#
#         for regex_config in self.config['regexes']:
#             self.error_handler.assert_key_exists_and_type_is(regex_config, 'classification', str)
#             self.error_handler.assert_key_exists_and_type_is(regex_config, 'regex', str)
#             self.error_handler.assert_key_exists_and_type_is(regex_config, 'groups', list)
#
#             regex_config['regex'] = re.compile(regex_config['regex'])
#             self.regexes.append(regex_config)
#
#
#     def execute(self):
#         """
#
#         :return:
#         """
#         super().execute()
#
#
#
#
#     def classify_match(self, text):
#         """
#
#         :param row:
#         :return:
#         """
#         for regex_config in self.regexes:
#             _classification = self.regexes['classification']
#             _regex = self.regexes['regex']
#             _groups = self.regexes['groups']
#
#             if matched_regex := _regex.match(text):
#                 return _classification, {
#                     x: matched_regex.group(x)
#                     for x in _groups
#                 }
#             # TODO: Decide whether to finish this.
#
#         else:
#             return False, {}