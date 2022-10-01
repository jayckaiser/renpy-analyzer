# import dask.dataframe as dd
# import pandasql as psql

from earthmover.nodes.operation import Operation


# class SqlSelectOperation(Operation):
#     """
#
#     """
#     def __init__(self, *args, **kwargs):
#         super().__init__(*args, **kwargs)
#
#         self.allowed_configs.update(["sql", "aliases"])
#
#         self.sql = None
#         self.alias_list = None
#
#
#     def compile(self):
#         """
#
#         :return:
#         """
#         super().compile()
#
#         # query: REQUIRED
#         self.error_handler.assert_key_exists_and_type_is(self.config, 'sql', str)
#         self.sql = self.config['sql']
#
#         # alias_list: REQUIRED
#         self.error_handler.assert_key_exists_and_type_is(self.config, 'aliases', list)
#
#         _aliases = self.config['aliases']
#
#         if len(_aliases) != len(self.source_list):
#             self.error_handler.throw(
#                 f"Number of provided sources and aliases are not equal"
#             )
#             raise
#
#         if len(set(_aliases)) != len(_aliases):
#             self.error_handler.throw(
#                 "One or more provided aliases are identical"
#             )
#             raise
#
#         self.alias_list = _aliases
#
#
#
#     def execute(self):
#         """
#
#         :return:
#         """
#         super().execute()
#
#         alias_mapping = dict(zip(self.alias_list, self.source_data_list))
#
#         for _name, _data in alias_mapping.items():
#             exec(f"{_name} = _data.compute()"
#                  f""
#                  )
#
#         self.data = psql.sqldf(self.sql)
#         self.force_dask()
#
#         return self.data




import duckdb

from earthmover.nodes.operation import Operation

class SqlSelectOperation(Operation):
    """

    """
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

        self.allowed_configs.update(["sql", "aliases", "extensions"])

        self.sql = None
        self.alias_list = None
        self.extensions = None  # TODO


    def compile(self):
        """

        :return:
        """
        super().compile()

        # query: REQUIRED
        self.error_handler.assert_key_exists_and_type_is(self.config, 'sql', str)
        self.sql = self.config['sql']

        # alias_list: REQUIRED
        self.error_handler.assert_key_exists_and_type_is(self.config, 'aliases', list)

        self.alias_list = self.config['aliases']

        if len(self.alias_list) != len(self.source_list):
            self.error_handler.throw(
                f"Number of provided sources and aliases are not equal"
            )
            raise

        if len(set(self.alias_list)) != len(self.alias_list):
            self.error_handler.throw(
                "One or more provided aliases are identical"
            )
            raise

        # extensions: OPTIONAL
        if 'extensions' in self.config:
            self.error_handler.assert_key_type_is(self.config, 'extensions', list)
        self.extensions = self.config.get('extensions', [])


    def execute(self):
        """

        :return:
        """
        super().execute()

        #
        conn = self._connect()

        # Register the datasets to their aliases.
        alias_mapping = dict(zip(self.alias_list, self.source_data_list))

        for name, data in alias_mapping.items():
            data = data.compute()
            conn.register(name, data)

        self.data = conn.execute(self.sql).df()
        self.force_dask()

        conn.close()
        return self.data


    def _connect(self):
        """

        :return:
        """

        conn = duckdb.connect(database=':memory:')

        ## TODO: Why the heck does this not work?
        # How do you load extensions using the Python API?
        for ext in self.extensions:
            conn.install_extension(ext)
            conn.load_extension(ext)

        return conn