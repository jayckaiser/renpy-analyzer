import dask.dataframe as dd
import pandasql as psql

from earthmover.nodes.operation import Operation


class SqlSelectOperation(Operation):
    """

    """
    CHUNKSIZE = 1024 * 1024 * 100  # 100 MB

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

        self.allowed_configs.update(["sql", "aliases"])

        self.sql = None
        self.alias_list = None


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

        _aliases = self.config['aliases']

        if len(_aliases) != len(self.source_list):
            self.error_handler.throw(
                f"Number of provided sources and aliases are not equal"
            )
            raise

        if len(set(_aliases)) != len(_aliases):
            self.error_handler.throw(
                "One or more provided aliases are identical"
            )
            raise

        self.alias_list = _aliases



    def execute(self):
        """

        :return:
        """
        super().execute()

        alias_mapping = dict(zip(self.alias_list, self.source_data_list))

        for _name, _data in alias_mapping.items():
            exec(f"{_name} = _data")

        self.data = dd.from_pandas(
            psql.sqldf(self.sql),
            chunksize=self.CHUNKSIZE
        )

        return self.data
