import dask
import pandasql as psql



from earthmover.nodes.operation import Operation


class SqlSelectOperation(Operation):
    """

    """
    CHUNKSIZE = 1024 * 1024 * 100  # 100 MB

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

        if self.source is not None:
            self.source_list = [self.source]  # Force `source_list` be used for simplicity.

        self.allowed_configs.update(["sql, alias_mapping"])

        self.sql = None
        self.alias_mapping = None


    def compile(self):
        """

        :return:
        """
        super().compile()

        # query: REQUIRED
        self.error_handler.assert_key_exists_and_type_is(self.config, 'sql', str)
        self.sql = self.config['sql']

        # source_mapping: REQUIRED
        self.error_handler.assert_key_exists_and_type_is(self.config, 'alias_mapping', dict)
        self.alias_mapping = self.config['alias_mapping']

        for name, alias in self.alias_mapping.items():
            if name not in self.sources:
                self.error_handler.throw(
                    f"Source alias `{name}` not declared in `sources`"
                )


    def execute(self):
        """

        :return:
        """
        super().execute()

        source_mapping = dict(zip(self.source_list, self.source_data_list))

        for name, _data in source_mapping.items():
            alias = self.alias_mapping[name]
            exec(f"{alias} = _data")

        self.data = dask.from_pandas(
            psql.sqldf(self.sql),
            chunksize=self.CHUNKSIZE
        )

        return self.data
