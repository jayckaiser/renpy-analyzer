import os

from earthmover.nodes.source import Source


class RenpyVisualNovel(Source):
    """

    """
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.mode = 'visual_novel'

        self.allowed_configs.update(["base_dir"])

        self.base_dir = None


    def compile(self):
        """

        :return:
        """
        super().compile()

        self.error_handler.assert_key_exists_and_type_is(self.config, 'base_dir', str)
        self.base_dir = self.config['base_dir']

        if not os.path.exists(self.base_dir) or not os.path.isdir(self.base_dir):
            self.error_handler.throw(
                f"Specified visual novel directory does not exist or is not unzipped: `{self.base_dir}`"
            )

