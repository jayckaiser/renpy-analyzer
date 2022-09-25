import collections
import numpy as np
import matplotlib.pyplot as plt
import pandas as pd
import scipy
import seaborn as sns

from earthmover.nodes.destination import Destination

from src.common.custom.util import io_util


class Relplot(Destination):
    """

    """
    DEFAULT_FIGSIZE = (16, 10)
    DEFAULT_STYLE = "darkgrid"

    def __init__(self, *args, **kwargs):
        """

        :param args:
        :param query:
        :param kwargs:
        """
        super().__init__(*args, **kwargs)
        self.mode = 'relplot'

        self.allowed_configs.update([
            "file", "relplot",  # Mandatory args
            "remove_outliers",  # Normalize the dataset
            "figsize", "style", "title", "axhline",  # Main plot configs
        ])

        self.file = None
        self.relplot_kwargs = None

        self.remove_outliers = None

        self.figsize = None
        self.style = None
        self.title = None
        self.axhline = None


    def compile(self):
        """

        :return:
        """

        super().compile()

        # file: REQUIRED
        self.error_handler.assert_key_exists_and_type_is(self.config, 'file', str)
        self.file = self.config['file']

        # relplot: REQUIRED
        self.error_handler.assert_key_exists_and_type_is(self.config, 'relplot', dict)
        self.relplot_kwargs = self.config['relplot']

        self.error_handler.assert_key_exists_and_type_is(self.relplot_kwargs, 'x', str)
        self.error_handler.assert_key_exists_and_type_is(self.relplot_kwargs, 'y', str)

        # remove_outliers: OPTIONAL
        if 'remove_outliers' in self.config:
            self.error_handler.assert_key_type_is(self.config, 'remove_outliers', bool)
        self.remove_outliers = self.config.get('remove_outliers', False)

        # figsize: OPTIONAL
        if 'figsize' in self.config:
            self.error_handler.assert_key_type_is(self.config, 'figsize', collections.abc.Iterable)
            if not len(self.config['figsize']) == 2:
                self.error_handler.throw(
                    f"Figsize must be of exactly length 2 ([width, height] inches)"
                )
                raise
        self.figsize = self.config.get('figsize', self.DEFAULT_FIGSIZE)

        # style: OPTIONAL
        if 'style' in self.config:
            self.error_handler.assert_key_type_is(self.config, 'style', str)
        self.style = self.config.get('style', self.DEFAULT_STYLE)

        # title: OPTIONAL
        self.title = self.config.get('title')

        # axhline: OPTIONAL
        if 'axhline' in self.config:
            self.error_handler.assert_key_type_is(self.config, 'axhline', float)
        self.axhline = self.config.get('axhline')


    def execute(self):
        """

        :return:
        """
        super().execute()  # Define self.data

        ### Normalize the data if specified.
        if self.remove_outliers:
            _y = self.relplot_kwargs['y']
            self.data[_y] = self.remove_outliers(self.data[_y])

        ### Build the relplot.
        # Set the style if specified (defaults to 'darkgrid' such that Cassius can be seen).
        sns.set_theme(style=self.style)

        # Establish and build the figure.
        plt.figure()
        sns.relplot(data=self.data, **self.relplot_kwargs)

        # Add a title if specified.
        plt.title(self.title)

        # Add a custom horizontal line if specified.
        if self.axhline:
            plt.axhline(self.axhline, linestyle='--', color='black', alpha=0.5)

        # Set the output size and return.
        fig = plt.gcf()
        fig.set_size_inches(*self.figsize)

        ### Save the figure to disk and reset the environment.
        io_util.prepare_directories(self.file)
        fig.savefig(self.file, bbox_inches='tight')

        self.reset_env()  # Pyplot is memory-hungry
        self.logger.debug(
            f"@ Relplot {self.name} written and pyplot reset."
        )


    @staticmethod
    def remove_outliers(series: pd.Series, sigma: int = 3):
        """

        """
        return [  # TODO: What is the correct syntax for this?
            (np.abs(scipy.stats.zscore(series)) < sigma)
        ]


    @staticmethod
    def reset_env():
        """

        """
        plt.close('all')
        # gc.collect()