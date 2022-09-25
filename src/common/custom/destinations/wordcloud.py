import numpy as np

from PIL import Image
from wordcloud import WordCloud, ImageColorGenerator

from earthmover.nodes.destination import Destination

from src.common.custom.util import io_util


class Wordcloud(Destination):
    """

    """
    DEFAULT_WORDCLOUD_KWARGS = {

    }

    def __init__(self, *args, **kwargs):
        """

        :param args:
        :param query:
        :param kwargs:
        """
        super().__init__(*args, **kwargs)
        self.mode = 'wordcloud'

        self.allowed_configs.update(["file", "image", "wordcloud", "resize"])

        self.file = None
        self.image = None
        self.wordcloud_kwargs = None
        self.resize = None


    def compile(self):
        """

        :return:
        """

        super().compile()

        # file: REQUIRED
        self.error_handler.assert_key_exists_and_type_is(self.config, 'file', str)
        self.file = self.config['file']

        # image: REQUIRED
        self.error_handler.assert_key_exists_and_type_is(self.config, 'image', str)
        self.image = self.config['image']

        # wordcloud: REQUIRED
        if 'wordcloud' in self.config:
            self.error_handler.assert_key_type_is(self.config, 'wordcloud', dict)
        self.wordcloud_kwargs = self.config.get('wordcloud', self.DEFAULT_WORDCLOUD_KWARGS)

        # resize: OPTIONAL
        if 'resize' in self.config:
            self.error_handler.assert_key_type_is(self.config, 'resize', float)
        self.resize = self.config.get('resize', 1)  # Default to no-resizing.


    def execute(self):
        """

        :return:
        """
        super().execute()  # Define self.data

        image_mask = self.get_image_mask(self.image, resize=self.resize)
        height, width, _ = image_mask.shape

        # Create the wordcloud shaped by the image.
        wc = WordCloud(
            mask=image_mask,
            width=width, height=height,
            **self.wordcloud_kwargs
        ).generate_from_frequencies(self.data)

        # Recolor the wordcloud to match the image.
        image_colors = ImageColorGenerator(image_mask)
        wc = wc.recolor(color_func=image_colors)

        # Save to file and reset environment.
        io_util.prepare_directories(self.file)
        wc.to_file(self.file)

        self.reset_env()  # Wordclouds are memory-hungry
        self.logger.debug(
            f"@ Wordcloud {self.name} written and environment reset."
        )


    @staticmethod
    def get_image_mask(image_path: str, resize: float = 1):
        """
        Convert an image on disk to a numpy image mask.
        """
        image = Image.open(image_path)
        image_mask = np.array(image)

        # Apply resizing if specified.
        if resize != 1:
            height, width, _ = image_mask.shape

            # Note: PILLOW does not keep transparency when rescaling.
            # TODO: Is this solvable?
            image = image.resize((height * resize, width * resize))
            image_mask = np.array(image)

        return image_mask


    @staticmethod
    def reset_env():
        """

        """
        # gc.collect()
        pass