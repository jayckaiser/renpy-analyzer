
from earthmover.nodes.destination import Destination
from earthmover.nodes.operation import Operation
from earthmover.nodes.source import Source

from typing import TYPE_CHECKING
if TYPE_CHECKING:
    from earthmover.earthmover import Earthmover


class SourceRA(Source):
    """

    """
    def __new__(cls, config: dict, *, earthmover: 'Earthmover'):
        """

        :param config:
        :param earthmover:
        """
        from src.common.custom.sources import visual_novel

        node_mapping = {
            'visual_novel': visual_novel.RenpyVisualNovel,
        }

        node_type = config.get('mode')
        node_class = node_mapping.get(node_type)

        if node_class is None:
            super().__new__(config, earthmover=earthmover)

        return node_class.__new__(node_class)



class OperationRA(Operation):
    """

    """
    def __new__(cls, config: dict, *, earthmover: 'Earthmover'):
        """

        :param config:
        :param earthmover:
        """
        print("Made it to the thing!")
        from src.common.custom.operations import sql as sql_operations

        node_mapping = {
            'sql_select': sql_operations.SqlSelectOperation,
        }

        node_type = config.get('operation')
        node_class = node_mapping.get(node_type)

        if node_class is None:
            super().__new__(config, earthmover=earthmover)

        return node_class.__new__(node_class)



class DestinationRA(Destination):
    """

    """
    def __new__(cls, config: dict, *, earthmover: 'Earthmover'):
        """

        :param config:
        :param earthmover:
        """
        from src.common.custom.destinations import relplot as relplot_dests
        from src.common.custom.destinations import screenplay as screenplay_dests
        from src.common.custom.destinations import wordcloud as wordcloud_dests

        node_mapping = {
            'relplot': relplot_dests.Relplot,
            'screenplay': screenplay_dests.Screenplay,
            'wordcloud': wordcloud_dests.Wordcloud,
        }

        node_type = config.get('mode')
        node_class = node_mapping.get(node_type)

        if node_class is None:
            super().__new__(config, earthmover=earthmover)

        return node_class.__new__(node_class)