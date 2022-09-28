
from earthmover.nodes.destination import Destination
from earthmover.nodes.operation import Operation
from earthmover.nodes.source import Source


class SourceRA(Source):
    """

    """

    @classmethod
    def select_class(cls, config: dict):
        """

        :param config:
        :param earthmover:
        """
        from renpy_analyzer.sources import visual_novel

        node_mapping = {
            'renpy_visual_novel': visual_novel.RenpyVisualNovel,
        }

        node_type = config.get('mode')

        return node_mapping.get(node_type)



class OperationRA(Operation):
    """

    """

    @classmethod
    def select_class(cls, config: dict):
        """

        :param config:
        :param earthmover:
        """
        from renpy_analyzer.operations import sql as sql_operations
        from renpy_analyzer.operations import nlp as nlp_operations
        from renpy_analyzer.operations import regex as regex_operations
        from renpy_analyzer.operations import tfidf as tfidf_operations

        node_mapping = {
            'sql_select': sql_operations.SqlSelectOperation,
            'apply_spacy': nlp_operations.ApplySpacyOperation,
            'regexp_replace': regex_operations.RegexReplaceOperation,
            'regexp_match_groups': regex_operations.RegexMatchGroupOperation,
            'tfidf_frequencies': tfidf_operations.TfIdfOperation,
        }

        node_type = config.get('operation')

        return node_mapping.get(node_type)



class DestinationRA(Destination):
    """

    """

    @classmethod
    def select_class(cls, config: dict):
        """

        :param config:
        :param earthmover:
        """
        from renpy_analyzer.destinations import relplot as relplot_dests
        from renpy_analyzer.destinations import screenplay as screenplay_dests
        from renpy_analyzer.destinations import wordcloud as wordcloud_dests

        node_mapping = {
            'relplot': relplot_dests.Relplot,
            'screenplay': screenplay_dests.Screenplay,
            'wordcloud': wordcloud_dests.Wordcloud,
        }

        node_type = config.get('mode')

        return node_mapping.get(node_type)