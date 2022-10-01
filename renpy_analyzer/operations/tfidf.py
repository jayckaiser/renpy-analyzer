import dask
import dask.dataframe as dd
import numpy as np
import pandas as pd
import sklearn

from sklearn.feature_extraction.text import CountVectorizer, TfidfVectorizer

from earthmover.nodes.operation import Operation


class TfIdfOperation(Operation):
    """

    """
    DEFAULT_TFIDFVECTORIZER_KWARGS = {

    }

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

        self.allowed_configs.update(['document_col', 'frequency_col', 'tfidfvectorizer'])

        self.document_col = None
        self.frequency_col = None
        self.tfidfvectorizer_kwargs = None


    def compile(self):
        """

        :return:
        """
        super().compile()

        # document_col: REQUIRED
        self.error_handler.assert_key_exists_and_type_is(self.config, "document_col", str)
        self.document_col = self.config['document_col']

        # frequency_col: REQUIRED
        self.error_handler.assert_key_exists_and_type_is(self.config, 'frequency_col', str)
        self.frequency_col = self.config['frequency_col']

        # countvectorizer: OPTIONAL
        if 'countvectorizer' in self.config:
            self.error_handler.assert_key_type_is(self.config, 'countvectorizer', dict)
        self.tfidfvectorizer_kwargs = self.config.get('tfidfvectorizer', self.DEFAULT_TFIDFVECTORIZER_KWARGS)

        if '__line__' in self.tfidfvectorizer_kwargs:
            del self.tfidfvectorizer_kwargs['__line__']


    def execute(self):
        """
        Convert a dataframe into term-freqs using CountVectorizer.

        :return:
        """
        super().execute()

        corpus = self.data[self.document_col]

        # Establish the TfIdfVectorizer with the user-provided arguments.
        vectorizer = TfidfVectorizer(**self.tfidfvectorizer_kwargs)

        # Get the term frequencies and document frequencies.
        self.data = pd.DataFrame(
            vectorizer.fit_transform(corpus).todense().tolist(),
            columns=vectorizer.get_feature_names_out()
        ).T.sum(axis=1).to_frame(name=self.frequency_col)
        self.force_dask()

        return self.data


# class TermFrequencyOperation(Operation):
#     """
#
#     """
#     CHUNKSIZE = 1024 * 1024 * 100  # 100 MB
#
#     DEFAULT_COUNTVECTORIZER_KWARGS = {
#
#     }
#
#     def __init__(self, *args, **kwargs):
#         super().__init__(*args, **kwargs)
#
#         self.allowed_configs.update(['document_col', 'countvectorizer'])
#
#         self.document_col = None
#         self.countvectorizer_kwargs = None
#
#
#     def compile(self):
#         """
#
#         :return:
#         """
#         super().compile()
#
#         self.error_handler.assert_key_exists_and_type_is(self.config, "document_col", str)
#         self.document_col = self.config['document_col']
#
#         if 'countvectorizer' in self.config:
#             self.error_handler.assert_key_type_is(self.config, 'countvectorizer', dict)
#         self.countvectorizer_kwargs = self.config.get('countvectorizer', self.DEFAULT_COUNTVECTORIZER_KWARGS)
#
#
#     def execute(self):
#         """
#         Convert a dataframe into term-freqs using CountVectorizer.
#
#         :return:
#         """
#         super().execute()
#
#         _index = list(self.data.columns)
#         _index.remove(self.document_col)
#
#         # Build out the list of documents to feed to the vectorizer.
#         documents = (
#             self.data
#             .reset_index()
#             .set_index(_index)
#             [self.document_col]
#         )
#
#         # Establish the CountVectorizer with the user-provided arguments.
#         vectorizer = CountVectorizer(**self.countvectorizer_args)
#
#         # Get the term frequencies and document frequencies.
#         X = vectorizer.fit_transform(documents)
#         features = vectorizer.get_feature_names_out()
#
#         self.data = dd.from_pandas(
#             pd.DataFrame(
#                 X.toarray(),
#                 columns=features
#             ),
#             chunksize=self.CHUNKSIZE
#         ).set_index(documents.index)
#
#         # self.data['_term_frequencies'] = self.data[features].agg(lambda x: x.to_json(), axis=1)
#         # self.data = self.data.drop(columns=features)
#
#         return self.data
#
#
#
# class TfIdfFromTermFrequencyOperation(Operation):
#     """
#
#     """
#     CHUNKSIZE = 1024 * 1024 * 100  # 100 MB
#
#     def __init__(self, *args, **kwargs):
#         super().__init__(*args, **kwargs)
#
#         self.allowed_configs.update(['filtered_source', 'smooth_idf'])
#
#         self.filtered_source = None
#         self.smooth_idf = None
#
#
#     def compile(self):
#         """
#
#         :return:
#         """
#         super().compile()
#
#         if 'filtered_source' in self.config:
#             self.error_handler.assert_key_type_is(self.config, 'filtered_source', str)
#             self.filtered_source = self.config['filtered_source']
#
#         if 'smooth_idf' in self.config:
#             self.error_handler.assert_key_type_is(self.config, 'smooth_idf', bool)
#         self.smooth_idf = self.config.get('smooth_idf', True)
#
#
#     def execute(self):
#         """
#         Combine term- and doc-freqs of a whole and/or filtered dataset
#         into one set of word-freqs to pass to Wordcloud Destination.
#
#         :return:
#         """
#         super().execute()  # Define self.data
#
#         ### Get all variables for calculating filtered IDFs.
#         # Build the document frequencies from the term frequencies.
#         term_freqs = self.data
#         doc_freqs = self.get_doc_freqs(term_freqs)
#
#         # Apply smoothing where required.
#         len_term_freqs = len(term_freqs) + int(self.smooth_idf)
#         doc_freqs += int(self.smooth_idf)
#
#         # Do the same for filtered document frequencies, if present.
#         if self.filtered_source:
#             filtered_term_freqs = self.filtered_source.data
#             filtered_doc_freqs = self.get_doc_freqs(filtered_term_freqs)
#
#             len_filtered_term_freqs = len(self.filtered_source)
#
#             # Calculate IDFs by combining filtered rows.
#             inverse_doc_freqs = np.log(
#                 (len_term_freqs - len_filtered_term_freqs)
#                 / (doc_freqs - filtered_doc_freqs)
#             ) + 1
#
#             term_freqs = filtered_term_freqs  # Update term-freqs to the subset to share more code below.
#
#         # Otherwise, build the IDFs off the entire dataset.
#         else:
#             inverse_doc_freqs = np.log(
#                 len_term_freqs / doc_freqs
#             ) + 1
#
#         # Merge them into one TF-IDF and normalize.
#         tfidfs = term_freqs * inverse_doc_freqs
#         tfidfs = pd.DataFrame(
#             sklearn.preprocessing.normalize(tfidfs, norm='l2', axis=1)
#         )
#
#         # Sum into word frequency dicts of words to tfidf.
#         word_frequencies = (
#             pd.DataFrame(tfidfs.sum())
#             .set_index(term_freqs.columns)
#             .to_dict()
#             [0]
#         )
#
#         # Filter out zero-count items (to allow word cloud repeat to actually work.)
#         word_frequencies = {
#             word: freq for word, freq in word_frequencies.items() if freq > 0
#         }
#
#         self.data = dd.from_pandas(
#             pd.DataFrame.from_dict(word_frequencies, columns=['word', 'frequency']),
#             chunksize=self.CHUNKSIZE
#         )
#         return self.data
#
#
#     @staticmethod
#     def get_doc_freqs(term_freqs):
#         """
#         Convert a dataframe of term-freqs of tokens by line into doc-freq counts.
#
#         :param term_freqs:
#         :return:
#         """
#         return np.array(term_freqs.astype(bool).sum())
