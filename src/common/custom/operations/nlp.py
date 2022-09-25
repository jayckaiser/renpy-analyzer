import spacy

# from spacytextblob.spacytextblob import SpacyTextBlob

from earthmover.nodes.operation import Operation


class ApplySpacyOperation(Operation):
    """

    """
    SENTENCE_JOIN_STR = '\n'
    WORD_JOIN_STR = ' '

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

        self.allowed_configs.update(["document_col"])

        self.document_col = None


    def compile(self):
        """

        :return:
        """
        super().compile()

        # document_col: REQUIRED
        self.error_handler.assert_key_exists_and_type_is(self.config, 'document_col', str)
        self.document_col = self.config['document_col']


    def execute(self):
        """

        :return:
        """
        super().execute()

        # Build the spacy language model, and add the sentiment pipe.
        nlp = spacy.load('en_core_web_sm')
        nlp.add_pipe('spacytextblob')

        # Pipe the lines through the spaCy document-creator.
        docs = list(
            nlp.pipe(self.data['line'])
        )

        ### Build up the new columns' values as lists.

        # Add sentiment (polarity) and subjectivity, from `spacytextblob`.
        sentiment_list = list(map(
            lambda doc: doc._.polarity,
            docs
        ))

        subjectivity_list = list(map(
            lambda doc: doc._.subjectivity,
            docs
        ))

        # Isolate sentences
        sentences_list = list(map(
            lambda doc: [sent.text for sent in doc.sents],
            docs
        ))
        num_sentences_list = list(map(len, sentences_list))
        sentences_list = list(map(
            lambda sents: self.SENTENCE_JOIN_STR.join(sents),
            sentences_list
        ))

        # Extract lists of actual words
        words_list = list(map(
            lambda doc: [token.orth_ for token in doc
                         if not token.is_punct],
            docs
        ))
        num_words_list = list(map(len, words_list))
        words_list = list(map(
            lambda words: self.WORD_JOIN_STR.join(words),
            words_list
        ))

        # Extract non-stop words
        content_words_list = list(map(
            lambda doc: [token.orth_.lower() for token in doc
                         if not token.is_stop and not token.is_punct],
            docs
        ))
        num_content_words_list = list(map(len, content_words_list))
        content_words_list = list(map(
            lambda words: self.WORD_JOIN_STR.join(words),
            content_words_list
        ))

        # Save the new NLP information as columns.
        self.data['sentiment'] = sentiment_list
        self.data['subjectivity'] = subjectivity_list

        self.data['sentences'] = sentences_list
        self.data['num_sentences'] = num_sentences_list

        self.data['words'] = words_list
        self.data['num_words'] = num_words_list

        self.data['content_words'] = content_words_list
        self.data['num_content_words'] = num_content_words_list

        return self.data
