""" Module for text processing.
"""

import nltk.tokenize
import re

from barbante.utils.profiling import profile
import barbante.utils.logging as barbante_logger

log = barbante_logger.get_logger(__name__)


def get_stems(tokens, language):
    """ Returns the stems of the informed tokens.

        :param tokens: A list of tokens.
        :param language: The language of the tokens.

        :returns: A list of tokens, in the same corresponding order as in the *tokens* list.
    """
    result = []
    try:
        stemmer = nltk.stem.SnowballStemmer(language)
    except ValueError:
        return tokens
    for token in tokens:
        try:
            stem = stemmer.stem(token)
            result += [stem]
        except Exception as err:
            log.error("Error while stemming {0} term [{1}]: {2}".format(language, token, err))
    return result


@profile
def calculate_tf(lang="", doc=""):
    """ Returns a map with all non-stopwords and its respective frequencies.

        Ex: {"work": 1, "going": 1}
    """
    tf_by_stem = {}

    # Cleaning document
    doc = re.sub(" +", " ", doc).lower()
    tokens = remove_stopwords(tokenize(doc), lang, min_len=3, max_len=30)

    stems = get_stems(tokens, lang)

    for stem in stems:
        tf_by_stem[stem] = tf_by_stem.get(stem, 0) + 1

    return tf_by_stem


@profile
def calculate_tf_from_stems(stems):
    """ Returns a map with the stems respective frequencies.

        Ex: {"work": 1, "going": 1}
    """
    tf_by_stem = {}

    for stem in stems:
        tf_by_stem[stem] = tf_by_stem.get(stem, 0) + 1

    return tf_by_stem


def remove_stopwords(tokens, language, min_len=1, max_len=30):
    """ Removes the stopwords from a list of terms.

        :param tokens: A list of tokens.
        :param language: The language of the terms.
        :param min_len: The minimum size to be considered when removing stopwords.
        :param max_len: The maximum size to be considered when removing stopwords.

        :returns: A list of tokens free of stopwords.
    """
    try:
        stopwords = set(nltk.corpus.stopwords.words(language))
    except Exception:
        return tokens

    stopwords.add("amp")
    stopwords.add("quot")
    stopwords.add("href")
    stopwords.add("http")
    stopwords.add("://")
    stopwords.add(".&#")
    try:
        result = [w for w in tokens if w not in stopwords and
                  min_len <= len(w) <= max_len]
    except IOError:
        return
    except AttributeError:
        return
    except TypeError as error:
        raise TypeError(
            "barbante.utils.text.remove_stopwords: {0}".format(error))

    return result


def parse_text_to_stems(language, text, min_length=3):
    """ Parse a text attribute performing cleanup, tokenization, stemmization and removal of stop-words.

        :param language: The text language, relevant for stemmization.
        :param text: The text to be stemmized.
        :param min_length: The minimum number of characters that a word must have; otherwise it is discarded.

        :returns: A list of terms.
    """
    text = re.sub(" +", " ", text).lower()
    tokens = tokenize(text)
    stems = get_stems(tokens, language)
    return remove_stopwords(stems, language, min_length)


def tokenize(text):
    """ Returns a list with all words (tokens) in *text*.

        :param text: The text to be tokenized.
        :returns: A list of tokens.

        See also: http://www.nltk.org/api/nltk.tokenize.html
    """

    if type(text) is not str:
        raise TypeError("barbante.utils.text.tokenize: text must be a string.")

    invalid_characters = set()
    invalid_characters.add("\n")
    invalid_characters.add("\t")
    invalid_characters.add(".")
    invalid_characters.add(",")
    invalid_characters.add("?")
    invalid_characters.add("!")
    invalid_characters.add("$")
    invalid_characters.add('"')
    # We may want to replace each invalid character with a reserved mnemonic.

    text_as_list = [c for c in text if c not in invalid_characters]
    text = ''.join(text_as_list)
    text = text.strip()

    tokens = nltk.tokenize.WordPunctTokenizer().tokenize(text)

    return tokens


def count_common_terms(list1, list2):
    """ Returns the number of common terms in two lists of terms.
    """
    if list1 is None or list2 is None:
        return 0
    return len(set(list1) & set(list2))
