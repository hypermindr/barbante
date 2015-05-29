""" Test module for barbante.text.
"""

import nose.tools

import barbante.utils.text as text


def test_calculate_tf_en():
    """ Tests calculate_tf for English contents.
    """
    language = "english"
    contents = "Cooks who don't love cooking don't cook well."
    results = text.calculate_tf(language, contents)
    nose.tools.eq_(results['cook'], 3, "Wrong TF")
    nose.tools.eq_(results['love'], 1, "Wrong TF")
    nose.tools.eq_(results['well'], 1, "Wrong TF")


def test_calculate_tf_pt():
    """ Tests calculate_tf for Portuguese contents.
    """
    language = "portuguese"
    contents = "Eu não gostava do gosto gasto do gesto de agosto."
    results = text.calculate_tf(language, contents)
    nose.tools.eq_(results['gost'], 2, "Wrong TF")
    nose.tools.eq_(results['gast'], 1, "Wrong TF")
    nose.tools.eq_(results['gest'], 1, "Wrong TF")
    nose.tools.eq_(results['agost'], 1, "Wrong TF")


def test_performance():
    """ Tests calculate_tf for huge texts.
    """
    import random
    palavras = ["zero", "one", "two", "three", "four", "five", "six",
                "seven", "eight", "nine", "R$1000.00"]
    contents = ""
    language = 'english'
    for _ in range(10000):  # increase number and measure time when necessary
        contents += palavras[random.randint(0, len(palavras) - 1)] + " "
    text.calculate_tf(language, contents)


def test_tokenize():
    """ Tests tokenization.
    """
    actual = text.tokenize("The car is going to Mountain View. You! You \
                            should go too... Or, maybe, shouldn't!?")
    expected = ["The", "car", "is", "going", "to", "Mountain", "View", "You",
                "You", "should", "go", "too", "Or", "maybe", "shouldn", "\'",
                "t"]

    nose.tools.eq_(actual, expected)


def test_remove_stopwords():
    """ Tests removal of stopwords.
    """
    actual = text.remove_stopwords(["The", "car", "is", "going", "to",
                                    "crash", "or", "going", "to", "win"],
                                   "english", 3)
    expected = ['The', 'car', 'going', 'crash', 'going', 'win']
    nose.tools.eq_(actual, expected)


def test_count_common_terms_English():
    """ Tests common terms counting.
    """
    language = "english"
    text1 = "Just a test sentence for the purpose of just testing common terms counting."
    text2 = "This is just a sentence for tests purposes."
    text1_tokens = text.tokenize(text1)
    text2_tokens = text.tokenize(text2)
    text1_stems = text.get_stems(text1_tokens, language)
    text2_stems = text.get_stems(text2_tokens, language)
    text1_stems_no_stopwords = set(text.remove_stopwords(text1_stems, language))
    text2_stems_no_stopwords = set(text.remove_stopwords(text2_stems, language))
    nose.tools.eq_(text.count_common_terms(text1_stems_no_stopwords,
                                           text2_stems_no_stopwords),
                   3)  # sentence, purpos3, tests
