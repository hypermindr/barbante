#!/usr/bin/env python3
# -*- coding: utf-8 -*-

""" Calculates the Term-Frequency (TF) of all terms (after stemming)
    that appear in a document..
"""

import sys

import barbante.utils.text as text
from barbante.context.context_manager import new_context
import barbante.utils.logging as barbante_logging


log = barbante_logging.get_logger(__name__)


def main(argv):
    if len(argv) < 2:
        msg = "You must specify the language and the text"
        log.error(msg)
        return {"success": False, "message": msg}

    vocabulary_language = argv[0]
    text_for_tf = argv[1]

    tf_by_term = text.calculate_tf(vocabulary_language, text_for_tf)

    if tf_by_term is not None:
        return {"success": True, "results": tf_by_term}


if __name__ == '__main__':
    with new_context():
        print(main(sys.argv[1:]))
