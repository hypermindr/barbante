#!/usr/bin/env python3
# -*- coding: utf-8 -*-

""" Merges 3 CSV files into 1.

    The two first columns identify the records.

    First file: numerators
        Columns: user, template, nc, na

    Second file: denominators
        Columns: user, template, denominator

    Third file: strengths
        Columns: user, template, strength

    **Example of usage**
        ``python3 -m barbante.scripts.merge_user_user_collections num.csv denom.csv strengths.csv output_file.csv``

    **Output**
        It saves a CSV file with the following columns: user, template, nc, na, denominator, strength.
"""

import json
import sys
import traceback
from time import time

import barbante.utils.logging as barbante_logging
log = barbante_logging.get_logger(__name__)


def merge_collections(numerators_file, denominators_file, strengths_file, output_file):
    log.info("----------")
    log.info("Start.")

    start = time()

    f_numerators = open(numerators_file, 'rU')
    f_denominators = open(denominators_file, 'rU')
    f_strengths = open(strengths_file, 'rU')

    # skips the headers
    next(f_numerators)
    next(f_denominators)
    next(f_strengths)

    f_output = open(output_file, 'w')
    f_output.write("user,template_user,nc,na,denominator,strength\n")

    numerator_key, nc, na = yield_numerator(f_numerators)
    denominator_key, denominator = yield_denominator(f_denominators)
    strength_key, strength = yield_strength(f_strengths)

    done = 0

    while True:

        keys = []
        if numerator_key is not None:
            keys += [numerator_key]
        if denominator_key is not None:
            keys += [denominator_key]
        if strength_key is not None:
            keys += [strength_key]
        if len(keys) == 0:
            break  # exhausted all files

        min_key = min(keys)

        merged_doc = {"user": min_key[0],
                      "template_user": min_key[1]}

        if numerator_key == min_key:
            merged_doc["nc"] = nc
            merged_doc["na"] = na
            numerator_key, nc, na = yield_numerator(f_numerators)
        else:
            merged_doc["nc"] = ""
            merged_doc["na"] = ""

        if denominator_key == min_key:
            merged_doc["denominator"] = denominator
            denominator_key, denominator = yield_denominator(f_denominators)
        else:
            merged_doc["denominator"] = ""

        if strength_key == min_key:
            merged_doc["strength"] = strength
            strength_key, strength = yield_strength(f_strengths)
        else:
            merged_doc["strength"] = ""

        write_to_file(merged_doc, f_output)

        done += 1
        if done % 100000 == 0:
            log.info("Done writing %d lines." % done)

    f_numerators.close()
    f_denominators.close()
    f_strengths.close()
    f_output.close()

    log.info("End. Took %d seconds." % (time() - start))


def yield_numerator(numerators_handler):
    try:
        numerator_line = next(numerators_handler).split(",")
        numerator_key = (numerator_line[0], numerator_line[1])
        nc = int(numerator_line[2])
        na = int(numerator_line[3])
    except:
        numerator_key, nc, na = None, None, None
    return numerator_key, nc, na


def yield_denominator(denominators_handler):
    try:
        denominator_line = next(denominators_handler).split(",")
        denominator_key = (denominator_line[0], denominator_line[1])
        denominator = int(denominator_line[2])
    except:
        denominator_key, denominator = None, None
    return denominator_key, denominator


def yield_strength(strengths_handler):
    try:
        strength_line = next(strengths_handler).split(",")
        strength_key = (strength_line[0], strength_line[1])
        strength = float(strength_line[2])
    except:
        strength_key, strength = None, None
    return strength_key, strength


def write_to_file(document, output_handler):
    line = ','.join([str(document["user"]),
                     str(document["template_user"]),
                     str(document["nc"]),
                     str(document["na"]),
                     str(document["denominator"]),
                     str(document["strength"])]) + '\n'
    output_handler.write(line)


def main(argv):
    if len(argv) < 4:
        msg = "You must specify the numerators file, the denominators file, " \
              "the strengths file and the output file."
        log.error(msg)
        return json.dumps({"success": False, "message": msg})
    try:
        # command-line arguments
        numerators_file = argv[0]
        denominators_file = argv[1]
        strengths_file = argv[2]
        output_file = argv[3]

        merge_collections(numerators_file, denominators_file, strengths_file, output_file)

    except Exception:
        log.exception('Exception on {0}'.format(__name__))
        return json.dumps({"success": False,
                           "message": traceback.format_exc()})

    return_json = json.dumps({"success": True})
    return return_json


if __name__ == '__main__':
    print(main(sys.argv[1:]))
