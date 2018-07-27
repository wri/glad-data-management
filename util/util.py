import json
from collections import Counter


def row_list_to_json(row_list, period=None):

    # NB: include period filter at some point

    # https://stackoverflow.com/a/11011846/
    final_date_dict = Counter()

    for row in row_list:
        date_dict = Counter(json.loads(row[0]))

        # adding Counters will add dicts, summing values where we have
        # overlapping keys
        final_date_dict += date_dict

    return final_date_dict

