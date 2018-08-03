import json
from collections import Counter


def row_list_to_json(row_list, period=None, confirm_only=False):

    # NB: include period and confidence filter at some point

    # https://stackoverflow.com/a/11011846/
    final_date_dict = Counter()

    for row in row_list:

        date_conf_dict = json.loads(row[0])
        proportion_covered = row[1]
        
        # update each value by the proportion covered of each tile
        # not spatially accurate, obviously, but better than binary 1|0
        # https://stackoverflow.com/a/16993582/
        date_conf_dict.update((x, y*proportion_covered) for x, y in date_conf_dict.items())

        # adding Counters will add dicts, summing values where we have
        # overlapping keys
        final_date_dict += Counter(date_conf_dict)

    # convert all to int
    return dict((x, int(y)) for x, y in final_date_dict.items())

