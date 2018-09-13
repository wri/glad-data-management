import json


def row_list_to_json(row_list, period=None, confirm_only=False):

    # NB: include period and confidence filter at some point
    final_date_dict = {}

    for row in row_list:

        final_date_dict[row[0]] = row[1]
        
    return final_date_dict

