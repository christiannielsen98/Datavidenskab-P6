import re
from difflib import SequenceMatcher

import pandas as pd

from Project.Database import Db


def replace_short_remove_fill(string):
    fill_words = ['Whether', 'Warm', 'Used', 'Usage', 'Total', 'Subset', 'Status', 'Simulate', 'Sens', 'Power',
                  'Plugged', 'Peninsula', 'Opening', 'Number', 'Midnight', 'Located', 'Load', 'load', 'Items', 'Into',
                  'Instrumentation', 'Instantaneous', 'Indicate', 'Includes', 'From', 'Emulator', 'Consumption',
                  'Appliance', 'Adding', 'Activated', '(1:Yes,O:No)', ';', '.', ',']
    something_dict = {
        '1Lights': ['1Lights', '1Light'],
        '1st': ['1st', 'First'],
        '2nd': ['2nd', 'Second'],
        'AUP': ['AUP', 'AUpstairs'],
        'BUP': ['BUP', 'BUpstairs'],
        'ADown': ['ADown', 'ADownstairs'],
        'BDown': ['BDown', 'BDownstairs'],
        'ALighting': ['ALighting', 'ALight'],
        'And': ['And', ''],
        'Are': ['Are', ''],
        'At': ['At', ''],
        'Blue': ['Blue', 'Blu'],
        'By': ['By', ''],
        'Cooktop': ['Cooktop', 'Oven'],
        'In': ['In', ''],
        'Is': ['Is', ''],
        'KPlugs': ['KPlugs', 'KitchenPlug'],
        'Kit': ['Kit', 'Kitchen'],
        'Lighting': ['Lighting', 'Light'],
        'Lights': ['Lights', 'Light'],
        'Of': ['Of', ''],
        'Off': ['Off', ''],
        'On': ['On', ''],
        'Or': ['Or', ''],
        'PCMonitor': ['PCMonitor', 'ComputerMonitor'],
        'Prnt': ['Prnt', 'Parent'],
        'Plugs': ['Plugs', 'Plug'],
        'Ray': ['Ray', 'ray'],
        'Receptacles': ['Receptacles', 'Plug'],
        'Room': ['Room', 'room'],
        'The': ['The', ''],
        'To': ['To', ''],
        'TV': ['TV', 'Television']
    }
    if 'MBR' in string:
        string = string.replace('MBR', 'MasterBedRoom')
    elif 'MBA' in string:
        string = string.replace('MBA', 'MasterBathroom')
    elif 'BR' in string:
        string = string.replace('BR', 'Bedroom')
    elif 'BA' in string:
        string = string.replace('BA', 'Bathroom')
    elif 'LR' in string:
        string = string.replace('LR', 'LivingRoom')
    elif 'DR' in string:
        string = string.replace('DR', 'DiningRoom')
    elif 'Receptacles' in string:
        string = string.replace('Receptacles', 'Plug')
    for fill_word in fill_words:
        if fill_word in string:
            string = string.replace(fill_word, '')
    for str_prt in re.findall('[A-Z0-9]+[a-z]*', string):
        if str_prt in something_dict.keys():
            string = string.replace(*something_dict[str_prt], 1)
    for str_prt in re.findall('[A-Z][a-z]*', string):
        if str_prt in something_dict.keys():
            string = string.replace(*something_dict[str_prt], 1)
    return string


def direct_matcher(status_parts_list, matches_dict, sub_subsystems):
    for consumer_orig, consumer_row in meta.loc[
        lambda self: consumer_condition(self, sub_subsystems) & location_condition(
            self)].iterrows():
        included = False
        consumer = replace_short_remove_fill(consumer_orig.split('_')[-1])
        joined_description = ''.join(part.capitalize() for part in consumer_row['Description'].split(' '))
        cleaned_description = replace_short_remove_fill(joined_description)
        consumer_parts_list = [
            re.findall('[A-Z0-9][a-z]*', cleaned_description),
            re.findall('[A-Z0-9][a-z]*', consumer)
        ]
        match_condition = (lambda n: (
                pd.Series([(status_part.lower() == compare_part.lower()) for status_part in
                           status_parts for compare_part in consumer_parts]).sum() >= n))
        # for i in range(1, max([len(lst) for lst in status_parts_list] + [len(lst) for lst in
        #                                                                  consumer_parts_list]) + 1)[::-1]:
        for status_parts in status_parts_list:
            for consumer_parts in consumer_parts_list:
                if ((('Light' in status_parts_list[1] and 'Light' in consumer_parts_list[1]) or (
                        'Plug' in status_parts_list[1] and 'Plug' in consumer_parts_list[1])) or (
                        ('Light' not in status_parts_list[1] and 'Light' not in consumer_parts_list[1] and
                        'Plug' not in status_parts_list[1] and 'Plug' not in consumer_parts_list[1]))):
                    # if match_condition(i):
                    matches_dict[status_orig].update({
                        # str(len(matches_dict[status_orig].keys())): {
                        str(len(set(consumer_parts) - set(status_parts))): {
                            'consumer': consumer_orig,
                            'match_words': [''.join(status_parts), ''.join(consumer_parts)]
                        }
                    })
        #             included = True
        #             break
        #         if included:
        #             break
        #     if included:
        #         break
        # if included:
        #     break
    return matches_dict


def distance_matcher(status_parts_list, other_dict, sub_subsystems):
    for consumer_orig, consumer_row in meta.loc[
        lambda self: consumer_condition(self, sub_subsystems) & location_condition(
            self)].iterrows():
        consumer = replace_short_remove_fill(consumer_orig.split('_')[-1])
        joined_description = ''.join(part.capitalize() for part in consumer_row['Description'].split(' '))
        cleaned_description = replace_short_remove_fill(joined_description)
        consumer_parts_list = [
            re.findall('[A-Z0-9][a-z]*', cleaned_description),
            re.findall('[A-Z0-9][a-z]*', consumer)
        ]
        if ('Light' in status and 'Light' in consumer) or not ('Light' in status or 'Light' in consumer):
            for status_parts in status_parts_list:
                for consumer_parts in consumer_parts_list:
                    other_dict[status_orig].update({
                        str(len(other_dict[status_orig])): {
                            'consumer': consumer_orig,
                            'match_words': [''.join(status_parts), ''.join(consumer_parts)],
                            'score': pd.Series([
                                SequenceMatcher(a=status_parts, b=consumer_parts).ratio()]).mean()
                            # for status_part in status_parts
                            # for consumer_part in consumer_parts]).mean()
                        }
                    })
    return other_dict


for hourly in [False]:  # True, False
    for year in [1]:  # 1, 2
        time_base = 'hour' if hourly else 'minute'
        meta = Db.load_data(meta=True, year=year, consumption=False)

        meta.set_index('Unnamed: 0', inplace=True)

        status_condition = (lambda self: (self['Units'] == 'Binary Status') &
                                         (~self.index.str.contains('SensHeat')) &
                                         (self['Subsystem'] == 'Loads'))
        status_attributes = meta.loc[status_condition]
        consumer_condition = (lambda self, subsystems:
                              (self['Subsystem'].isin(subsystems)) &
                              (~self['Description'].str.contains('emulator')) &
                              (self['Units'] == 'W'))

        matches_dict = {}
        other_dict = {}
        for status_orig, status_row in status_attributes.iterrows():
            status = replace_short_remove_fill(status_orig.split('_')[-1])
            joined_description = ''.join(part.capitalize() for part in status_row['Description'].split(' '))
            cleaned_description = replace_short_remove_fill(joined_description)
            status_room = status_row['Measurement_Location']

            location_condition = (
                lambda self: self['Measurement_Location'].isin([status_row['Measurement_Location'], 'Multiple']))

            status_parts_list = [
                re.findall('[A-Z0-9][a-z]*', cleaned_description),
                re.findall('[A-Z0-9][a-z]*', status)
            ]

            matches_dict[status_orig] = matches_dict.get(status_orig, {})
            matches_dict.update(
                direct_matcher(status_parts_list, matches_dict, ['Electrical', 'Lighting', 'Loads', 'Load']))

            # if status_orig not in matches_dict.keys():
            #     matches_dict.update(direct_matcher(status_parts_list, matches_dict, ['Loads', 'Load']))

            other_dict[status_orig] = other_dict.get(status_orig, {})
            other_dict.update(
                distance_matcher(status_parts_list, other_dict, ['Electrical', 'Lighting', 'Loads', 'Load']))

        for key, value in matches_dict.items():
            print(key)
            print('  ', value)
            print('  ', value[min(value.keys())])
        print(len(matches_dict))

        missing_statuses = [status for status in status_attributes.index if status not in matches_dict.keys()]
        print(missing_statuses)
        print(len(missing_statuses))

        # for key, value in other_dict.items():
        #     print(key)
        #     # print('  ', value[str(max(value.keys()))])
        #     for s_key, s_value in value.items():
        #         print('  ', s_key)
        #         print('    ', s_value)
