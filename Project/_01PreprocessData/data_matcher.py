import re


def find_status_consumer_match(meta):
    def replace_short_remove_fill(string):
        fill_words = ['Whether', 'Warm', 'Used', 'Usage', 'Total', 'Supply', 'Subset', 'Status', 'Simulate', 'Sens',
                      'Power', 'Plugged', 'Peninsula', 'Opening', 'Number', 'Note', 'Needed', 'Midnight', 'Located',
                      'Load',
                      'load', 'Leg', 'Items', 'Into', 'Instrumentation', 'Instantaneous', 'Inst', 'Indicate',
                      'Includes', 'Get',
                      'From', 'Emulator', 'Electrical', 'Consumption', 'Appliance', 'Adding', 'Activated',
                      '(1:Yes,O:No)', ';', '.', ',']
        something_dict = {
            '1Lights': ['1Lights', '1Light'],
            '1st': ['1st', 'First'],
            '2nd': ['2nd', 'Second'],
            'AUP': ['AUP', 'AUpstairs'],
            'BUP': ['BUP', 'BUpstairs'],
            'ADOWN': ['ADOWN', 'ADownstairs'],
            'BDOWN': ['BDOWN', 'BDownstairs'],
            'ALighting': ['ALighting', 'ALight'],
            'And': ['And', ''],
            'Are': ['Are', ''],
            'At': ['At', ''],
            'Blue': ['Blue', 'Blu'],
            'By': ['By', ''],
            'Cooktop': ['Cooktop', 'Oven'],
            'Entry': ['Entry', ''],
            'Hall': ['Hall', 'Hallway'],
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

    def direct_matcher(sub_subsystems, condition):
        included = False
        return_dict = None
        for consumer_orig, consumer_row in meta.loc[
            lambda self: consumer_condition(self, sub_subsystems) & location_condition(
                self)].iterrows():
            consumer = replace_short_remove_fill(consumer_orig.split('_')[-1])
            joined_consumer_description = ''.join(part.capitalize() for part in consumer_row['Description'].split(' '))
            cleaned_consumer_description = replace_short_remove_fill(joined_consumer_description)
            consumer_parts_set = set(
                re.findall('[A-Z0-9][a-z]*', cleaned_consumer_description) +
                re.findall('[A-Z0-9][a-z]*', consumer)
            )
            if ((('Light' in status_parts_set and 'Light' in consumer_parts_set) or (
                    'Plug' in status_parts_set and 'Plug' in consumer_parts_set)) or (
                    ('Light' not in status_parts_set and 'Light' not in consumer_parts_set and
                     'Plug' not in status_parts_set and 'Plug' not in consumer_parts_set))):
                if condition(status_parts_set, consumer_parts_set):
                    return_dict = {
                        'consumer': consumer_orig,
                        'match_words': [status_parts_set, consumer_parts_set]
                    }
                    included = True
                    break

        return return_dict, included

    conditions = [
        (lambda set1, set2: len(set1 - set2) + len(set2 - set1) == 0),
        (lambda set1, set2: len(set2 - set1) == 0),
        (lambda set1, set2: len(set1 - set2) == 0),
        (lambda set1, set2: len(set1 - set2) + len(set2 - set1) == 1),
        (lambda set1, set2: len(set1 - set2) + len(set2 - set1) < max(len(set1), len(set2))),
        (lambda set1, set2: len(set1 - set2) + len(set2 - set1) < len(set1) + len(set2) / 2),
        (lambda set1, set2: len(set1 - set2) + len(set2 - set1) < min(len(set1), len(set2))),
        (lambda set1, set2: max(len(set1 - set2), len(set2 - set1)) < min(len(set1), len(set2))),
        (lambda set1, set2: (len(set1 - set2) + len(set2 - set1)) / 2 < min(len(set1), len(set2))),
        (lambda set1, set2: min(len(set1 - set2), len(set2 - set1)) < min(len(set1), len(set2)))
    ]

    status_condition = (lambda self:
                        # (self.index.str.contains("Load")) &
                        # (self['Subsystem'] == 'Loads') &
                        # (~self.index.str.contains('SensHeat')) &
                        (self['Units'] == 'BinaryStatus'))
    status_attributes = meta.loc[status_condition]
    consumer_condition = (lambda self, subsystems:
                          self.index.str.contains(subsystems) &
                          # (self['Subsystem'].isin(subsystems)) &
                          (self['Units'] == 'W'))

    matches_dict = {}
    other_dict = {}
    for status_orig, status_row in status_attributes.iterrows():
        status = replace_short_remove_fill(status_orig.split('_')[-1])
        joined_status_description = ''.join(part.capitalize() for part in status_row['Description'].split(' '))
        cleaned_status_description = replace_short_remove_fill(joined_status_description)
        status_room = status_row['Measurement_Location']

        location_conditions = [
            (lambda self:
             (self['Measurement_Location'].isin([status_row['Measurement_Location'], 'Multiple'])) &
             (self['Measurement_Floor'] == status_row['Measurement_Floor'])),
            (lambda self:
             (self['Measurement_Floor'] == status_row['Measurement_Floor']))
        ]

        status_parts_set = set(
            re.findall('[A-Z0-9][a-z]*', cleaned_status_description) +
            re.findall('[A-Z0-9][a-z]*', status)
        )

        for sub_system in ['Load', 'Elec', 'DHW']:
            included = False
            matches_dict[sub_system] = matches_dict.get(sub_system, {})
            matches_dict[sub_system][status_orig] = matches_dict[sub_system].get(status_orig, None)
            for location_condition in location_conditions:
                condition_index = 0
                while not included and condition_index < len(conditions):
                    matches_dict[sub_system][status_orig], included = direct_matcher(sub_system,
                                                                                     conditions[condition_index])
                    condition_index += 1

    return matches_dict
