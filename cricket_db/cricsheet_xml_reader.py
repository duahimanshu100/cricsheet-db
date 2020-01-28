import os
import xmltodict
import yaml

from cricket_db.models import Scoresheet, Match, Team, Competition
from cricket_db.models import Player, Umpire, Innings, Delivery, Wicket
from cricket_db.parsers.scoresheet_info import ScoresheetInfoParser
from cricket_db.parsers.match import MatchParser
from cricket_db.parsers.innings import InningsParser
from cricket_db.parsers.delivery import DeliveryParser
from cricket_db.parsers.wicket import WicketParser

ENSURE_LIST = lambda x: [x] if not isinstance(x, list) else x


class CricsheetXMLReader(object):
    def __init__(self):
        pass

    @staticmethod
    def first_key_dict(temp_dict):
        return next(iter(temp_dict))

    def get_lst_objects_from_directory(self, directory):
        objects = list()
        for filename in os.listdir(directory):
            file_name = '/'.join([directory, filename])
            lst_of_objects = self.get_lst_objects_from_file(file_name)
            if lst_of_objects:
                objects.extend(lst_of_objects)
        return objects

    def get_lst_objects_from_file(self, file_name):
        if file_name.startswith('data/.'):
            return []
        objects = list()
        match_id = file_name.split('/')[-1].split('.')[0]
        with open(file_name, 'r') as stream:
            print(f'{file_name} is being processed')
            raw_file = stream.read()
            try:
                raw = yaml.safe_load(raw_file)
            except yaml.YAMLError as e:
                print("Parsing YAML string failed")
                print("Reason:", e.reason)
                print("At position: {0} with encoding {1}".format(e.position, e.encoding))
                print("Invalid char code:", e.character)

        match_parser = MatchParser(match_id)
        match_parse_result = match_parser.parse(raw['info'])
        objects.append(Match(**match_parse_result))

        scoresheet_info_parser = ScoresheetInfoParser(match_id)
        objects.append(Scoresheet(**scoresheet_info_parser.parse(raw['meta'])))

        for innings in ENSURE_LIST(raw['innings']):
            innings_number = CricsheetXMLReader.first_key_dict(innings)
            innings_parser = InningsParser(match_id, str(innings_number))
            objects.append(Innings(**innings_parser.parse(innings[innings_number])))

            for delivery in ENSURE_LIST(innings[innings_number]['deliveries']):
                delivery_first_key = CricsheetXMLReader.first_key_dict(delivery)
                over_number, ball_number = str(delivery_first_key).split('.')[0], str(delivery_first_key).split('.')[1]
                delivery_parser = DeliveryParser(match_id, innings_number, over_number, ball_number)
                objects.append(Delivery(**delivery_parser.parse(delivery[delivery_first_key])))
                if 'wicket' in delivery[delivery_first_key]:
                    for wicket in ENSURE_LIST(delivery[delivery_first_key]['wicket']):
                        wicket_parser = WicketParser(match_id, innings_number, over_number, ball_number)
                        objects.append(Wicket(**wicket_parser.parse(wicket)))
        return objects
