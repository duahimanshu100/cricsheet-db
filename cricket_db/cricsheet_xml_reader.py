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

    def get_objects_from_directory(self, directory):
        objects = list()
        for filename in os.listdir(directory):
            match_id = filename.split('.')[0]
            file_name = '/'.join([directory, filename])

            with open(file_name, 'r') as stream:
                if file_name.startswith('data/.'):
                    continue
                print(f'{file_name} is being processed')
                raw_file = stream.read()
                try:
                    raw = yaml.safe_load(raw_file)
                except yaml.YAMLError as e:
                    print("Parsing YAML string failed")
                    print("Reason:", e.reason)
                    print("At position: {0} with encoding {1}".format(e.position, e.encoding))
                    print("Invalid char code:", e.character)

            scoresheet_info_parser = ScoresheetInfoParser(match_id)
            objects.append(Scoresheet(**scoresheet_info_parser.parse(raw['meta'])))
            match_parser = MatchParser(match_id)
            objects.append(Match(**match_parser.parse(raw['info'])))

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
