import abc
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from cricket_db.models import Base, Match, Competition, Team, Player, Umpire, Delivery, Innings
from cricket_db.cricsheet_xml_reader import CricsheetXMLReader
from cricket_db.utils import Utils


class Engine:
    def __init__(self, host="", database_name="cricsheet", user="", password=""):
        self.database_name = database_name
        self.host = host
        self.user = user
        self.password = password

    @abc.abstractmethod
    def create_engine(self, input):
        """Create engine and return engine"""
        return


class SQlLiteEngine(Engine):

    def create_engine(self):
        """Create engine and return engine"""
        return create_engine('sqlite:///' + self.database_name, echo=True)


class PostgresEngine(Engine):

    def create_engine(self):
        """Create engine and return engine"""
        connection_string = f'postgresql://{self.user}:{self.password}@{self.host}/{self.database_name}'
        return create_engine(connection_string)


class DumpCricketDB:
    def __init__(self, engine):
        Session = sessionmaker(bind=engine)
        self.session = Session()
        Base.metadata.create_all(engine)

    def dump_data_from_directory(self, dir_path='data'):
        reader = CricsheetXMLReader()
        lst_objects = reader.get_lst_objects_from_directory(dir_path)
        self.dump_match(lst_objects)
        self.dump_innings(lst_objects)
        self.dump_deliveries(lst_objects)

    def dump_data_from_file(self, file_name):
        reader = CricsheetXMLReader()
        lst_objects = reader.get_lst_objects_from_file(file_name)
        self.session.bulk_save_objects(lst_objects)
        self.session.commit()
        self.session.close()

    def dump_match(self, lst_objects):
        lst_modified_objects = []
        for object in lst_objects:
            if isinstance(object, Match):
                match = MatchPreprocessObjects(object, self.session)
                match.process()
                lst_modified_objects.append(match.obj)
        self.session.bulk_save_objects(lst_modified_objects)
        self.session.commit()

    def dump_innings(self, lst_objects):
        lst_modified_objects = []
        for object in lst_objects:
            if isinstance(object, Innings):
                innings = InningsPreprocessObjects(object, self.session)
                innings.process()
                lst_modified_objects.append(innings.obj)
        self.session.bulk_save_objects(lst_modified_objects)
        self.session.commit()

    def dump_deliveries(self, lst_objects):
        lst_modified_objects = []
        for object in lst_objects:
            if isinstance(object, Delivery):
                delivery = DeliveryPreprocessObjects(object, self.session)
                delivery.process()
                lst_modified_objects.append(delivery.obj)
                try:
                    self.session.add(delivery.obj)
                    self.session.commit()
                except Exception as e:
                    print(e, self.session.rollback())


class AbstractPreprocessObjects:

    def __init__(self, obj, session):
        self.obj = obj
        self.session = session
        self.lst_objects = []

    @abc.abstractmethod
    def process(self):
        """ process the object """

        return


class MatchPreprocessObjects(AbstractPreprocessObjects):

    def process(self):
        self.process_competition()
        self.process_team_home()
        self.process_team_away()
        self.process_winner()
        self.process_player_of_match()
        self.process_toss_won_by()
        self.process_umpire_first()
        self.process_umpire_second()
        self.process_umpire_third()
        self.process_umpire_forth()

    def process_competition(self):
        kwargs = {"name": self.obj.competition}
        competition, created = Utils.get_or_create(self.session, Competition, **kwargs)
        self.obj.competition = competition.id
        self.lst_objects.append(competition)

    def process_team_home(self):
        kwargs = {"name": self.obj.team_home}
        team, created = Utils.get_or_create(self.session, Team, **kwargs)
        self.obj.team_home = team.id
        self.lst_objects.append(team)

    def process_team_away(self):
        kwargs = {"name": self.obj.team_away}
        team, created = Utils.get_or_create(self.session, Team, **kwargs)
        self.obj.team_away = team.id
        self.lst_objects.append(team)

    def process_winner(self):
        kwargs = {"name": self.obj.winner}
        team, created = Utils.get_or_create(self.session, Team, **kwargs)
        self.obj.winner = team.id
        self.lst_objects.append(team)

    def process_player_of_match(self):
        kwargs = {"name": self.obj.player_of_match}
        player_of_match, created = Utils.get_or_create(self.session, Player, **kwargs)
        self.obj.player_of_match = player_of_match.id
        self.lst_objects.append(player_of_match)

    def process_toss_won_by(self):
        kwargs = {"name": self.obj.toss_won_by}
        toss_won_by, created = Utils.get_or_create(self.session, Team, **kwargs)
        self.obj.toss_won_by = toss_won_by.id
        self.lst_objects.append(toss_won_by)

    def process_umpire_first(self):
        kwargs = {"name": self.obj.umpire_first}
        umpire_first, created = Utils.get_or_create(self.session, Umpire, **kwargs)
        self.obj.umpire_first = umpire_first.id
        self.lst_objects.append(umpire_first)

    def process_umpire_second(self):
        kwargs = {"name": self.obj.umpire_second}
        umpire_second, created = Utils.get_or_create(self.session, Umpire, **kwargs)
        self.obj.umpire_second = umpire_second.id
        self.lst_objects.append(umpire_second)

    def process_umpire_third(self):
        kwargs = {"name": self.obj.umpire_third}
        umpire_third, created = Utils.get_or_create(self.session, Umpire, **kwargs)
        self.obj.umpire_third = umpire_third.id
        self.lst_objects.append(umpire_third)

    def process_umpire_forth(self):
        kwargs = {"name": self.obj.umpire_forth}
        umpire_forth, created = Utils.get_or_create(self.session, Umpire, **kwargs)
        self.obj.umpire_forth = umpire_forth.id
        self.lst_objects.append(umpire_forth)


class InningsPreprocessObjects(AbstractPreprocessObjects):

    def process(self):
        self.process_batting_team()
        self.process_match()

    def process_batting_team(self):
        kwargs = {"name": self.obj.batting_team}
        batting_team, created = Utils.get_or_create(self.session, Team, **kwargs)
        self.obj.batting_team = batting_team.id
        self.lst_objects.append(batting_team)

    def process_match(self):
        kwargs = {"id": self.obj.match}
        match, created = Utils.get_or_create(self.session, Match, **kwargs)
        self.obj.match = match.id
        self.lst_objects.append(match)


class DeliveryPreprocessObjects(AbstractPreprocessObjects):

    def process(self):
        self.process_batsman()
        self.process_bowler()
        self.process_non_striker()
        self.process_inning_number()

    def process_batsman(self):
        kwargs = {"name": self.obj.batsman}
        batsman, created = Utils.get_or_create(self.session, Player, **kwargs)
        self.obj.batsman = batsman.id
        self.lst_objects.append(batsman)

    def process_bowler(self):
        kwargs = {"name": self.obj.bowler}
        bowler, created = Utils.get_or_create(self.session, Player, **kwargs)
        self.obj.bowler = bowler.id
        self.lst_objects.append(bowler)

    def process_non_striker(self):
        kwargs = {"name": self.obj.non_striker}
        non_striker, created = Utils.get_or_create(self.session, Player, **kwargs)
        self.obj.non_striker = non_striker.id
        self.lst_objects.append(non_striker)

    def process_inning_number(self):
        self.obj.innings = self.session. \
            query(Innings).filter_by(match=self.obj.match,
                                     innings_number=self.obj.innings).first().id


if __name__ == '__main__':
    # engine = SQlLiteEngine(database_name="cricsheet3.db").create_engine()
    HOST = "localhost"
    DATABASE_NAME = "cricsheet"
    DB_USER = "postgres"
    PASSWORD = "password"
    engine = PostgresEngine(HOST, DATABASE_NAME, DB_USER, PASSWORD).create_engine()
    dump_cricket_db = DumpCricketDB(engine)
    dump_cricket_db.dump_data_from_directory()
