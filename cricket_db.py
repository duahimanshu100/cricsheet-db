import abc
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from cricket_db.models import Base, Match, Competition, Team, Player, Umpire
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
        lst_objects = PreprocessObjects(lst_objects, self.session).modify_lst_objects_process()
        self.session.bulk_save_objects(lst_objects)
        self.session.commit()
        self.session.close()

    def dump_data_from_file(self, file_name):
        reader = CricsheetXMLReader()
        lst_objects = reader.get_lst_objects_from_file(file_name)
        lst_objects = PreprocessObjects(lst_objects).modify_lst_objects_process()
        self.session.bulk_save_objects(lst_objects)
        self.session.commit()
        self.session.close()


class PreprocessObjects:
    def __init__(self, lst_objects, session):
        self.lst_objects = lst_objects
        self.session = session

    def modify_lst_objects_process(self):
        lst_modified_objects = []
        for object in self.lst_objects:
            if isinstance(object, Match):
                match = MatchPreprocessObjects(object, self.session)
                match.process()
                lst_modified_objects.append(match.match)
        return lst_modified_objects


class MatchPreprocessObjects:
    def __init__(self, match, session):
        self.match = match
        self.session = session
        self.lst_objects = []

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
        kwargs = {"name": self.match.competition}
        competition, created = Utils.get_or_create(self.session, Competition, **kwargs)
        self.match.competition = competition.id
        self.lst_objects.append(competition)

    def process_team_home(self):
        kwargs = {"name": self.match.team_home}
        team, created = Utils.get_or_create(self.session, Team, **kwargs)
        self.match.team_home = team.id
        self.lst_objects.append(team)

    def process_team_away(self):
        kwargs = {"name": self.match.team_away}
        team, created = Utils.get_or_create(self.session, Team, **kwargs)
        self.match.team_away = team.id
        self.lst_objects.append(team)

    def process_winner(self):
        kwargs = {"name": self.match.winner}
        team, created = Utils.get_or_create(self.session, Team, **kwargs)
        self.match.winner = team.id
        self.lst_objects.append(team)

    def process_player_of_match(self):
        kwargs = {"name": self.match.player_of_match}
        player_of_match, created = Utils.get_or_create(self.session, Player, **kwargs)
        self.match.player_of_match = player_of_match.id
        self.lst_objects.append(player_of_match)

    def process_toss_won_by(self):
        kwargs = {"name": self.match.toss_won_by}
        toss_won_by, created = Utils.get_or_create(self.session, Team, **kwargs)
        self.match.toss_won_by = toss_won_by.id
        self.lst_objects.append(toss_won_by)

    def process_umpire_first(self):
        kwargs = {"name": self.match.umpire_first}
        umpire_first, created = Utils.get_or_create(self.session, Umpire, **kwargs)
        self.match.umpire_first = umpire_first.id
        self.lst_objects.append(umpire_first)

    def process_umpire_second(self):
        kwargs = {"name": self.match.umpire_second}
        umpire_second, created = Utils.get_or_create(self.session, Umpire, **kwargs)
        self.match.umpire_second = umpire_second.id
        self.lst_objects.append(umpire_second)

    def process_umpire_third(self):
        kwargs = {"name": self.match.umpire_third}
        umpire_third, created = Utils.get_or_create(self.session, Umpire, **kwargs)
        self.match.umpire_third = umpire_third.id
        self.lst_objects.append(umpire_third)

    def process_umpire_forth(self):
        kwargs = {"name": self.match.umpire_forth}
        umpire_forth, created = Utils.get_or_create(self.session, Umpire, **kwargs)
        self.match.umpire_forth = umpire_forth.id
        self.lst_objects.append(umpire_forth)


if __name__ == '__main__':
    # engine = SQlLiteEngine(database_name="cricsheet3.db").create_engine()
    engine = PostgresEngine("localhost", "cricsheet", "postgres", "password").create_engine()
    dump_cricket_db = DumpCricketDB(engine)
    dump_cricket_db.dump_data_from_directory()
