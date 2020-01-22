import abc
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from cricket_db.models import Base
from cricket_db.cricsheet_xml_reader import CricsheetXMLReader


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
        objects = reader.get_lst_objects_from_directory(dir_path)
        self.session.bulk_save_objects(objects)
        self.session.commit()
        self.session.close()

    def dump_data_from_file(self, file_name):
        reader = CricsheetXMLReader()
        objects = reader.get_lst_objects_from_file(file_name)
        self.session.bulk_save_objects(objects)
        self.session.commit()
        self.session.close()



if __name__ == '__main__':
    engine = SQlLiteEngine(database_name = "cricsheet3.db").create_engine()
    # engine = PostgresEngine("localhost", "cricsheet", "postgres", "password").create_engine()
    dump_cricket_db = DumpCricketDB(engine)
    dump_cricket_db.dump_data_from_directory()
