from sqlalchemy import Table, Column, Integer, String, MetaData, Date
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from pathlib import Path
from sqlalchemy import Table, Column, Integer, String, MetaData, Date
from sqlalchemy.ext.declarative import declarative_base
from io import StringIO
from sqlalchemy.sql import text

_session = None


Base = declarative_base()


class InfoBoxInstance(Base):
    __tablename__ = "InfoBoxInstance"

    id = Column(
        Integer,
        primary_key=True,
        unique=True,
        autoincrement=True,
    )
    literal = Column(String, index=True)
    language_code = Column(String, index=True)
    concept = Column(String, index=True)
    attributes = Column(String)

    def __repr__(self):
        return f"InfoBoxInstance: {self.id} {self.language_code},{self.literal},{self.concept},{self.attributes}"

    def __str__(self):
        with StringIO() as output:
            output.write(f"""{{ "id":{self.id}, "literal":"{self.literal}", "language":"{self.language_code}" """)
            if self.concept:
                output.write(f""", "concept":"{self.concept}" """)
            if self.concept:
                output.write(f""", "attributes":{self.attributes} """)
            output.write("}")
            return output.getvalue()


class InfoBoxData(Base):
    __tablename__ = "InfoBoxData"
    literal = Column(String, primary_key=True)
    language_code = Column(String, primary_key=True)
    source = Column(String, primary_key=True)
    json_string = Column(String)

    def __repr__(self):
        return f"InfoBoxData: {self.language_code},{self.literal},{self.source},{self.json_string}"


def checkTableExists(engine, tablename):
    with engine.connect() as con:
        dbcur = con.execute(
            text(
                f"""
            SELECT name
            FROM sqlite_master
            WHERE name = '{tablename}' """
            )
        )

        one = dbcur.fetchone()
        if one and one[0] == tablename:
            return True

    return False


#  perform a full text match on the literals,
#  we will have to see what the future brings when we unleash the beast.
def get_rec_match(session, literal):
    results = [i[0] for i in session.execute(text(f'SELECT rowid from InfoBoxInstance_idx where literal match "{literal}"')).all()]
    return results


def update_fulltext_table(engine):
    ddl = [
        """
        CREATE VIRTUAL TABLE InfoBoxInstance_idx USING fts5(
            literal,
            content='InfoBoxInstance',
            content_rowid='id'
        )
        """,
        """
        CREATE TRIGGER InfoBoxInstance_ai AFTER INSERT ON InfoBoxInstance BEGIN
            INSERT INTO InfoBoxInstance_idx (rowid, literal)
            VALUES (new.id, new.literal);
        END
        """,
        """
        CREATE TRIGGER InfoBoxInstance_ad AFTER DELETE ON InfoBoxInstance BEGIN
            INSERT INTO InfoBoxInstance_idx (InfoBoxInstance_idx, rowid, literal)
            VALUES ('delete', old.id, old.literal);
        END
        """,
        """
        CREATE TRIGGER InfoBoxInstance_au AFTER UPDATE ON InfoBoxInstance BEGIN
            INSERT INTO InfoBoxInstance_idx (InfoBoxInstance_idx, rowid, literal)
            VALUES ('delete', old.id, old.literal);
            INSERT INTO InfoBoxInstance_idx (rowid, literal)
            VALUES (new.id, new.literal);
        END
        """,
    ]

    if not checkTableExists(engine, "InfoBoxInstance_idx"):
        with engine.connect() as con:
            for statement in ddl:
                con.execute(text(statement))


def init_database(filename=None):
    try:
        if not filename:
            cfloder = Path(f"~/.wowool/cache/").expanduser()
            if not cfloder.exists():
                cfloder.mkdir(parents=True, exist_ok=True)
            filename = cfloder / f"wowool-wiki-attribute.db"

        engine = create_engine(f"sqlite:///{filename}?check_same_thread=false&timeout=10&nolock=1")
        Session = sessionmaker(bind=engine)

        global _session
        _session = Session()
        Base.metadata.create_all(engine)
        update_fulltext_table(engine)
    except Exception as ex:
        raise Exception(f"Error: infobox, init_database error [{filename}][{ex}]")

    return True


def session():
    if _session is None:
        if not init_database():
            raise Exception("Session not initialized")
    return _session
