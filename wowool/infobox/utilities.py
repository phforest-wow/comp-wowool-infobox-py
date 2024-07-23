from requests.utils import requote_uri
from wowool.infobox.session import session, get_rec_match
from wowool.infobox.session import InfoBoxInstance, InfoBoxData

from wowool.infobox.config import config
import json
import traceback
import sys
from sqlalchemy.sql import text


def get_language_code(language):
    return config.get_language_code(language)


def convert_args(input, language):
    return input, requote_uri(input), config.get_language_code(language), config.get_language(language)


def get_rec(input, language="english", redirect=None):
    try:
        language_code = config.get_language_code(language)
        literal = input
        encoded_literal = requote_uri(input)
        exact_results = [i for i in session().query(InfoBoxInstance).filter_by(literal=literal, language_code=language_code)]
        if not exact_results:
            match_result = get_rec_match(session(), literal)
            if match_result:
                exact_results = [i for i in session().query(InfoBoxInstance).filter_by(id=match_result[0])]
        return exact_results
    except Exception as ex:
        print("infobox:get_rec:", ex)
        traceback.print_exc(file=sys.stdout)
        return None


def update_concept(literal: str, language: str, concept: str, vps: dict, commit=True):
    language_code = config.get_language_code(language)
    item = session().query(InfoBoxInstance).filter_by(literal=literal, language_code=language_code, concept=concept).first()
    attributes = json.dumps(vps) if vps else None
    if item:
        setattr(item, "attributes", attributes)
        if commit:
            session().commit()
    else:
        item = InfoBoxInstance(literal=literal, language_code=language_code, concept=concept, attributes=attributes)
        session().add(item)
        if commit:
            session().commit()
    return item


def delete_info(input: str, language: str):
    language_code = config.get_language_code(language)
    literal = input
    if input == "all":
        session().query(InfoBoxInstance).delete()
        session().query(InfoBoxData).delete()
    elif input == "data":
        session().query(InfoBoxData).delete()
    elif input == "instance":
        session().query(InfoBoxInstance).delete()
    else:
        session().query(InfoBoxData).filter(InfoBoxData.literal == literal, InfoBoxData.language_code == language_code).delete()
        session().query(InfoBoxInstance).filter(InfoBoxInstance.literal == literal, InfoBoxInstance.language_code == language_code).delete()
    session().commit()


def get_recid(recid):
    try:
        return [i for i in session().query(InfoBoxInstance).filter_by(id=recid)]
    except Exception as ex:
        print("infobox:get_rec:", ex)
        traceback.print_exc(file=sys.stdout)
        return None


def add_search_literal(input, recid):
    """add a index to alternative literal in the fulltext index MATCH"""
    statement = f"""INSERT INTO InfoBoxInstance_idx (rowid, literal) VALUES ("{recid}", "{input}");"""
    query = session().execute(text(statement))
    session().flush()
    session().commit()


def find_search_literal(input, language="en"):

    statement = f"""SELECT rowid FROM InfoBoxInstance_idx WHERE literal MATCH "{input}";"""
    # print(statement)
    query = session().execute(text(statement))
    items = []
    for rec_id in query.all():
        item = get_recid(rec_id[0])
        items.append(item[0])
    return items
