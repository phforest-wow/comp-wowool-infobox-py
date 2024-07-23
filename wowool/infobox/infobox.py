import argparse
import sys
import json
import re
import requests
import re
from pathlib import Path


from wowool.infobox.session import session, init_database

from wowool.infobox.session import InfoBoxInstance, InfoBoxData
from wowool.infobox.wikidata import wikidata_discover, get_infobox_wikidata
from wowool.infobox.wikipedia import get_infobox_wikipedia, wikipedia_discover, get_infobox_attributes
from wowool.infobox.utilities import get_language_code
from wowool.infobox.utilities import delete_info, update_concept, get_rec, add_search_literal, find_search_literal
from wowool.diagnostic import Diagnostics, Diagnostic, DiagnosticType
from wowool.document import Document
from wowool.native.core import Domain
from wowool.native.core.engine import Engine
from wowool.utility.apps.decorators import (
    exceptions_to_diagnostics,
    requires_analysis,
)

from wowool.native.core.analysis import get_internal_concept


def main_match(input, verbose=False):
    records = find_search_literal(input=input)
    if records:
        for record in records:
            print(json.dumps(json.loads(str(record)), indent=2))
    else:
        print("Not Found!")


def discover(input, language, source="wikidata", verbose=False):
    if verbose:
        print(f"infobox.discover: {input}")
    if source == "wikidata":
        wikidata_discover(input, language, verbose)
    else:
        wikipedia_discover(input, language, verbose)
    if verbose:
        main_match(input)


def main_discover(language, input, source, verbose):
    discover(input, language, source=source, verbose=verbose)


def main_list_instances(where=None, verbose=False):
    if where:
        results = session().execute(f"""SELECT id,concept,literal FROM InfoBoxInstance WHERE {where} ;""")
    else:
        results = session().query(InfoBoxInstance)
    for item in results:
        print(f"{item.id}, {item.concept}, {item.literal}")


def get_info(input, key=None, language="english", redirect=None):
    return get_rec(input, language, redirect)


def main_add_instances(language, literal, concept, verbose):
    add_infobox_instance(language, literal, concept)
    if verbose:
        for item in get_info(literal, language):
            print(f"{item.id}, {item.concept}, {item.literal}")


def main_add_search_literal(literal, recid):
    add_search_literal(literal, recid)


DEFAULT_ATTRIBUTES = ["occupation", "political_party"]


def main_attributes(literal, verbose=False):
    item = get_rec(literal)
    if not item:
        attributes = get_infobox_attributes(literal, DEFAULT_ATTRIBUTES)
    else:
        attributes = json.loads(item[0].attributes)
    if attributes:
        print(*attributes.items(), sep="\n")


def add_argument_parser(parser):
    subparsers = parser.add_subparsers()

    parser_match = subparsers.add_parser("match", help="matchs a new index, usage: match [literal] " "")
    parser_match.add_argument("input", type=str, help="entry to search for in the fulltext index")
    parser_match.set_defaults(function=main_match)

    parser_discover = subparsers.add_parser(
        "discover",
        help="discovers the given and update the Instance and the Data, usage: discover [language_code] [literal] [wikidata/wikipedia]",
        usage="""infobox discover [language_code] [literal] [source]\n   ex: infobox discover en "Rafael Nadal" wikidata""",
    )
    parser_discover.add_argument("language", type=str, help="language")
    parser_discover.add_argument("input", type=str, help="input literal")
    parser_discover.add_argument("source", type=str, help="wikidata")
    parser_discover.set_defaults(function=main_discover)

    parser_list = subparsers.add_parser("list", help="list all the entries, usage: list", usage="""infobox list [where]""")
    parser_list.add_argument("where", type=str, help="where clause", nargs="?")
    parser_list.set_defaults(function=main_list_instances)

    parser_add = subparsers.add_parser(
        "add",
        help="add a entry, usage add [language_code] [literal] [concept]",
        usage="""infobox add [language_code] [literal] [concept]\n   ex: infobox add en "Rafael Nadal" Player""",
    )
    parser_add.add_argument("language", type=str, help="language")
    parser_add.add_argument("literal", type=str, help="literal of the concept ")
    parser_add.add_argument("concept", type=str, help="concept uri")
    parser_add.set_defaults(function=main_add_instances)

    parser_add_id = subparsers.add_parser(
        "add_id",
        help="add a search id to the fultext, infobox add_id [literal] [recid]",
        usage="""infobox add_id [literal] [recid]\n   ex: infobox add_id "Rafa" 1""",
    )
    parser_add_id.add_argument("literal", type=str, help="literal of the recid ")
    parser_add_id.add_argument("recid", type=str, help="recid in the database of the ")
    parser_add_id.set_defaults(function=main_add_search_literal)

    subparser = subparsers.add_parser(
        "attributes",
        help="attributes infobox attributes [literal]",
        usage="""infobox add_id [literal] \n   ex: infobox attributes "Kamala Harris" """,
    )
    subparser.add_argument("literal", type=str, help="literal to find")
    subparser.set_defaults(function=main_attributes)

    parser.add_argument("--verbose", help="verbose output", default=False, action="store_true")
    return parser


def get_infobox_data(source, input, language):
    if source == "wikidata":
        item = get_infobox_wikidata(input, language)
        hits = json.loads(item.json_string)
        if hits:
            return item
        else:
            return get_infobox_wikipedia(input, language)
    else:
        return get_infobox_wikipedia(input, language)


def add_infobox_instance(language, input, concept, vps={}):
    update_concept(input, language, concept, vps)


def clean_up(kwargs):
    keys = [k for k in kwargs]
    for key in keys:
        if not kwargs[key]:
            del kwargs[key]


def expand_keys(kwargs):
    if "key" in kwargs:
        if kwargs["key"] == "sd":
            kwargs["key"] = "short description"
        if kwargs["key"] == "ad":
            kwargs["key"] = "ambiguous description"
        if kwargs["key"] == "c":
            kwargs["key"] = "concept"


def is_descriptor(concept):
    return concept.uri == "Descriptor"


def infobox_main(**kwargs):

    dbname = kwargs["database"] if "database" in kwargs else None
    init_database(dbname)
    action = kwargs["function"]
    del kwargs["function"]
    action(**kwargs)
    exit(0)


class Infobox:
    def __init__(self, engine: Engine = None):
        """
        Initialize the Snippet application

        :param source: The Wowool source code
        :param source: str
        """
        pass

    @exceptions_to_diagnostics
    @requires_analysis
    def __call__(self, document: Document, diagnostics: Diagnostics) -> Document:
        """
        :param document: The document to be processed and enriched with the annotations from the snippet
        :type document: Document

        :returns: The given document with the new annotations. See the :ref:`JSON format <json_apps_snippet>`
        """
        for concept in document.analysis.concepts():
            if concept.uri == "Person":
                literal = concept.canonical
                item = get_rec(literal)
                if not item:
                    attributes = get_infobox_attributes(literal, DEFAULT_ATTRIBUTES)
                else:
                    attributes = json.loads(item[0].attributes)
                if attributes:
                    internal_concept = get_internal_concept(document.analysis, concept)
                    if internal_concept:
                        for key, values in attributes.items():
                            if isinstance(values, list):
                                for value in values:
                                    internal_concept.add_attribute(key, value)
                            else:
                                internal_concept.add_attribute(key, str(values))
                        document.analysis.reset()

        return document
