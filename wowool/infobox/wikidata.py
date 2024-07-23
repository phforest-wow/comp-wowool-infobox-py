import argparse
import sys
import json
import re
import requests
import re
import json
from pathlib import Path
import traceback
from wowool.infobox.session import session
from wowool.infobox.session import InfoBoxInstance, InfoBoxData

from wowool.infobox.utilities import update_concept, convert_args, add_search_literal
from wowool.string import *

# TODO: Notes:
# ------------------------------------------------------
# - There is only one keep that one even if it does not have any links.
# - Find longest matching part of the original lookup string.
# -------------------------------------------------------


REDIRECTION_PATTERN = re.compile(r"#REDIRECT *\[\[(.*?)\]\].*")

# 92 statements, 42 sitelinks - 04:33, 21 January 2021
SEARCHRESULT_PATTERN = re.compile(r"([0-9]+) statements?, ([0-9]+) sitelinks?.*")

stripper = str.maketrans("\u200e\xa0", "  ")

SOURCE = "wikidata"


def get_rec_wikidata(literal, language_code):
    assert session(), "session has not been initiailized"
    return session().query(InfoBoxData).filter_by(literal=literal, language_code=language_code, source=SOURCE).first()


def get_infobox_wikidata(input, language="english", redirect=None):
    from bs4 import BeautifulSoup

    try:
        literal, encoded_literal, language_code, language = convert_args(input, language)
        item = get_rec_wikidata(literal, language_code)
        if item:
            return item
        else:
            url = f"https://www.wikidata.org/w/index.php?search={encoded_literal}&title=Special:Search&go=Go&ns0=1&ns120=1"
            data = requests.get(url).text
            soup = BeautifulSoup(str(data), "html5lib")
            lone_wolf_hit = []
            hits = {}
            for sr in soup.find_all("li", "mw-search-result"):
                hit = {}

                _link = sr.find("a", href=True)
                href = _link["href"]
                hit["href"] = href
                title = _link["title"].translate(stripper).strip()
                hit["title"] = title

                for _srd in sr.find_all("div", "mw-search-result-data"):
                    m = SEARCHRESULT_PATTERN.match(_srd.text)
                    if m:
                        hit["sitelinks"] = int(m.group(2))

                for _language in sr.find_all("sup", "wb-language-fallback-indicator"):
                    hit["language"] = _language.text.lower()

                for _description in sr.find_all("span", "wb-itemlink-description"):
                    if "description" not in hit:
                        hit["description"] = _description.text.translate(stripper)
                    else:
                        hit["description"] += ". " + _description.text.translate(stripper)

                if "sitelinks" in hit and hit["sitelinks"] >= 2:
                    canonical, *reat = hit["title"].split("|")
                    canonical = canonical.strip()
                    if canonical.endswith("Inc."):
                        canonical = canonical[:-5].strip()
                    hit["canonical"] = canonical
                    hits[href] = hit
                elif not lone_wolf_hit:
                    canonical, *reat = hit["title"].split("|")
                    canonical = canonical[0].strip()
                    if canonical.endswith("Inc."):
                        canonical = canonical[:-5].strip()
                    hit["canonical"] = canonical
                    lone_wolf_hit = [hit]

            hits = hits.values()

            # 1. if we only have one hit , then it's easy, we will use that one.
            if len(hits) == 1:
                hit = hits[0]
                if hit["canonical"] == input:
                    del hit["canonical"]
            # 2. if we have more hits then let first sort them on the number of sitelinks
            #    As we sort the results on the sitelinks AND we filter on if they have
            #    the same pattern in the string. (Example 'Apple Inc' is still a valid candidate for Apple ).
            #    with what's left we will see how much the first and the second hit differ
            #    from each other. If we defer more then 0.1 (TBD) then we take the first one.
            #    This mean that we are quite sure we have the correct one.
            elif len(hits) > 1:
                import re

                matcher = re.compile(literal, re.I)

                hits_links = []
                for hit in hits:
                    m = matcher.search(hit["canonical"])
                    if m:
                        hits_links.append([hit["sitelinks"], hit])
                    elif hit["description"].startswith(literal):
                        hits_links.append([hit["sitelinks"], hit])

                # print(hits_links)

                if len(hits_links) > 1:
                    hits_links.sort(key=lambda x: x[0], reverse=True)
                    per_rel = hits_links[1][0] / hits_links[0][0]
                    if per_rel < 0.1:
                        # we are pretty sure we have the most commen one.
                        hits = [hits_links[0][1]]
                    else:
                        # we only have one that have a exact match.
                        hits = [hit for hit in hits if hit["canonical"] == literal]
                        if hits_links[0][0] > 20:
                            hits = [hits_links[0][1]]
                        else:
                            hits = []
                elif len(hits_links) == 1:
                    # 3. we only have one the has the correct casing.
                    hits = [hits_links[0][1]]

            elif lone_wolf_hit:
                hits = [lone_wolf_hit[0]]

            if len(hits):
                hits.sort(key=lambda x: x["sitelinks"], reverse=True)
                item = InfoBoxData(literal=literal, language_code=language_code, source=SOURCE, json_string=json.dumps(hits))
                session().add(item)
                session().commit()
                return item
    except Exception as ex:
        print("infobox:wikidata", ex)
        traceback.print_exc(file=sys.stdout)

    return None


def get_page_wikidata(item, href, language="english"):
    from bs4 import BeautifulSoup

    fn = Path(f"href:{href}.txt".replace("/", "_"))
    if fn.exists():
        with open(fn) as fh:
            data = fh.read()
    else:
        url = f"https://www.wikidata.org{href}"
        data = requests.get(url).text
        with open(fn, "w") as fh:
            fh.write(data)

    soup = BeautifulSoup(str(data), "html5lib")
    attributes = json.loads(item.attributes)
    for alias in soup.find_all("li", "wikibase-entitytermsview-aliases-alias"):
        alias_rec = update_concept(alias.text, language, item.concept, attributes)
        print(alias_rec)


def is_descriptor(concept):
    return concept.uri == "Descriptor"


def is_others(concept):
    return concept.uri in set(["Weapon"])


def add_concept_to_database(hit, language, literal, concept, concept_type):

    attributes = concept.attributes
    del attributes["type"]
    attributes["descriptor"] = concept.stem
    attributes["source"] = SOURCE

    if "language" in hit:
        if hit["language"] == language:
            del hit["language"]

    for key, value in hit.items():
        attributes[key] = value
    # print("concept_type:", concept_type, attributes)
    # TODO : update all re records in one commit.
    original_literal = literal
    literal = attributes["canonical"] if "canonical" in attributes else literal

    item = update_concept(literal, language, concept_type, attributes)
    # if verbose:
    #     print(f"UPDATE:{item.concept}@({item.attributes}) -> {item.literal}")
    if original_literal != literal:
        add_search_literal(original_literal, item.id)


def wikidata_discover(input, language, verbose=True):
    literal, encoded_literal, language_code, language = convert_args(input, language)
    item = get_rec_wikidata(literal, language_code)
    # print("wikidata_discover:", literal)
    if not item:
        get_infobox_wikidata(input, language)

    item = get_rec_wikidata(literal, language_code)
    if item:
        from wowool.annotation import Concept

        # from eot.wowool.plugin.wowool_plugin import process as discovery
        import wowool.package.lib.wowool_plugin as wowool_plugin

        from wowool.infobox.process.process import process as mp_process

        hits = json.loads(item.json_string)
        collected = []
        for hit in hits:
            if len(collected) >= 2:
                break

            data = hit["title"] + ". " + hit["description"]
            description = hit["description"]

            if verbose:
                print("infobox:data:", data, hit)

            del hit["title"]
            del hit["description"]
            del hit["sitelinks"]

            doc = mp_process("english,discovery", data)
            if verbose:
                print(doc)
            found = False

            for concept in Concept.iter(doc, is_descriptor):
                if "type" in concept.attributes:
                    found = True
                    concept_type = camelize(concept.attributes["type"][0])
                    add_concept_to_database(hit, language, literal, concept, concept_type)
                    collected.append([concept.attributes["href"], item])

            if not found:
                doc = mp_process("english,discovery", description)
                for sentence in doc:
                    for concept in Concept.iter(sentence, is_others):
                        if (
                            "type" in concept.attributes
                            and concept.begin_offset == sentence.begin_offset
                            and concept.end_offset == sentence.end_offset
                        ):
                            found = True
                            add_concept_to_database(hit, language, literal, concept, concept.uri)
                            collected.append([concept.attributes["href"], item])

            # if not found:
            #     item = update_concept(literal, language, None, None)
        return collected
    else:

        item = update_concept(literal, language, None, None)
        # for href, item in collected:
        #     get_page_wikidata(item, href, language)
        #     break
