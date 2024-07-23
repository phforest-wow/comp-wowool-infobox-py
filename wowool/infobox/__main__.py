#!/usr/bin/python3
import argparse
import sys
from .infobox import add_argument_parser, infobox_main


def parse_arguments(argv):
    """
    This EyeOnText Infobox tool ,
    """
    parser = argparse.ArgumentParser(prog="infobox", description=parse_arguments.__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    return add_argument_parser(parser).parse_args(argv)


def main():

    args = dict(parse_arguments(sys.argv[1:])._get_kwargs())
    infobox_main(**args)


if __name__ == "__main__":
    main()
