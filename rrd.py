# -*- coding: utf-8 -*-
from __future__ import print_function

import os
import argparse
import round_robin

class Rrdtool(object):
    """An object that holds a reference to the round-robin database.

    This object represents our program. It configures the `round_robin`
    module to manage the backing data store (SQLite3 database) with our
    specific parameters - a minute database with 60 entries and an hour
    database with 24 entries.
    """
    def __init__(self):
        # User can pass in the database they want to use as an environment var
        # or it defaults to rrd-data in the current working directory
        db_file = os.getenv('RRD_DATABASE', os.path.join(os.getcwd(),'rrd-data.db'))
        rrd_backing = ('SQLite', db_file)
        self.rrd = round_robin.open_database(rrd_backing)

    def query(self, db):
        """Query the specified RRD and output all values and a summary."""
        print("Query called for the %s database" % db)
        values = self.rrd.query(db) 
        print("Values are %r" % values)

    def save(self, timestamp, value):
        """Save the specified value in the RRD at the given timestamp."""
        print("Save called - store %f at %d timestamp" % (value, timestamp))

    def close_db(self):
        """Close connection to the database, if necessary."""
        self.rrd = None

# Set up a parser for command-line arguments, store the command given
parser = argparse.ArgumentParser(description="Save and query data in an RRD", 
                                 add_help=False)
subparsers = parser.add_subparsers(dest="command")

# Create a parser for "save"
save_parser = subparsers.add_parser("save", add_help=False)
save_parser.add_argument("timestamp", type=int)
save_parser.add_argument("value", type=float)

# Create a parser for "query"
query_parser = subparsers.add_parser("query", add_help=False)
query_parser.add_argument("db", choices=["minutes","hours"])


# Parse arguments and call the respective function for the command given
args = parser.parse_args()

# Create our Rrdtool object
rrdtool = Rrdtool()

if args.command == "query":
    rrdtool.query(args.db)
elif args.command == "save":
    rrdtool.save(args.timestamp, args.value)

rrdtool.close_db()
