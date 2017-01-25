# -*- coding: utf-8 -*-
from __future__ import print_function, division

import os
import sys
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
        rrd_backing = os.getenv('RRD_DATABASE',
                ":/".join(["SQLite", os.path.join(os.getcwd(),'rrd-data.db')]))
        rrd_backing = rrd_backing.split(":/")
        self.rrd = round_robin.open_database(rrd_backing)

    def query(self, db):
        """Query the specified RRD and output all values and a summary."""
        entries = self.rrd.query(db) 
        count_values, total = 0,0
        smallest, largest = None, None
        for ts, value in entries:
            if ts is not None:
                if value is not None:
                    count_values += 1
                    total += value
                    if largest is None or value > largest:
                        largest = value
                    if smallest is None or value < smallest:
                        smallest = value

                    # Choosing 2 decimal place precision
                    print("%d, %.2f" %(ts, value))
                else:
                    print("%d, NULL" % ts)
        if count_values == 0:
            print("Database is empty. Please add some values.")
        else:
            print("%s: min: %r, avg: %.2f, max: %r" % (db, smallest,
                (total/count_values) if count_values > 0 else float('nan'), largest))

    def save(self, timestamp, value):
        """Save the specified value in the RRD at the given timestamp."""
        try:
            self.rrd.save(timestamp, value)
        except ValueError as e:
            print(str(e), file=sys.stderr)
            sys.exit(1) # Error

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
