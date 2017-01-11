# -*- coding: utf-8 -*-
import sqlite3
from . import RoundRobinDb

"""The interface between the python logic and the SQLite database.

The database structure is as follows:

TABLE Meta:
    name   value
    
TABLE Minutes:
    id   timestamp   value

TABLE Hours:
    id   timestamp   value

Meta table stores persistent state information (last timestamp entered, etc)
The Minutes and Hours tables store the data. This class ensures that only 
the first 60 entries in Minutes and 24 in Hours are actually used.
"""
class SqliteRoundRobinDb(RoundRobinDb):
    """Creates and manages connection to the SQLite database.

    Arguments:
        sqlite_db   A string filename to the database file
    
    Throws:
        SQLite.Error    If database initialization fails

    """
    def __init__(self, sqlite_db):
        self.connection = sqlite3.connect(sqlite_db)
        
        # Check if the database has been initialized, and create it if not
        self._check_and_init_db()

    # Our class can cause exceptions, so provide __enter__ and __exit__ methods
    # to allow Python to automatically clean up after itself when exceptions occur.
    # We can use the `with` keyword to avoid having to catch exceptions and close
    # the connection explicitly.
    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        if self.connection:
            self.connection.close()        

    def _check_and_init_db(self):
        """ Check if database is initalized, and initializes if not.

        Throws SQLite.Error if database initialization fails.
        """
        cur = self.connection.cursor()
        initialized = True
        try:
            cur.execute("SELECT * FROM Meta;")
        except sqlite3.OperationalError:
            initialized = False

        if initialized and len(cur.fetchall()) > 0:
            # assume that since we didn't error, and have some Meta items 
            # configured, our database is in a sane format
            return
        
        # Set up our empty RRD database
        cur.executescript("""
            DROP TABLE IF EXISTS Meta;
            DROP TABLE IF EXISTS Minutes;
            DROP TABLE IF EXISTS Hours;
            CREATE TABLE Meta(Name TEXT PRIMARY KEY, Value TEXT);
            CREATE TABLE Minutes(Id INTEGER PRIMARY KEY, Timestamp INTEGER, Value REAL);
            CREATE TABLE Hours(Id INTEGER PRIMARY KEY, Timestamp INTEGER, Value REAL);
            """)

        cur.execute("INSERT INTO Meta VALUES('last_timestamp',?);",(None,))

        # Create empty entries for our RRD
        mins = [(i, None, None) for i in xrange(60)]
        hours = [(i, None, None) for i in xrange(24)]

        cur.executemany("INSERT INTO Minutes VALUES(?, ?, ?);", mins)
        cur.executemany("INSERT INTO Hours VALUES(?, ?, ?);", hours)
        
        self.connection.commit()

    def read_all(self, table):
        """Read all values from the specified table.

        Keyword arguments:
        table -- One of 'minutes' or 'hours'

        Returns:
        a list of (timestamp, value) tuples representing the state of the table
        ordered by timestamp value ascending."""
        # use the table parameter to query the appropriate table
        # simple sanity check to make sure the user isn't passing in garbage 
        if table.lower() == "minutes":
            tablename = "Minutes"
        elif table.lower() == "hours":
            tablename = "Hours"
        else:
            raise ValueError("Table must be one of 'hours' or 'minutes'")

        ## We could use SQL ORDER BY (which would be more efficient than Python
        ## sorting if we were only selecting a small subset of data and the db 
        ## was large), but since we want everything, it's easier to just do our 
        ## own array splicing in memory.

        cur = self.connection.cursor()
        cur.execute("SELECT timestamp, value FROM (SELECT * FROM "+tablename+
                    " ORDER BY id);") # discard the id value
        return cur.fetchall()

        

    @property
    def last_timestamp(self):
        if hasattr(self, '_last_timestamp') and self._last_timestamp is not None:
            return self._last_timestamp # use memoization to save a DB call

        cur = self.connection.cursor()
        cur.execute("SELECT name, value FROM Meta WHERE name='last_timestamp';")
        last_ts = cur.fetchone()[1]
        self._last_timestamp = last_ts

        return self._last_timestamp

    def get_timestamp_index(self, timestamp, table):
        """Resolve timestamp to database index."""
        # Call superclass for things like parameter sanitizing, etc.
        super(self.__class__, self).get_timestamp_index(timestamp, table)

        cur = self.connection.cursor()
        cur.execute("SELECT id FROM "+table+" WHERE timestamp=?;", (timestamp,))
        index = cur.fetchone()
        if index is None:
            # means this is a new database, so start storing at index 0.
            return 0
        else:
            return index[0]
