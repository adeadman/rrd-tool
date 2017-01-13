# -*- coding: utf-8 -*-
import sqlite3
from . import RoundRobinDb, range_func

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
            cur.execute("SELECT * FROM Hours LIMIT 1;")
        except sqlite3.OperationalError:
            initialized = False

        if initialized and len(cur.fetchall()) > 0:
            # assume that since we didn't error, and have some Meta items 
            # configured, our database is in a sane format
            return
        
        # Set up our empty RRD database
        cur.executescript("""
            DROP TABLE IF EXISTS Minutes;
            DROP TABLE IF EXISTS Hours;
            CREATE TABLE Minutes(Id INTEGER PRIMARY KEY, Timestamp INTEGER, Value REAL);
            CREATE TABLE Hours(Id INTEGER PRIMARY KEY, Timestamp INTEGER, Value REAL);
            """)

        # Create empty entries for our RRD
        mins = [(i, None, None) for i in xrange(60)]
        hours = [(i, None, None) for i in xrange(24)]

        cur.executemany("INSERT INTO Minutes VALUES(?, ?, ?);", mins)
        cur.executemany("INSERT INTO Hours VALUES(?, ?, ?);", hours)
        
        self.connection.commit()

    def _update_table_row(self, table, id, timestamp, value):
        cur = self.connection.cursor()
        cur.execute("UPDATE "+table+" SET Timestamp=?, Value=? WHERE Id=?;", 
                (timestamp, value, id))

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
        cur.execute("SELECT timestamp FROM Minutes ORDER BY timestamp DESC LIMIT 1;")
        last_ts = cur.fetchone()[0]
        self._last_timestamp = last_ts

        return self._last_timestamp

    def get_timestamp_index(self, timestamp, table, default=None):
        """Resolve timestamp to database index."""
        # Call superclass for things like parameter sanitizing, etc.
        super(self.__class__, self).get_timestamp_index(timestamp, table)

        cur = self.connection.cursor()
        cur.execute("SELECT id FROM "+table+" WHERE timestamp=?;", (timestamp,))
        index = cur.fetchone()
        if index is None:
            # means this is a new database, so start storing at index 0.
            return default
        else:
            return index[0]

    def get_timestamp_value(self, table, timestamp):
        """Query database table for value associated with specified timestamp."""
        cur = self.connection.cursor()
        # look up based on Id rather than timestamp, to get the input checking 
        # from get_timestamp_index()
        cur.execute("SELECT value FROM "+table+" WHERE Id=?;", (
                        self.get_timestamp_index(timestamp, table, default=None),))
        value = cur.fetchone()
        return value[0]

    def update_timestamp(self, table, timestamp, value):
        # Get the index of the specified timestamp. If it's not found, default to None
        ts_index = self.get_timestamp_index(timestamp, table)
        if ts_index is None:
            raise ValueError("Timestamp does not exist in the database.")
        else:
            self._update_table_row(table, ts_index, timestamp, value)
            self.connection.commit()

    def save_timestamps(self, data):
        # Update values in the `Minute` table
        start_index = self.get_timestamp_index(self.last_timestamp, 'Minutes', -1) + 1
        for ix in range_func(len(data['minutes'])):
            ts, value = data['minutes'][ix]
            self._update_table_row('Minutes', (ix + start_index) % 60, ts, value)
            

        # Update values in the `Hour` table TODO`
        start_index = self.get_timestamp_index(self.last_hour_timestamp, 'Hours', -1) + 1
        for ix in range_func(len(data['hours'])):
            ts, value = data['hours'][ix]
            self._update_table_row('Hours', (ix + start_index) % 60, ts, value)

        self.connection.commit()
