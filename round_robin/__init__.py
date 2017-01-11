# -*- coding: utf-8 -*-
import abc
import datetime

# Helper functions to convert between timestamps and datetimeobjects
timestamp_to_time = lambda t: datetime.datetime.fromtimestamp(t)
time_to_timestamp = lambda t: int(t.strftime('%s'))

class RoundRobinDb(object):
    """ An abstract RoundRobinDb that can be implemented with a backing subclass.

    Subclasses implement the usual CRUD methods to support writing and reading data
    from any backing format (e.g. SQLite, JSON, RDBMS, etc.)
    """
    # Since this is an abstract base class, use ABCMeta to have useful abstract decorators
    __metaclass__ = abc.ABCMeta
    
    @abc.abstractmethod
    def read_all(self, table):
        return NotImplemented

    @abc.abstractproperty
    def last_timestamp(self):
        """Read-only property - get the last-saved timestamp in the database."""
        return NotImplemented

    @abc.abstractmethod
    def get_timestamp_index(self, timestamp, table):
        """Find the index in the RRD for the specified timestamp."""
        assert (table.lower() == "minutes" or table.lower() == "Hours"), \
                "Table name must be 'Minutes' or 'Hours'"

    @property
    def minutes(self):
        return self.read_all('minutes')

    @property
    def hours(self):
        return self.read_all('hours')

    def add_timestamp(self, timestamp, value):
        # TODO find latest timestamp, and the difference between it and the new timestamp
        # and then create a list of intermediary timestamps with `None` values
        pass

    def query(self, table):
        values = getattr(self,table) # `table` must be 'hours' or 'minutes'

        # Get the index of the last entered timestamp in the respective table.
        ## The next position in the round-robin database will be the oldest.
        last_entry_index = self.get_timestamp_index(self.last_timestamp, table)

        # Our entries are in order, but the last_entry should be at the end
        # of the list so join two splices of the list around the last_entry
        return values[(last_entry_index + 1):] + values[:(last_entry_index+1)]

    def save(self, timestamp, value):
        print("Called RoundRobinDB")




def open_database(backing):
    """ Open a connection to a Round Robin Database.

    Keyword arguments:
    backing --  A tuple of the format (engine, uri)
                Presently only the `SQLite` engine is supported.
                The uri is the argument passed to `sqlite3.connect()`.

    Returns:
    A RoundRobinDb object.
    """
    engine,db_path = backing
    assert (engine == "SQLite"),"Only SQLite backing is supported."

    # We only support SQLite3 at the moment, defined in the db.py module
    from . import db
    return db.SqliteRoundRobinDb(db_path)
