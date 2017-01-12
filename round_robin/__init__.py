# -*- coding: utf-8 -*-
import abc
import datetime
import sys

# Python 2/3 compatible `range` function
if sys.version_info < (3, 0):
    range_func = xrange
else:
    range_func = range

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
    def get_timestamp_index(self, timestamp, table, default=None):
        """Find the index in the RRD for the specified timestamp."""
        assert (table.lower() == "minutes" or table.lower() == "hours"), \
                "Table name must be 'Minutes' or 'Hours'"

    @abc.abstractmethod
    def get_timestamp_value(self, timestamp):
        """Retrieve the value for the specified timestamp.

        Returns `None` if the timestamp is not in the database."""
        return NotImplemented

    @abc.abstractmethod
    def add_timestamps(self, data):
        """Add timestamps and values to the database.

        Subclasses should override this method to use their own storage.
        
        Keyword Arguments:
        data -- a list of tuples of format [(timestamp, value),..] in ascending timestamp order
        """
        return NotImplemented

    @abc.abstractmethod
    def update_timestamp(self, timestamp, data):
        """Update the RRD's timestamp entry.

        Throws ValueError if the timestamp is not found in the database.
        """
        return NotImplemented

    @property
    def last_hour_timestamp(self):
        # Get the timestamp corresponding to the hour-mark of the latest timestamp
        # If the last timestamp is `None` (new database), do not try to find the hour
        return None if not self.last_timestamp else time_to_timestamp(
                timestamp_to_time(self.last_timestamp)
                .replace(minute=0, second=0, microsecond=0)
            )

    @property
    def minutes(self):
        values = self.read_all('minutes')
        # Get the index of the last entered timestamp in the respective table.
        # The next position in the round-robin database will be the oldest.
        last_entry_index = self.get_timestamp_index(self.last_timestamp,
                                                    'minutes', default=0)

        # Our entries are in order, but the last_entry should be at the end
        # of the list so join two splices of the list around the last_entry
        return values[(last_entry_index + 1):] + values[:(last_entry_index+1)]

    @property
    def hours(self):
        values = self.read_all('hours')
        # Reorder as in the minutes() property, but looking at the hours values
        last_entry_index = self.get_timestamp_index(self.last_hour_timestamp,
                                                    'hours', default=0)
        return values[(last_entry_index + 1):] + values[:(last_entry_index+1)]

    def query(self, table):
        return getattr(self,table) # `table` must be 'hours' or 'minutes'

    def save(self, timestamp, value):
        # First let's truncate our timestamp to the nearest "minute" value, as 
        # noted in the `Design Consideration` section of the README
        minute_ts = time_to_timestamp(timestamp_to_time(timestamp)
                                     .replace(second=0, microsecond=0))

        print("minute_ts for this timestamp is set to %d" % minute_ts)
        if minute_ts == self.last_timestamp:
            # This is essentially an update to the recently-added value. Technically
            # it's allowed according to the Design Considerations, but is probably
            # not what the user wants (if they're calling the ``rrd save`` command
            # more than once per minute, this situation may occur). 
            print("Warning: updating existing timestamp with new value!")
            # Update record to be the mean of this value and the saved value
            self.update_timestamp(minute_ts, 
                                  (self.get_timestamp_value(minute_ts)+value)/2)
        else:
            # Find out how many seconds have elapsed since the previous entry, and create
            # a list of up to 59 previous entries with `None` values to correspond to
            # entries that have been missed
            elapsed_secs = minute_ts - self.last_timestamp
            elapsed_values = [(minute_ts - t, None) for t in 
                    range_func(60, min(elapsed_secs,60**2), 60)]
            # The above list comprehension returns the values in descending order, so 
            # let's reverse the list
            elapsed_values.reverse()

            # Add our elapsed values and the latest value (list size is max 60)
            self.add_timestamps(elapsed_values + [(minute_ts, value)])

        


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
