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
# Helper function to get base minute and hour from second-level timestamps
timestamp_minute = lambda t: time_to_timestamp(timestamp_to_time(t)
                        .replace(second=0, microsecond=0))
timestamp_hour = lambda t: time_to_timestamp(timestamp_to_time(t)
                        .replace(minute=0, second=0, microsecond=0))

class RoundRobinDb(object):
    """ An abstract RoundRobinDb that can be implemented with a backing subclass.

    Subclasses implement the usual CRUD methods to support writing and reading data
    from any backing format (e.g. SQLite, JSON, RDBMS, etc.)
    """
    # Since this is an abstract base class, use ABCMeta to have useful abstract decorators
    __metaclass__ = abc.ABCMeta
    
    def _validate_tablename(self, name):
        """A shared method to validate the tablename is either `Minutes` or `Hours`."""
        assert(name.lower() == "minutes" or name.lower() == "hours"), \
                "Table name must be 'Minutes' or 'Hours'"

    @abc.abstractmethod
    def read_all(self, table):
        """Read all values from the specified table.

        Keyword arguments:
        table -- One of 'minutes' or 'hours'

        Returns:
        a list of (timestamp, value) tuples representing the state of the table
        ordered by timestamp value ascending."""
        # This must be implemented by a subclass.
        return NotImplemented

    @abc.abstractproperty
    def last_timestamp(self):
        """Read-only property - the last-saved timestamp in the database."""
        return NotImplemented

    @abc.abstractmethod
    def get_timestamp_index(self, timestamp, table, default=None):
        """Find the index in the RRD for the specified timestamp."""
        # Do some basic input validation (prevent SQL injection)
        self._validate_tablename(table)

    @abc.abstractmethod
    def get_timestamp_value(self, table, timestamp):
        """Retrieve the value for the specified timestamp.

        Returns `None` if the timestamp is not in the database."""
        self._validate_tablename(table)

    @abc.abstractmethod
    def save_timestamps(self, data):
        """Add timestamps and values to the database.

        Subclasses should override this method to use their own storage.
        
        Keyword Arguments:
        data -- a dictionary with two entries, 'minutes' and 'hours'
                each entry's value is a list of tuples of format 
                [(timestamp, value),..] in ascending timestamp order
        """
        return NotImplemented

    @abc.abstractmethod
    def update_timestamp(self, table, timestamp, data):
        """Update the RRD's timestamp entry in the specified table.

        Throws ValueError if the timestamp is not found.
        """
        self._validate_tablename(table)

    @property
    def last_hour_timestamp(self):
        """The most recent timestamp in the `hour` table."""
        # Get the timestamp corresponding to the hour-mark of the latest timestamp
        # If the last timestamp is `None` (new database), do not try to find the hour
        if self.last_timestamp is None:
            return None
        return timestamp_hour(self.last_timestamp)

    @property
    def minutes(self):
        """An ordered list of all `minutes` entries in our RRD."""
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
        """An ordered list of all `hours` entries in our RRD."""
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
        minute_ts = timestamp_minute(timestamp)
        hour_ts = timestamp_hour(timestamp)

        if self.last_timestamp is not None and minute_ts < self.last_timestamp:
            raise ValueError("Timestamp must be greater than %s" % self.last_timestamp)
        elif minute_ts == self.last_timestamp:
            # This is essentially an update to the recently-added value. Technically
            # it's allowed according to the Design Considerations, but is probably
            # not what the user wants (if they're calling the ``rrd save`` command
            # more than once per minute, this situation may occur). 
            print("Warning: updating existing timestamp with new value!")
            # Update record to be the min of this value and the saved value
            self.update_timestamp('Minutes', minute_ts, min(
                        self.get_timestamp_value('Minutes', minute_ts), value))
            self.update_timestamp('Hours', hour_ts, min(
                        self.get_timestamp_value('Hours', hour_ts), value))
        else:
            # Data object to be passed to the backing storage implementation
            data={}
            
            # Find out how many seconds have elapsed since the previous entry, and create
            # a list of up to 59 previous entries with `None` values to correspond to
            # entries that have been missed
            if self.last_timestamp:
                elapsed_secs = minute_ts - self.last_timestamp
            else:
                # New database
                elapsed_secs = 0

            elapsed_values = [(minute_ts - t, None) for t in 
                    range_func(60, min(elapsed_secs,60**2), 60)]
            # The above list comprehension returns the values in descending order, so 
            # let's reverse the list
            elapsed_values.reverse()

            # Add our elapsed values and the latest value (list size is max 60)
            data['minutes'] = elapsed_values + [(minute_ts, value)]

            # If current timestamp's hour > last_saved_hour,
            # add a new hour entry with value set to value being saved.
            # if there are intermediate hourly values, create those too with
            # `None` value. If there are none, then update the hour value to
            # be the minimum of its current value and the new value.

            if self.last_hour_timestamp is not None:
                elapsed_secs = minute_ts - self.last_hour_timestamp
            else:
                # New database
                elapsed_secs = 0

            elapsed_hour_values = [(hour_ts - t, None) for t in
                    range_func(60**2, min(elapsed_secs, 24*60**2), 60**2)]
            elapsed_hour_values.reverse()

            if hour_ts == self.last_hour_timestamp:
                # just update the hours value without adding new hours.
                # We don't need to read from the DB, because we can only add
                # one timestamp at a time and each save will update this value
                # as appropriate.
                self.update_timestamp('Hours', hour_ts, min(
                        self.get_timestamp_value('Hours', hour_ts), value))
                data['hours'] = []
            else:
                data['hours'] = elapsed_hour_values + [(hour_ts, value)]
            self.save_timestamps(data)

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
