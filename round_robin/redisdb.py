import redis
from ast import literal_eval

from . import RoundRobinDb, range_func

class RedisRoundRobinDb(RoundRobinDb):
    def __init__(self, redis_db):
        server,port=redis_db.split(":")
        port = int(port)
        self.db = redis.StrictRedis(host=server, port=port, db=0)
        self._init_db()
        self._mins_cache = None
        self._hours_cache = None

    def _init_db(self):
        if self.db.get("initialized"):
            return # already initialized
        self.db.set("initialized", True)

    def _clear_db(self):
        for ix in range_func(24):
            self.db.delete('min%d' % ix)
            self.db.delete('hour%d' % ix)
        for ix in range_func(24,60):
            self.db.delete('min%d' % ix)

        self.db.delete('last_timestamp')
        self.db.delete('initialized')

    def _get_key_as_tuple(self, key):
        data = self.db.get(key)
        if data is None:
            return None
        return literal_eval(data)

    def _get_filtered_cache(self, base, length):
        cache = {ts:(ix, val) for ix,(ts, val) in filter(
            lambda i: i[1] is not None,
            [(d, self._get_key_as_tuple(base+str(d))) for d in range_func(length)])}
        return cache
        
    def _get_minutes(self):
        if self._mins_cache is None:
            self._mins_cache = self._get_filtered_cache('min', 60)
            print("Minutes cache is %r" % self._mins_cache)
        return self._mins_cache

    def _get_hours(self):
        if self._hours_cache is None:
            self._hours_cache = self._get_filtered_cache('hour', 24)
            print("Hours cache is %r" % self._hours_cache)
        return self._hours_cache

    def read_all(self, table):
        if table.lower() == 'minutes':
            data = self._get_minutes()
        elif table.lower() == 'hours':
            data = self._get_hours()
        else:
            raise ValueError("Table must be one of 'hours' or 'minutes'")
        return [(val[0],val[1][1]) for val in 
                        sorted(data.items(), key=lambda i: i[1][0])]


    @property
    def last_timestamp(self):
        ts = self.db.get("last_timestamp")
        return None if ts is None else int(ts)

    def get_timestamp_data(self, timestamp, table):
        if table.lower() == 'minutes':
            ts_data = self._get_minutes().get(timestamp)
        elif table.lower() == 'hours':
            ts_data = self._get_hours().get(timestamp)
        return ts_data

    def get_timestamp_index(self, timestamp, table, default=None):
        super(self.__class__, self).get_timestamp_index(timestamp, table)
        ts_data = self.get_timestamp_data(timestamp, table)
        if ts_data is None:
            return default
        else:
            return ts_data[0]

    def _update(self, table, index, timestamp, value):
        keybase = 'min' if table.lower() == 'minutes' else 'hour'
        self.db.set(keybase+str(index), (timestamp, value))

    def get_timestamp_value(self, table, timestamp):
        super(self.__class__, self).get_timestamp_value(table, timestamp)
        ts_data = self.get_timestamp_data(timestamp, table)
        return None if ts_data is None else ts_data[1]

    def update_timestamp(self, table, timestamp, value):
        super(self.__class__, self).update_timestamp(table, timestamp, value)
        ts_index = self.get_timestamp_index(timestamp, table)
        if ts_index is None:
            raise ValueError("Timestamp does not exist in the database.")
        else:
            self._update(table, ts_index, timestamp, value)

    def save_timestamps(self, data):
        start_index = self.get_timestamp_index(self.last_timestamp, 'Minutes', -1) + 1
        for ix in range_func(len(data['minutes'])):
            ts, value = data['minutes'][ix]
            self._update('minutes', (ix + start_index) % 60, ts, value)

        start_index = self.get_timestamp_index(self.last_hour_timestamp, 'hours', -1) + 1
        for ix in range_func(len(data['hours'])):
            ts, value = data['hours'][ix]
            self._update('hours', (ix + start_index) % 24, ts, value)

        self.db.set("last_timestamp", data['minutes'][-1][0])
        self._mins_cache = None
        self._hours_cache = None
