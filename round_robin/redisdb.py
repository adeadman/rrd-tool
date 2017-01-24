import redis
from . import RoundRobinDb, range_func

class RedisRoundRobinDb(RoundRobinDb):
    def __init__(self, redis_db):
        server,port=redis_db.split(":")
        port = int(port)
        self.db = redis.StrictRedis(host=server, port=port, db=0)
        self._init_db()

    def _init_db(self):
        if self.db.get("initialized"):
             return # already initialized
        for i in range_func(60):
             self.db.set('mins%d' % i, (None, None))
        for i in range_func(24):
             self.db.set('hours%d' % i, (None, None))

        self.db.set('initialized', True)
        

    def read_all(self, table):
        if table.lower() == 'minutes':
            return self.db.get('mins')
        elif table.lower() == 'hours':
            return self.db.get('hours')


    @property
    def last_timestamp(self):
        return None

    def get_timestamp_index(self, timestamp, table, default=None):
        return None

    def get_timestamp_value(self, timestamp, table):
        return None

    def update_timestamp(self, table, timestamp, value):
        pass

    def save_timestamps(self, timestamp, value):
        pass
