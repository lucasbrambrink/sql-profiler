import re
from hashlib import sha1
from collections import defaultdict
from datetime import datetime

from django.db import connection


class ProfileSql:
    def __init__(self, function_name, sql_statements, execution_time):
        self.function_name = function_name
        self.sql_statements = sql_statements
        self.sql_statements.sort(key=lambda x: float(x['time']))
        self.analyzed = [self.analyze_query(q) for q in self.sql_statements]
        self.sql = self._deduplicate_sql_expressions()
        self.top_ten = self.sql[-10:]
        self.execution_time = execution_time

    def __str__(self):
        return f"{self.function_name}: {len(self.sql_statements)} ({self.execution_time}s)"

    def __repr__(self):
        return str(self)

    @classmethod
    def profile(cls, func, *args, **kwargs):
        """
        Executes `func` and returns SQL analysis
        """
        connection.force_debug_cursor = True
        starting_connections_num = len(connection.queries)
        start_time = datetime.now()
        func(*args, **kwargs)
        execution_time = datetime.now() - start_time
        ending_connections = list(connection.queries)
        return cls(
            function_name=func.__name__,
            sql_statements=ending_connections[starting_connections_num:],
            execution_time=execution_time.total_seconds(),
        )

    @staticmethod
    def analyze_query(query):
        """
        Cleans and analyzes a given SQL query
        """
        sql = query['sql']
        num_joins = sql.count('JOIN')
        query_stripped_ids = re.sub(r'_id" = \d+', '_id" = ?', sql)
        return {
            'num_joins': num_joins,
            'time': float(query['time']),
            'query': query_stripped_ids,
            'hash': sha1(query_stripped_ids.encode()).hexdigest(),
        }

    def pretty_print(self, query_locator=None):
        """
        Prints an easily copy and pasted expression
        """
        if query_locator:
            queries = getattr(self, query_locator)
        else:
            queries = self.top_ten

        for query in queries:
            print('--', '*' * 25)
            print('-- count', query['count'], 'individual_time', query['individual_time'], 'total_time',
                  query['total_time'], 'num_join', query['statement']['num_joins'])
            print(f"{query['statement']['query']};")

    def _deduplicate_sql_expressions(self):
        """
        Performs an aggregate analysis over the entire set of SQL expressions (repeats, etc)
        """
        results = defaultdict(int)
        statements = {}
        # count statements by hash
        for s in self.analyzed:
            results[s['hash']] += 1
            statements[s['hash']] = s
        profile = []
        for hash_id, count in results.items():
            statement = statements[hash_id]
            profile.append({
                'count': count,
                'individual_time': statement['time'],
                'total_time': count * statement['time'],
                'statement': statement,
            })
        profile.sort(key=lambda r: r['total_time'])
        return profile
