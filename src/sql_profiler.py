from hashlib import sha1
from django.db import connection


class ParseSql:

    def __init__(self, sql_statements):
        self.sql_statements = sql_statements
        self.sql_statements.sort(key=lambda x: float(x['time']))
        self.analyzed = [self.analyze_query(q) for q in self.sql_statements]
        self.sql = self._deduplicate_sql_expressions()
        self.top_ten = self.sql[-10:]

    @staticmethod
    def analyze_query(query):
        sql = query['sql']
        num_joins = sql.count('JOIN')
        return {
            'num_joins': num_joins,
            'time': float(query['time']),
            'query': sql,
            'hash': sha1(sql.encode()).hexdigest(),
        }

    def _deduplicate_sql_expressions(self):
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


def profile_sql(func, *args, **kwargs):
    """
    Executes `func` and returns SQL analysis
    """
    starting_connections_num = len(connection.queries)

    func(*args, **kwargs)

    ending_connections = list(connection.queries)
    return ParseSql(ending_connections[starting_connections_num:])
