from IPython.utils.capture import capture_output
from decimal import Decimal
from hashlib import sha1
from collections import defaultdict


class SqlStatement:
    """
    Represents an executed SQL statement
    """
    SPACES = " " * 7  # replace tab-characters (long spaces)

    def __init__(self, statement, execution_time):
        self.statement = statement
        self.execution_time = execution_time
        self.clean_statement = "".join(line.replace(self.SPACES, " ") for line in statement)
        self.id = id(self)
        self.hash = sha1(self.clean_statement.encode()).hexdigest()

    def __repr__(self):
        return str(self)

    def __str__(self):
        return f"{self.execution_time}: {self.clean_statement}"


class ParseSQL:
    """
    Util class to analyze the STDOUT for a given function
    """
    EXECUTION_TIME_REGEX = "Execution time:"


    def __init__(self, stdout):
        self.stdout = stdout
        self.sql_statements = self._parse(stdout)
        self.sorted = sorted(self.sql_statements, key=lambda s: s.execution_time)
        self.duplicates = self._count_duplicates()

    def show_sql(self):
        for sql in self.sql_statements:
            print(sql)

    @staticmethod
    def _extract_execution_time(execution_time_str):
        digits = "".join(d for d in execution_time_str if d.isnumeric() or d == '.')
        return Decimal(digits)

    def _parse(self, stdout):
        sql_lines = stdout.splitlines()
        sql_statements = []
        current_sql_statement = []

        for line in sql_lines:
            if self.EXECUTION_TIME_REGEX in line:
                # need to mark the execution time, and statement
                sql_statements.append(
                    SqlStatement(
                        statement=list(current_sql_statement),
                        execution_time=self._extract_execution_time(line),
                    )
                )
                current_sql_statement = []
                continue

            current_sql_statement.append(line)

        return sql_statements

    def _count_duplicates(self):
        results = defaultdict(int)
        statements = {}
        for s in self.sorted:
            results[s.hash] += 1
            statements[s.hash] = s

        profile = []
        for hash_id, count in results.items():
            statement = statements[hash_id]
            profile.append({
                'count': count,
                'individual_time': statement.execution_time,
                'total_time': count * statement.execution_time,
                'statement': statement,
            })
        profile.sort(key=lambda r: r['count'])
        return profile


def profile_sql(func, args=None, kwargs=None):
    args = args or []
    kwargs = kwargs or {}
    with capture_output() as profile:
        result = func(*args, **kwargs)

    sql = ParseSQL(profile.stdout)
    return result, sql

