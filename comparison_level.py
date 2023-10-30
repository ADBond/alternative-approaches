# from splink import levenshtein_level
from abc import ABC, abstractmethod, abstractproperty

class Dialect(ABC):

    @abstractproperty
    def name(self):
        pass

    @property
    def levenshtein_fn(self):
        raise NotImplementedError(f"No Levenshtein available :( for {type(self)}")

class _DuckDBDialect(Dialect):
    @property
    def name(self):
        return "duckdb"

    @property
    def levenshtein_fn(self):
        return "levenshtein"

class _QuackDBDialect(Dialect):
    @property
    def name(self):
        return "quackdb"

    @property
    def levenshtein_fn(self):
        return "levenshtein_quack"

class _EchoDBDialect(Dialect):
    @property
    def name(self):
        return "echodb"
    # no levenshtein available for this backend


duckdb_dialect = _DuckDBDialect()
quackdb_dialect = _QuackDBDialect()
echodb_dialect = _EchoDBDialect()
# or a nicer singleton approach - but something so that we have these importable

all_dialects = [duckdb_dialect, quackdb_dialect, echodb_dialect]


class ComparisonLevel(ABC):
    # all the core functionality we need
    # only thing specific versions need to override (+ charts label)
    @abstractmethod
    def sql_expression(self, dialect: Dialect):
        pass

    # basic check of backend availablity
    def available_backends(self):
        available_dialects = []
        for dialect in all_dialects:
            try:
                self.sql_expression(dialect)
                available_dialects.append(dialect.name)
            except NotImplementedError:
                pass
        return available_dialects

class LevenshteinLevel(ComparisonLevel):

    # col_name really covered by ComparisonLevel
    def __init__(self, col_name, threshold):
        self.col_l = f"{col_name}_l"
        self.col_r = f"{col_name}_r"
        self.threshold = threshold
        # super().__init__(blah blah blah)

    def sql_expression(self, dialect: Dialect):
        return f"{dialect.levenshtein_fn}({self.col_l}, {self.col_r}) <= {self.threshold}"
        # could have a placeholder eg <levenshtein_function_name> for printing in isolation, without a dialect set
        # separate out the __str__ representation (for users) vs the actual SQL string that is used


def sqlglot_translate(expr_str: str, from_dialect: Dialect, to_dialect: Dialect):
    # not actually implemented here, but you get the idea
    return f"{expr_str}<{to_dialect.name}>"


# within a comparison any simple dictionaries get converted to this:
class CustomLevel(ComparisonLevel):
    def __init__(self, sql_expression: str, dialect: Dialect=None):
        # allow dialect=None for backend-agnostic expressions
        self._sql_expression = sql_expression
        self._dialect = dialect
    
    def sql_expression(self, dialect: Dialect):
        if dialect is None or dialect == self._dialect:
            return self._sql_expression
        # whatever sqlglot function does this kind of thing, where possible:
        return sqlglot_translate(self._sql_expression, from_dialect=self._dialect, to_dialect=dialect)


class Linker():

    def __init__(self, settings, dialect: Dialect):
        self.dialect = dialect
        self.settings = settings

    # not really how this works, of course, but a flavour:
    @property
    def sql_expression(self):
        print(self.settings["cl"].sql_expression(self.dialect))

# can always put a wrapper around this
lev_lazy_level = LevenshteinLevel("mycol", 2)

settings = {"cl": lev_lazy_level}

linker_ddb = Linker(settings, duckdb_dialect)
linker_qdb = Linker(settings, quackdb_dialect)
linker_edb = Linker(settings, echodb_dialect)

linker_ddb.sql_expression
linker_qdb.sql_expression
try:
    linker_edb.sql_expression
except NotImplementedError as e:
    print(e)

print(lev_lazy_level.available_backends())

custom_lazy_level = CustomLevel("some sql expr", duckdb_dialect)

settings = {"cl": custom_lazy_level}

linker_ddb = Linker(settings, duckdb_dialect)
linker_qdb = Linker(settings, quackdb_dialect)
linker_edb = Linker(settings, echodb_dialect)

linker_ddb.sql_expression
linker_qdb.sql_expression
try:
    linker_edb.sql_expression
except NotImplementedError as e:
    print(e)
