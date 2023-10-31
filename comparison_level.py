# from splink import levenshtein_level
from abc import ABC, abstractmethod, abstractproperty
from contextlib import contextmanager
from dataclasses import dataclass

class Dialect(ABC):

    @abstractproperty
    def name(self):
        pass

    def sqlglot_dialect_name(self):
        # probably useful to have this distinct from _our_ name, which maps more directly to engines
        pass

    @property
    def levenshtein_fn(self):
        raise NotImplementedError(f"No Levenshtein available :( for dialect '{self.name}'")

    # this really lives with InputColumn
    def enquote(self, col_name):
        pass

class _DuckDBDialect(Dialect):
    @property
    def name(self):
        return "duckdb"

    @property
    def levenshtein_fn(self):
        return "levenshtein"

    def enquote(self, col_name):
        # single quotes
        return f"'{col_name}'"

class _QuackDBDialect(Dialect):
    @property
    def name(self):
        return "quackdb"

    @property
    def levenshtein_fn(self):
        return "levenshtein_quack"

    def enquote(self, col_name):
        # backticks
        return f"`{col_name}`"

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


@dataclass
class InputColumn:
    col_name: str
    dialect: Dialect

    def get_name_in_dialect(self):
        # whatever magic the real class has
        # probably doesn't _really_ live on dialect, but just for illustration
        return self.dialect.enquote(self.col_name)

@contextmanager
def dialect_context(obj, dialect):
    obj._dialect = dialect
    yield
    del obj._dialect

class ComparisonLevelBuilder(ABC):
    # This abstracts a user-based 'description' of a comparison level
    # this is what is represented on the user-side in json
    @abstractmethod
    def _sql_expression(self, dialect: Dialect):
        pass

    def sql_expression(self, dialect: Dialect):
        with dialect_context(self, dialect):
            return self._sql_expression(dialect)

    @property
    def label(self, dialect=None):
        # default option
        return f"level with SQL: {self.sql_expression}"

    def input_column(self, col_name: str, dialect: Dialect):
        # To create a comparison level, a dialect is essential even if no special
        # SQL functions are needed due to differencnes in identifier quotes "" ``
        return InputColumn(col_name, dialect)

    def get_dict(self, dialect: Dialect):
        return {
            "sql_expression": self.sql_expression(dialect),
            "label_for_charts": self.label,
        }

    def _get_dialected_name(self, col):
        return self.input_column(col, self._dialect).get_name_in_dialect()

    # really accessed via InputColumn more directly
    @property
    def col_name(self):
        return self._get_dialected_name(self._col_name)

    @property
    def col_l(self):
        return self._get_dialected_name(f"{self._col_name}_l")

    @property
    def col_r(self):
        return self._get_dialected_name(f"{self._col_name}_r")

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

class LevenshteinLevel(ComparisonLevelBuilder):

    # col_name really covered by ComparisonLevelBuilder
    def __init__(self, col_name, threshold):
        self._col_name = col_name
        self.threshold = threshold
        # super().__init__(blah blah blah)

    @property
    def label(self, dialect=None):
        return f"Levenshtein <= {self.threshold}"

    def _sql_expression(self, dialect: Dialect):
        return f"{dialect.levenshtein_fn}({self.col_l}, {self.col_r}) <= {self.threshold}"
        # could have a placeholder eg <levenshtein_function_name> for printing in isolation, without a dialect set
        # separate out the __str__ representation (for users) vs the actual SQL string that is used


def sqlglot_translate(expr_str: str, from_dialect: Dialect, to_dialect: Dialect):
    # not actually implemented here, but you get the idea
    return f"{expr_str}<translated to {to_dialect.name}>"


# within a comparison any simple dictionaries get converted to this:
class CustomLevel(ComparisonLevelBuilder):
    def __init__(self, sql_expression: str, dialect: Dialect=None):
        # allow dialect=None for backend-agnostic expressions
        self._raw_sql_expression = sql_expression
        self._cl_dialect = dialect
    
    def _sql_expression(self, dialect: Dialect):
        if dialect is None or dialect == self._cl_dialect:
            return self._raw_sql_expression
        # whatever sqlglot function does this kind of thing, where possible:
        return sqlglot_translate(self._raw_sql_expression, from_dialect=self._cl_dialect, to_dialect=dialect)


@dataclass
class ComparisonLevel():
    sql_expression: str
    label_for_charts: str
    # and whatever else
    # plus all the methods, obvs

class Linker():

    def __init__(self, settings, dialect: Dialect):
        self.dialect = dialect
        settings = settings.copy()
        self.settings = self.create_comparison_levels(settings, dialect)

    @staticmethod
    def create_comparison_levels(settings, dialect):
        recipe = settings["cl"]
        try:
            settings["cl"] = ComparisonLevel(**recipe.get_dict(dialect))
        except NotImplementedError as e:
            raise NotImplementedError(f"settings object contains a backend-incompatible level: {e}")
        return settings

    @property
    def comparison_level(self):
        return self.settings["cl"]

    def execute_sql(self):
        print(f"I'm executing the SQL: {self.comparison_level.sql_expression}")

# can always put a wrapper around this
lev_lazy_level = LevenshteinLevel("mycol", 2)

settings = {"cl": lev_lazy_level}

linker_ddb = Linker(settings, duckdb_dialect)
linker_qdb = Linker(settings, quackdb_dialect)
try:
    linker_edb = Linker(settings, echodb_dialect)
except NotImplementedError as e:
    print(e)

linker_ddb.execute_sql()
linker_qdb.execute_sql()

print(lev_lazy_level.available_backends())

# if you want to actually get the comparison level dict
print(lev_lazy_level.get_dict(quackdb_dialect))

custom_lazy_level = CustomLevel("some sql expr", duckdb_dialect)

settings = {"cl": custom_lazy_level}

linker_ddb = Linker(settings, duckdb_dialect)
linker_qdb = Linker(settings, quackdb_dialect)
linker_edb = Linker(settings, echodb_dialect)

linker_ddb.execute_sql()
linker_qdb.execute_sql()
linker_edb.execute_sql()

