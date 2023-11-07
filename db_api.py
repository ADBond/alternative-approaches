from abc import ABC, abstractmethod
from typing import overload, Literal, Type, final

class DatabaseAPI(ABC):
    """
    just stuff for interacting with db
    probably better if we can keep stuff to do with writing SQL elsewhere
    eg things like _random_sample_sql
    """

    def __init__(self, *args, **kwargs):
        print(f"Creating type {type(self)} with {args} and {kwargs}")

    @classmethod
    def subclass_from_str(cls, string):
        # use sequence unpacking form as will catch if we duplicate
        # _name_for_factory in subclasses
        [subclass] = (
            c for c in cls.__subclasses__()
            if c._name_for_factory == string
        )
        return subclass

    @final
    def _log_and_execute_sql(
        self, final_sql: str, templated_name: str, physical_name: str
    ):
        """
        logging and error handling for running SQL. This should call _directly_execute_sql
        in turn this should only ever be called by _execute_sql_to_make_table
        """
        # aka _log_and_run_sql_execution
        print(f"log some stuff for {templated_name} aka {physical_name}")
        print("not actually making that table here, mind")
        self._directly_execute_sql(final_sql)
        print("no errors on this occasion :)")

    @abstractmethod
    def _directly_execute_sql(self, final_sql):
        """
        this should be literally the only thing that ever touches the db
        the only thing that should use this is _log_and_run_sql_execution
        """
        # aka _run_sql_execution
        pass

    def execute_sql_to_make_table(self, final_sql, templated_name, physical_name):
        """
        this is what we call to make a table via some SQL
        subclasses can override if they need to do special processing,
        but this default implementation should suffice for simple backends
        """
        # aka _execute_sql_against_backend
        self._delete_table_from_database(physical_name)

        print("making a table")
        create_table_sql = f"CREATE TABLE {physical_name} AS ({final_sql})"
        self._log_and_execute_sql(create_table_sql, templated_name, physical_name)

        return f"frame for {physical_name}"

class DuckDBAPI(DatabaseAPI):
    """
    DuckDB API
    just stuff for interacting with db
    probably better if we can keep stuff to do with writing SQL elsewhere
    eg things like _random_sample_sql
    """
    _name_for_factory = "duckdb"
    def __init__(
        self,
        ddb_connection: str = ":memory:",
    ):
        """how to make a duck"""
        self._con = ddb_connection

    def _directly_execute_sql(self, final_sql):
        print(f"actually executing SQL in duckdb [{self._con}]: {final_sql}")

    def _table_exists_in_database(self, table_name):
        return True

    def _delete_table_from_database(self, name):
        print("dropping table")

    def export_to_duckdb_file(self, output_path, delete_intermediate_tables=False):
        print("duckdb stuff")


class SparkAPI(DatabaseAPI):
    """num tum rum etc explanations for Spark"""
    _name_for_factory = "spark"
    def __init__(self, num, tum, rum, thing=None, spark="spork"):
        print(f"{num=}, {tum=}, {rum=}, {thing=}")

@overload
def db_api(backend_str: Literal["duckdb"]) -> Type[DuckDBAPI]:
    ...

@overload
def db_api(backend_str: Literal["spark"]) -> Type[SparkAPI]:
    ...

# user-facing:
def db_api(backend_str: str) -> Type[DatabaseAPI]:
    """docstring for db_api"""
    return DatabaseAPI.subclass_from_str(backend_str)


if __name__ == "__main__":
    dk = db_api("duckdb")("dbfile.db")
    table = dk.execute_sql_to_make_table("SOMESQL", "splink_name", "tablename")
    print(table)
