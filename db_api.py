from abc import ABCMeta, abstractmethod
from random import choice, seed
from typing import overload, Literal, Type, final


class SomeException(Exception):
    pass

def log_and_run_with_try(execute_sql_func):
    """
    logging and error handling for running SQL. This should call _directly_execute_sql
    in turn this should only ever be called by __execute_sql_to_make_table
    supersedes _log_and_run_sql_execution
    """
    def log_and_run_sql_with_error_handling(self, final_sql, templated_name, physical_name):
        # aka _log_and_run_sql_execution
        print(f"log some stuff for {templated_name} aka {physical_name}")
        print("not actually making that table here, mind")
        try:
            execute_sql_func(self, final_sql)
        except SomeException as e:
            print(f"\t\tERROR: {e}")
        else:
            print("\t\tno errors on this occasion :)")

    return log_and_run_sql_with_error_handling

class WrapSQLExecutionMeta(ABCMeta):
    def __new__(cls, clsname, bases, attrs):
        # wrap class '_directly_execute_sql' with log_and_run_with_try
        # all subclasses will then have this method automatically decorated
        attrs["_directly_execute_sql"] = log_and_run_with_try(attrs["_directly_execute_sql"])

        return super().__new__(cls, clsname, bases, attrs)


class DatabaseAPI(metaclass=WrapSQLExecutionMeta):
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

    @abstractmethod
    def _delete_table_from_database(self, name):
        pass
    
    @abstractmethod
    def _directly_execute_sql(self, final_sql):
        """
        this should be literally the only thing that ever touches the db
        this is not directly accessible, and is automatically wrapped
        with logging + error handling
        Main thing that subclasses need to implement
        """
        # aka _run_sql_execution
        pass

    def _setup_for_execute_sql(self, final_sql, templated_name, physical_name):
        """
        customise this per backend if needed
        """
        self._delete_table_from_database(physical_name)

        print("making a table")
        return f"CREATE TABLE {physical_name} AS ({final_sql})"

    def _cleanup_for_execute_sql(self, frame):
        """
        customise this per backend if needed
        """
        return f"frame for {frame}"

    @final
    def _execute_sql_to_make_table(self, final_sql, templated_name, physical_name):
        # aka _execute_sql_against_backend
        create_table_sql = self._setup_for_execute_sql(final_sql, templated_name, physical_name)
        frame = self._directly_execute_sql(create_table_sql, templated_name, physical_name)
        splink_df = self._cleanup_for_execute_sql(frame)

        return splink_df

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
        if choice(range(2)) != 1:
            # normal execution
            print(f"\tactually executing SQL in duckdb [{self._con}]: {final_sql}")
        else:
            raise SomeException(f"the SQL [{final_sql}] failed")

    def _delete_table_from_database(self, name):
        print("\ndropping table")

    def export_to_duckdb_file(self, output_path, delete_intermediate_tables=False):
        print("duckdb stuff")


class SparkAPI(DatabaseAPI):
    """num tum rum etc explanations for Spark"""
    _name_for_factory = "spark"
    def __init__(self, spark, num_partitions_on_repartition=None):
        self._spark = spark

    def _directly_execute_sql(self, final_sql):
        if choice(range(2)) != 1:
            # normal execution
            print(f"\tactually executing SQL in spark [{self._spark}]: {final_sql}")
        else:
            raise SomeException(f"the SQL [{final_sql}] failed")

    def _delete_table_from_database(self, name):
        print("\ndropping table in a sparky way")

    def _setup_for_execute_sql(self, final_sql, templated_name, physical_name):
        self._delete_table_from_database(physical_name)

        print("making a spark table")
        return f"CREATE TABLE {physical_name} AS ({final_sql})"

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
    seed(1402)
    dk = db_api("duckdb")("dbfile.db")
    table = dk._execute_sql_to_make_table("SOMESQL", "splink_name", "tablename")

    dk._execute_sql_to_make_table("SOMESQL", "splink_name2", "tablename")
    dk._execute_sql_to_make_table("SOMESQL", "splink_name3", "tablename")
    dk._execute_sql_to_make_table("SOMESQL", "splink_name4", "tablename")

    spk = db_api("spark")("sparkinstance")
    spk._execute_sql_to_make_table("SOMESPARKSQL", "splink_spk_", "tablename2")
