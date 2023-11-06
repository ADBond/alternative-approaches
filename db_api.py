from typing import overload, Literal, Type

class DatabaseAPI:

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

class DuckDBAPI(DatabaseAPI):
    """num tum etc explanations for Duckdb"""
    _name_for_factory = "duckdb"
    def __init__(self, num, tum, thing=None):
        print(f"{num=}, {tum=}, {thing=}")

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
    DuckDBAPI(3, 1, thing=123)
    db_api("duckdb")(3, 4, thing="spling")
    db_api("spark")(5, 4, 2, thing="splingo", spark="spark")
    try:
        db_api("invalid_backend")(5, 4, 2, thing="splingo", spark="spark")
    except ValueError as e:
        print(e)
    # try out tooltips:
    # sp = db_api("spark")
    # sp = db_api("spark")()
    # dk = db_api("duckdb")()
