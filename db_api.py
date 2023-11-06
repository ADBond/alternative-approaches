class DatabaseAPI:

    def __init__(self, *args, **kwargs):
        print(f"Creating type {type(self)} with {args} and {kwargs}")

    @classmethod
    def from_str(cls, string, *args, **kwargs):
        # use sequence unpacking form as will catch if we duplicate
        # _name_for_factory in subclasses
        [subclass] = (
            c for c in cls.__subclasses__()
            if c._name_for_factory == string
        )
        return subclass(*args, **kwargs)

class DuckDBAPI(DatabaseAPI):
    _name_for_factory = "duckdb"

class SparkAPI(DatabaseAPI):
    _name_for_factory = "spark"


if __name__ == "__main__":
    DatabaseAPI.from_str("duckdb", 3, 4, thing="spling")
    DatabaseAPI.from_str("spark", 5, 4, 2, thing="splingo", spark="spark")
    try:
        DatabaseAPI.from_str("invalid_backend", 5, 4, 2, thing="splingo", spark="spark")
    except ValueError as e:
        print(e)
