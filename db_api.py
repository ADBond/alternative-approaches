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

# user-facing:
def db_api(backend_str, *args, **kwargs):
    return DatabaseAPI.from_str(backend_str, *args, **kwargs)

class DuckDBAPI(DatabaseAPI):
    _name_for_factory = "duckdb"

class SparkAPI(DatabaseAPI):
    _name_for_factory = "spark"


if __name__ == "__main__":
    db_api("duckdb", 3, 4, thing="spling")
    db_api("spark", 5, 4, 2, thing="splingo", spark="spark")
    try:
        db_api("invalid_backend", 5, 4, 2, thing="splingo", spark="spark")
    except ValueError as e:
        print(e)
