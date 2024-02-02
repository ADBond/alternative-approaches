from splink import DB_API
from splink.linker import Linker, Settings
from splink.datasets import splink_datasets
import splink.comparison_library as cl
import splink.comparison_level_library as cll


name_comparison = cl.levenshtein("name", [2, 3])
dob_comparison = cl.date("dob")
postcode_comparison = cl.custom_comparison(
    levels=[
        cll.null_level("postcode"),
        cll.exact_match("postcode"),
        cll.custom_level(
            sql_condition="postcode_dist(postcode_l, postcode_r) < 30",
            tf_adjustment_column="postcode"
        )
    ],
    output_column_name="postcode",
)

settings = Settings(
    link_type="link_and_dedupe",
    comparisons=[
        name_comparison,
        dob_comparison,
        postcode_comparison,
    ],
    unique_id_column_name="record_id",
)

settings_dict = Settings.to_dict()
settings.save_settings("settings.json")
settings.save_settings("model.json", with_parameters=True)

db = DB_API("duckdb", ":memory:")

df = splink_datasets.fake_1000

linker = Linker(df, settings, db)
