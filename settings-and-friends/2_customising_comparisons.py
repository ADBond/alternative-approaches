import pandas as pd

from splink.database_api import DuckDBAPI
from splink.linker import Linker
from splink.blocking_rule_library import block_on
import splink.comparison_level_library as cll
import splink.comparison_library as cl
import splink.comparison_template_library as ctl


# can mix and match - any of SettingsCreator, CustomComparison, CustomComparisonLevel
# can instead be dictionaries - will be converted to the corresponding object at
# first opportunity
settings = {
    "link_type": "dedupe_only",
    "comparisons": [
        cl.ExactMatch("name"),
        cl.CustomComparison(
            "county",
            [
                cll.NullLevel("county"),
                cll.LiteralLevel("county", "OtherCounty", "string", side="both"),
                cll.ExactMatchLevel("county"),
                # these two are equivalent constructions - latter is coerced to CustomLevel
                cll.CustomLevel(
                    sql_condition="custom_fuzzy(county_l, county_r) <= 0.9",
                    label_for_charts="fuzzy county",
                ),
                {
                    "sql_condition": "custom_fuzzy(county_l, county_r) <= 0.7",
                    "label_for_charts": "fuzzy county",
                },
                cll.ElseLevel(),
            ]
        ),
        # this is coerced to CustomComparison:
        {
            "output_column_name": "height",
            "comparison_levels": [
                cll.NullLevel("height"),
                cll.ExactMatchLevel("height"),
                cll.CustomLevel(
                    "abs(height_l - height_r) < 3",
                    "height within 3 units"
                ),
                {
                    "sql_condition": "2*abs(height_l - height_r)/(height_l + height_r) <= 0.2",
                    "label_for_charts": "heights within 20%",
                },
                cll.ElseLevel()
            ]
        },
        ctl.EmailComparison("primary_email"),
    ],
    "blocking_rules_to_generate_predicions": [
        block_on("name"),
        block_on("county", "town"),
    ],
}


df = pd.read_csv("people.csv")
linker = Linker(df, settings, DuckDBAPI())
# the linker first converts everything from dicts to creators
# then does the 'creating'

linker.predict()
