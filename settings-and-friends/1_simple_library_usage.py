import pandas as pd

from splink.database_api import DuckDBAPI
from splink.linker import Linker
from splink.settings_creator import SettingsCreator
from splink.blocking_rule_library import block_on
import splink.comparison_library as cl
import splink.comparison_template_library as ctl


settings = SettingsCreator(
    link_type="dedupe_only",
    comparisons=[
        cl.ExactMatch("name"),
        cl.LevenshteinAtThresholds("county").configure(term_frequency_adjustments=True),
        ctl.EmailComparison("primary_email"),
    ],
    blocking_rules_to_generate_predicions=[
        block_on("name"),
        block_on("county", "town"),
    ],
    retain_intermediate_calculation_columns=True,
)
# this is allowed to be a dictionary instead, but I think we should advocate for this way that
# works nicely with Intellisense. This interface defines the schema for dicts / json


df = pd.read_csv("people.csv")
linker = Linker(df, settings, DuckDBAPI())
# the linker creates the Settings, which also creates the Comparisons and BlockingRules
# none of these classes are exposed to the user

linker.predict()
