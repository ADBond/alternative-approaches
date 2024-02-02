from splink.comparison_creator import ComparisonCreator
from splink.comparison_library import CustomComparison
from splink.settings import Settings
from splink.settings_creator import SettingsCreator


# this is a sketch of how the logic would work in the linker:
class Linker:
    def __init__(self, df, settings: dict | SettingsCreator | str, database_api):

        dialect = database_api.dialect

        # can adjust how this looks - don't have to use the same parameter for both
        if isinstance(settings, str):  # or path-like
            # can vary this - who deals with file, etc.
            real_settings = Settings.from_json(settings)
            # in this case we have a proper settings object so don't need to deal with any Creator classes
        else:
            if isinstance(settings, dict):
                # convenince for user, as we have for ComparisonCreator and ComparisonLevelCreator
                # see below also
                settings_creator = SettingsCreator(**settings)
            else:
                settings_creator = settings

            real_settings = settings_creator.create_settings(dialect)

        # no more dicts!
        self.settings: Settings = real_settings


class SettingsCreator:
    def __init__(self, link_type, comparisons):
        real_comparisons = []
        for comparison in comparisons:
            if isinstance(comparison, dict):
                real_comparison = CustomComparison(**comparison)
            else:
                real_comparison = comparison
            real_comparisons.append(real_comparison)
        
        # no more dicts
        self.real_comparisons: list[ComparisonCreator] = real_comparisons

# and ComparisonCreator does the same for ComparisonLevelCreator!
