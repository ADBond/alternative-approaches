# Sketch of new-look settings etc

Users can use any combination of proper objects / dicts as they like (though think we should push the former).
These are all undialected. Dicts get converted to the proper objects at first chance before anything else happens.

When we have linker + api these will be converted to (dialected) internal objects - `Settings`, `Comparison`, `ComparisonLevel`.
These are not something the user should be dealing with directly.

Key point is that, after some initial handling which we provide as a convenience for the user, everything is a fixed type.
We also fully separate the classes which the user will deal with to configure things from the classes the linker (+ friends)
will use behind the scenes to do all the Splinking.

## JSON

We have an option to save a model to JSON. I think this should be distinct from the way a user initially constructs settings,
and doesn't _necessarily_ need to conform to the same standards.
I think in fact we might want to distinguish certain things - e.g. `m_probability` that is supplied as an initial 'seed'
vs those that come from various training runs.

I can't see a reason that people should be hand-editing the model JSON (correct me if I'm wrong).
So I don't think that the user should really have to care what that looks like in any detail (only care about compatibility),
and thus we can separate this model from settings construction.
We can provide an interface for interacting with this / finding out information.

## Names

I have called things `Settings` and `SettingsCreator` just to make this sketch clear, not wedded to these for the final version.

## Linker

If we are loading a model we don't need to go through all the `XCreator` steps because we have something already set up (and dialected!).
Otherwise we have a recipe for a model, so need to go through the creator logic to generate the dialected objects.

In this sketch they use the same parameter, but we could have something like:

```py
Linker(df, settings_creator=, model_path=)
```

where users must supply **exactly one** of `settings_creator` or `model_path` if we want to make that clearer.

## Files

* ['getting started' sketch](./1_simple_library_usage.py) Basic example of the 'most common' use of Splink, where user is using the comparison library and doing everything the simplest way
* [more customised sketch](./2_customising_comparisons.py) Example of where the user wants to customise a comparison level with custom sql
* [load model sketch](./3_load_settings.py) Example of how the user would load in a serialised settings object and call predict()
* [sketch of relevant class logic](./sketch_settings_logic.py)

