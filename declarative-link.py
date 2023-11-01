from splink.config import linker_from_config

linker = linker_from_config(
    "./splink-decl.yaml",
    format="yaml",
    keep_existing_parameter_estimates=True,
)
# optional step
# linker.run_training()
linker.predict()
