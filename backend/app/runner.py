import time, random


def apply_inputs_stub(config: dict) -> None:
    # Placeholder: later we will edit Working.sce / IFsBase.run.db here
    _ = config


def run_ifs_stub() -> dict:
    # Simulate a model run
    time.sleep(0.2)
    # Fake output value to feed a toy metric
    return {"POP_2019": random.uniform(100.0, 105.0)}
