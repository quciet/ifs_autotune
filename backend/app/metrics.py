def metric_stub(output: dict) -> float:
    # Toy objective: distance from 102.0 (smaller is better)
    return abs(output["POP_2019"] - 102.0)
