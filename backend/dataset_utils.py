from __future__ import annotations
import json, hashlib, sqlite3


def compute_dataset_id(ifs_id: int, input_param: dict, input_coef: dict, output_set: dict) -> str:
    param_keys = sorted(input_param.keys())
    coef_keys = sorted(
        f"{func}.{x}.{beta}"
        for func, xmap in input_coef.items()
        for x, betamap in xmap.items()
        for beta in betamap.keys()
    )
    output_keys = sorted(output_set.keys())

    structure = {
        "ifs_id": int(ifs_id),
        "param_keys": param_keys,
        "coef_keys": coef_keys,
        "output_keys": output_keys,
    }
    return hashlib.sha256(json.dumps(structure, sort_keys=True).encode("utf-8")).hexdigest()


def extract_structure_keys(input_param: dict, input_coef: dict, output_set: dict):
    param = set(input_param.keys())
    coef = set(
        f"{func}.{x}.{beta}"
        for func, xmap in input_coef.items()
        for x, betamap in xmap.items()
        for beta in betamap.keys()
    )
    out = set(output_set.keys())
    return (param, coef, out)


def is_subset_dataset(structA, structB):
    PA, CA, OA = structA
    PB, CB, OB = structB
    return PA.issubset(PB) and CA.issubset(CB) and OA.issubset(OB)


def load_compatible_training_samples(db_path: str, current_structure: tuple):
    conn = sqlite3.connect(db_path)
    cur = conn.cursor()

    cur.execute("SELECT model_id, input_param, input_coef, output_set FROM model_input")
    rows = cur.fetchall()

    compatible = []
    for model_id, ipjs, icjs, osjs in rows:
        try:
            ip = json.loads(ipjs)
            ic = json.loads(icjs)
            os = json.loads(osjs)
        except Exception:
            continue

        structA = extract_structure_keys(ip, ic, os)
        if is_subset_dataset(structA, current_structure):
            compatible.append(model_id)

    samples = []
    for mid in compatible:
        cur.execute("SELECT fit_pooled FROM model_output WHERE model_id = ?", (mid,))
        fr = cur.fetchone()
        fit = fr[0] if fr else None

        cur.execute("SELECT input_param, input_coef, output_set FROM model_input WHERE model_id = ?", (mid,))
        r2 = cur.fetchone()
        if r2:
            samples.append({
                "model_id": mid,
                "input_param": json.loads(r2[0]),
                "input_coef": json.loads(r2[1]),
                "output_set": json.loads(r2[2]),
                "fit_pooled": fit,
            })

    conn.close()
    return samples
