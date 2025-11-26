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


def load_compatible_training_samples(
    db_path: str, current_structure: tuple, dataset_id: str | None
):
    conn = sqlite3.connect(db_path)
    cur = conn.cursor()

    if dataset_id is None:
        cur.execute(
            "SELECT model_id, input_param, input_coef, output_set, dataset_id"
            " FROM model_input WHERE dataset_id IS NULL"
        )
    else:
        cur.execute(
            "SELECT model_id, input_param, input_coef, output_set, dataset_id"
            " FROM model_input WHERE dataset_id = ?",
            (dataset_id,),
        )
    rows = cur.fetchall()

    compatible = []
    for model_id, ipjs, icjs, osjs, _ in rows:
        try:
            ip = json.loads(ipjs)
            ic = json.loads(icjs)
            os = json.loads(osjs)
        except Exception:
            continue

        structA = extract_structure_keys(ip, ic, os)
        if is_subset_dataset(structA, current_structure):
            compatible.append((model_id, ip, ic, os))

    samples = []
    for mid, ip, ic, os in compatible:
        if dataset_id is None:
            cur.execute(
                """
                SELECT mo.fit_pooled
                FROM model_output mo
                JOIN model_input mi ON mo.model_id = mi.model_id
                WHERE mi.model_id = ? AND mi.dataset_id IS NULL
                """,
                (mid,),
            )
        else:
            cur.execute(
                """
                SELECT mo.fit_pooled
                FROM model_output mo
                JOIN model_input mi ON mo.model_id = mi.model_id
                WHERE mi.model_id = ? AND mi.dataset_id = ?
                """,
                (mid, dataset_id),
            )

        fr = cur.fetchone()
        fit = fr[0] if fr else None

        samples.append({
            "model_id": mid,
            "input_param": ip,
            "input_coef": ic,
            "output_set": os,
            "fit_pooled": fit,
        })

    conn.close()
    return samples
