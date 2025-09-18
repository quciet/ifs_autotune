from fastapi.testclient import TestClient
from app.main import app


def test_ifs_run_stub():
    c = TestClient(app)
    payload = {"parameters": {"tfrmin": 1.5}}
    r = c.post("/ifs/run", json=payload)
    assert r.status_code == 200
    data = r.json()
    assert "run_id" in data and data["run_id"] > 0
    assert "metric" in data and isinstance(data["metric"], float)
    assert "output" in data and "POP_2019" in data["output"]
