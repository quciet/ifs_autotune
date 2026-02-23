from backend.common_sce_utils import parse_dimension_flag


def test_parse_dimension_flag_text_variants() -> None:
    assert parse_dimension_flag("1") == 1
    assert parse_dimension_flag("1.0") == 1
    assert parse_dimension_flag("0") == 0
    assert parse_dimension_flag("0.0") == 0


def test_parse_dimension_flag_invalids() -> None:
    assert parse_dimension_flag("") is None
    assert parse_dimension_flag(None) is None
    assert parse_dimension_flag("2") is None
    assert parse_dimension_flag("abc") is None
