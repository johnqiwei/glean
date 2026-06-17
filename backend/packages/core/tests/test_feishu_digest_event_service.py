"""Unit tests for Feishu digest event helpers."""

from datetime import date, datetime
from zoneinfo import ZoneInfo

from glean_core.services.feishu_digest_event_service import FeishuDigestEventService


def test_parse_digest_code_accepts_common_request_formats():
    assert FeishuDigestEventService._parse_digest_code("@Glean n01", "Asia/Singapore") == (
        "n01",
        None,
    )
    assert FeishuDigestEventService._parse_digest_code("@Glean N1", "Asia/Singapore") == (
        "n01",
        None,
    )
    assert FeishuDigestEventService._parse_digest_code("@Glean #1", "Asia/Singapore") == (
        "n01",
        None,
    )
    assert FeishuDigestEventService._parse_digest_code("@Glean news 1", "Asia/Singapore") == (
        "n01",
        None,
    )


def test_parse_digest_codes_accepts_multiple_codes_and_deduplicates():
    assert FeishuDigestEventService._parse_digest_codes(
        "@Glean n01 n02 #2 news 3",
        "Asia/Singapore",
    ) == [
        ("n01", None),
        ("n02", None),
        ("n03", None),
    ]


def test_parse_digest_code_accepts_date_prefix_with_hash_number():
    today = datetime.now(ZoneInfo("Asia/Singapore")).date()
    expected_date = date(today.year, 6, 16)
    if expected_date > today:
        expected_date = date(today.year - 1, 6, 16)

    assert FeishuDigestEventService._parse_digest_code("@Glean 0616 #01", "Asia/Singapore") == (
        "n01",
        expected_date,
    )
