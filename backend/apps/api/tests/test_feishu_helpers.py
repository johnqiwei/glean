"""Unit tests for Feishu webhook helper functions."""

from glean_api.routers.feishu import _message_mentions_bot, _verify_verification_token
from glean_core.schemas.config import FeishuConfig


def test_verify_feishu_token_accepts_header_token():
    config = FeishuConfig(verification_token="expected-token")
    _verify_verification_token({"header": {"token": "expected-token"}}, config)


def test_feishu_mention_detection_requires_at_prefix_or_mentions():
    assert _message_mentions_bot({}, "@Glean N01")
    assert _message_mentions_bot({"mentions": [{"name": "Glean"}]}, "N01")
    assert not _message_mentions_bot({}, "please check N01")
