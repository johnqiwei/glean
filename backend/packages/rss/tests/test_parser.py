"""Tests for RSS parser."""

from glean_rss.parser import _get_favicon_url, ParsedEntry


class TestFaviconURL:
    """Test favicon URL generation."""

    def test_get_favicon_url_valid_http(self) -> None:
        """Test favicon URL generation with valid HTTP URL."""
        url = "http://example.com/blog"
        result = _get_favicon_url(url)
        assert result == "https://www.google.com/s2/favicons?domain=example.com&sz=64"

    def test_get_favicon_url_valid_https(self) -> None:
        """Test favicon URL generation with valid HTTPS URL."""
        url = "https://example.com/blog"
        result = _get_favicon_url(url)
        assert result == "https://www.google.com/s2/favicons?domain=example.com&sz=64"

    def test_get_favicon_url_with_subdomain(self) -> None:
        """Test favicon URL generation with subdomain."""
        url = "https://blog.example.com"
        result = _get_favicon_url(url)
        assert result == "https://www.google.com/s2/favicons?domain=blog.example.com&sz=64"

    def test_get_favicon_url_with_port(self) -> None:
        """Test favicon URL generation with port."""
        url = "http://example.com:8080/blog"
        result = _get_favicon_url(url)
        assert result == "https://www.google.com/s2/favicons?domain=example.com:8080&sz=64"

    def test_get_favicon_url_none(self) -> None:
        """Test favicon URL generation with None."""
        result = _get_favicon_url(None)
        assert result is None

    def test_get_favicon_url_empty(self) -> None:
        """Test favicon URL generation with empty string."""
        result = _get_favicon_url("")
        assert result is None

    def test_get_favicon_url_invalid(self) -> None:
        """Test favicon URL generation with invalid URL."""
        result = _get_favicon_url("not-a-url")
        assert result is None

    def test_get_favicon_url_relative(self) -> None:
        """Test favicon URL generation with relative URL."""
        result = _get_favicon_url("/blog/feed")
        assert result is None


class TestParsedEntry:
    """Test ParsedEntry content extraction logic."""

    def test_parsed_entry_with_true_full_content(self) -> None:
        """Test entry with a long content block that qualifies as full content."""
        data = {
            "id": "1",
            "link": "https://example.com/1",
            "title": "Test Title",
            "summary": "This is a brief summary of the test entry.",
            "content": [
                {
                    "type": "text/html",
                    "value": "This is a much longer body of text that represents the actual article content. "
                             "It needs to exceed 250 characters in total length to qualify. "
                             "Let's add some more sentences here to ensure we cross that threshold. "
                             "A healthy amount of prose is critical for full content detection, "
                             "so we keep typing until the character count is comfortably over 250 characters."
                }
            ]
        }
        entry = ParsedEntry(data)
        assert entry.has_full_content is True
        assert entry.content.startswith("This is a much longer body")

    def test_parsed_entry_with_short_caption_content(self) -> None:
        """Test entry with content that is too short (e.g. an image caption) and should be rejected."""
        data = {
            "id": "2",
            "link": "https://example.com/2",
            "title": "Test Title 2",
            "summary": "This is a brief summary of the test entry that is longer than the content caption.",
            "content": [
                {
                    "type": "text/plain",
                    "value": "An image caption."
                }
            ]
        }
        entry = ParsedEntry(data)
        assert entry.has_full_content is False
        assert entry.content == "An image caption."

