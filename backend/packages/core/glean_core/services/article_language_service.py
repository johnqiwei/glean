"""
Article language and processing service.

Handles HTML text cleaning, summarization, and translation via DeepSeek API.
"""

import re

import httpx
from bs4 import BeautifulSoup

from glean_core import get_logger
from glean_core.schemas.config import DigestConfig
from glean_database.models import Entry

logger = get_logger(__name__)

DETAIL_STOP_SECTION_PATTERNS = [
    r"ai\s*news\s*(?:website|site|网站)",
    r"ai\s*(?:twitter|x)\s*recap",
    r"ai\s*推特\s*回顾",
    r"推特\s*回顾",
]


def clean_html_to_text(html: str | None) -> str:
    """
    Clean HTML content and convert to readable structured plain text.
    Preserves links in 'Text (URL)' format.
    """
    if not html:
        return ""

    # Parse with BeautifulSoup
    soup = BeautifulSoup(html, "html.parser")

    # Remove script, style, iframe, svg etc.
    for tag in soup(["script", "style", "iframe", "svg", "img"]):
        tag.decompose()

    # Convert links to Text (URL) format
    for a in soup.find_all("a"):
        href = a.get("href")
        text = a.get_text().strip()
        if href and text and href != text and href.startswith(("http://", "https://")):
            a.replace_with(f"{text} ({href})")

    # Get separator-based text
    text = soup.get_text(separator="\n")

    # Normalize lines and whitespace
    lines = [line.strip() for line in text.splitlines()]
    non_empty_lines = []
    for line in lines:
        if line and (not non_empty_lines or non_empty_lines[-1] != line):
            non_empty_lines.append(line)

    return "\n\n".join(non_empty_lines)


def filter_detail_source_text(text: str, title: str | None = None) -> str:
    """
    Remove trailing newsletter sections that readability can attach to a single article.
    """
    if not text:
        return ""

    title_normalized = (title or "").strip().casefold()
    lines = text.splitlines()
    kept_lines: list[str] = []
    non_empty_seen = 0

    for line in lines:
        stripped = line.strip()
        if stripped:
            non_empty_seen += 1

        if non_empty_seen > 1 and stripped and _is_unrelated_detail_section(stripped):
            if title_normalized and stripped.casefold() in title_normalized:
                kept_lines.append(line)
                continue
            break

        kept_lines.append(line)

    return "\n".join(kept_lines).strip()


def _is_unrelated_detail_section(line: str) -> bool:
    heading = line.strip().strip("#:：- ")
    if not heading or len(heading) > 80:
        return False

    return any(
        re.fullmatch(pattern, heading, flags=re.IGNORECASE)
        for pattern in DETAIL_STOP_SECTION_PATTERNS
    )


class ArticleLanguageService:
    """
    Service for summarization and translation using LLMs.
    """

    def __init__(self, config: DigestConfig) -> None:
        """
        Initialize the service with config.
        """
        self.config = config
        self.api_key = config.llm_api_key
        self.base_url = config.llm_base_url or "https://api.deepseek.com"
        self.model = config.llm_model or "deepseek-v4-flash"

    async def _call_llm(self, system_prompt: str, user_prompt: str) -> str:
        """
        Helper method to call OpenAI-compatible LLM provider (DeepSeek).
        """
        if not self.api_key:
            logger.error("LLM API key is not configured")
            raise ValueError("LLM API key is not configured")

        headers = {
            "Authorization": f"Bearer {self.api_key}",
            "Content-Type": "application/json",
        }

        # Build API payload.
        # DeepSeek V4 Flash runs in non-thinking mode by default (standard chat).
        payload = {
            "model": self.model,
            "messages": [
                {"role": "system", "content": system_prompt},
                {"role": "user", "content": user_prompt},
            ],
            "temperature": 0.3,
            "max_tokens": 2048,
        }

        # Resolve completion URL
        url = self.base_url.rstrip("/") + "/chat/completions"

        async with httpx.AsyncClient(timeout=60.0) as client:
            try:
                response = await client.post(url, json=payload, headers=headers)
                response.raise_for_status()
                data = response.json()
                content = data["choices"][0]["message"]["content"]
                return str(content).strip()
            except Exception:
                logger.exception("LLM API call failed", extra={"url": url, "model": self.model})
                raise

    async def summarize_to_zh(self, entry: Entry) -> str:
        """
        Generate a Chinese summary for a feed entry.
        Prioritizes entry.summary, then entry.content, and falls back to entry.title.
        Target length is 120-200 characters.
        """
        # Determine source text
        source_text = ""
        if entry.summary:
            source_text = clean_html_to_text(entry.summary)

        # If summary is too short or empty, use content
        if len(source_text) < 100 and entry.content:
            source_text = clean_html_to_text(entry.content)

        if not source_text:
            source_text = entry.title

        # Limit source text length sent to summarizer to avoid huge token usage
        if len(source_text) > 6000:
            source_text = source_text[:6000] + "\n...(Text truncated)..."

        system_prompt = (
            "你是一个专业的新闻编辑和摘要生成助手。请阅读以下文章，并为其生成一段简明扼要的中文摘要。\n"
            "要求：\n"
            "1. 语言简练、客观，不要出现“这篇文章讨论了”、“摘要如下”等废话。\n"
            "2. 长度严格控制在 120-200 个中文字符之间。\n"
            "3. 突出文章的核心事实、核心论点以及结论。"
        )

        user_prompt = f"标题: {entry.title}\n\n内容:\n{source_text}"

        try:
            summary = await self._call_llm(system_prompt, user_prompt)
            # Basic validation
            if summary:
                return summary
            return entry.title
        except Exception:
            logger.warning("Summarization failed, falling back to title", extra={"entry_id": entry.id})
            return entry.title

    async def translate_fulltext_to_zh(self, entry: Entry) -> str:
        """
        Translate the entry's full-text content into Chinese.
        If the text is too long, splits it into chunks and merges the translation.
        """
        source_text = filter_detail_source_text(
            clean_html_to_text(entry.content or entry.summary),
            entry.title,
        )
        if not source_text:
            return "（该文章没有可用正文，请点击原链接阅读）"

        # Split text into chunks if it is too long (approx 4000 characters per chunk)
        chunk_size = 4000
        chunks = [source_text[i:i + chunk_size] for i in range(0, len(source_text), chunk_size)]

        system_prompt = (
            "你是一个专业的翻译官。请将以下文章段落翻译为中文。\n"
            "要求：\n"
            "1. 翻译要信达雅，符合中文的表达和阅读习惯，对于专业技术/金融词汇需翻译准确。\n"
            "2. 保持段落格式和逻辑结构。\n"
            "3. 只需要输出翻译后的文本，不要有任何你的解释、总结、前言或译者注。"
        )

        translated_chunks = []
        for index, chunk in enumerate(chunks):
            logger.info(
                "Translating chunk",
                extra={"entry_id": entry.id, "chunk_index": index, "total_chunks": len(chunks)},
            )
            user_prompt = f"请翻译以下文本段落：\n\n{chunk}"
            try:
                translated_chunk = await self._call_llm(system_prompt, user_prompt)
                translated_chunks.append(translated_chunk)
            except Exception as e:
                logger.error(
                    "Failed to translate chunk",
                    extra={"entry_id": entry.id, "chunk_index": index, "error": str(e)},
                )
                # Fallback to original chunk if translation fails to prevent loss of content
                translated_chunks.append(f"\n[翻译失败，保留原文]:\n{chunk}")

        return "\n\n".join(translated_chunks)
