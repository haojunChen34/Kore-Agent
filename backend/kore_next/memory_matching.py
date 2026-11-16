"""Small, deterministic lexical helpers shared by memory retrieval and deduplication."""

from __future__ import annotations

import re
import unicodedata

# Remove request scaffolding, not domain words or negation in stored facts.
QUERY_STOP_WORDS = (
    "请帮我", "帮我", "请问", "查询", "搜索", "相关的", "相关", "关于", "有关",
    "记忆", "信息", "一下", "什么", "如何", "怎么", "请", "我的", "我们", "安排",
)
ENGLISH_STOP_WORDS = {"the", "a", "an", "and", "of", "to", "is", "my", "me", "please", "find", "about", "what"}


def normalized(text: str) -> str:
    return re.sub(r"[^\w\u4e00-\u9fff]+", "", unicodedata.normalize("NFKC", text).casefold())


def query_terms(text: str) -> set[str]:
    text = unicodedata.normalize("NFKC", text).casefold()
    for word in QUERY_STOP_WORDS:
        text = text.replace(word, " ")
    terms = {word for word in re.findall(r"[a-z0-9_]+", text) if word not in ENGLISH_STOP_WORDS}
    for phrase in re.findall(r"[\u4e00-\u9fff]+", text):
        if len(phrase) == 1:
            continue
        for size in (2, 3):
            terms.update(phrase[i:i + size] for i in range(len(phrase) - size + 1))
    return terms


def lexical_match(query: str, text: str) -> tuple[float, bool]:
    """Coverage is absolute lexical evidence, independent of relative BM25 ranks."""
    terms = query_terms(query)
    if not terms:
        return 0.0, False
    lowered = unicodedata.normalize("NFKC", text).casefold()
    matched = {term for term in terms if term in lowered}
    coverage = len(matched) / len(terms)
    # A short literal term is sufficient; long requests need more than one
    # accidental Chinese bigram. This deliberately does not infer synonyms.
    distinctive = any(re.search(r"[a-z0-9]", term) and len(term) >= 3 for term in matched)
    credible = bool(matched) and (len(terms) <= 3 or distinctive or (len(matched) >= 2 and coverage >= 0.12))
    return coverage, credible


def similarity(left: str, right: str) -> float:
    a, b = query_terms(left), query_terms(right)
    return len(a & b) / max(1, len(a | b))


def literal_relation(old: str, new: str) -> str | None:
    a, b = normalized(old), normalized(new)
    if a == b and a:
        return "duplicate"
    # Only literal additions without a correction marker are safe to merge
    # without semantic judgement. Other relations are reviewed or model-checked.
    correction = re.search(r"不再|改为|改用|改成|搬到|不是|不喜欢|但是|但现在|instead|no longer|not\b", new, re.I)
    if a and b and not correction:
        if a in b:
            return "supplement"
        if b in a:
            return "duplicate"
    return None
