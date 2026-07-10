"""Provider-neutral helpers for extracting response text."""

from __future__ import annotations

from typing import Any


def _extract_content_text(content: Any) -> list[str]:
    if isinstance(content, str):
        return [content]
    if not isinstance(content, list):
        return []
    fragments: list[str] = []
    for part in content:
        if isinstance(part, str):
            fragments.append(part)
            continue
        if not isinstance(part, dict):
            continue
        text = part.get("text")
        if isinstance(text, str):
            fragments.append(text)
        elif isinstance(text, dict) and isinstance(text.get("value"), str):
            fragments.append(text["value"])
    return fragments


def extract_openai_response_text(response_record: dict[str, Any]) -> str:
    """Extract text from OpenAI Responses or Chat Completions output."""
    response = response_record.get("response")
    body = response.get("body") if isinstance(response, dict) else None
    if not isinstance(body, dict):
        body = response_record.get("body")
    if not isinstance(body, dict):
        return ""

    fragments: list[str] = []
    output = body.get("output")
    if isinstance(output, list):
        for output_item in output:
            if not isinstance(output_item, dict):
                continue
            fragments.extend(_extract_content_text(output_item.get("content")))
            text = output_item.get("text")
            if isinstance(text, str):
                fragments.append(text)
            elif isinstance(text, dict) and isinstance(text.get("value"), str):
                fragments.append(text["value"])
    if isinstance(body.get("output_text"), str):
        fragments.append(body["output_text"])

    choices = body.get("choices")
    if isinstance(choices, list):
        for choice in choices:
            if not isinstance(choice, dict):
                continue
            message = choice.get("message")
            if isinstance(message, dict):
                fragments.extend(_extract_content_text(message.get("content")))
    return "\n".join(fragment for fragment in fragments if fragment)
