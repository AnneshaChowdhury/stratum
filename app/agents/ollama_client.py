import json
import re

from groq import AsyncGroq

from app.config import settings

_client: AsyncGroq | None = None


def get_client() -> AsyncGroq:
    global _client
    if _client is None:
        _client = AsyncGroq(api_key=settings.groq_api_key)
    return _client


async def chat(prompt: str, system: str = "") -> str:
    messages = []
    if system:
        messages.append({"role": "system", "content": system})
    messages.append({"role": "user", "content": prompt})

    response = await get_client().chat.completions.create(
        model=settings.groq_model,
        messages=messages,
        temperature=0.1,
        max_tokens=4096,
    )
    return response.choices[0].message.content


def extract_json(text: str) -> dict | list:
    text = re.sub(r"```(?:json)?\s*", "", text).strip().rstrip("`").strip()
    start = next((i for i, c in enumerate(text) if c in "{["), None)
    if start is None:
        raise ValueError(f"No JSON found in LLM response: {text[:200]}")
    return json.loads(text[start:])
