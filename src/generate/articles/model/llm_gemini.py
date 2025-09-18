from __future__ import annotations

from dataclasses import dataclass
from typing import Optional
import os

try:
    import google.generativeai as genai
    from google.api_core.exceptions import ResourceExhausted  # rate-limit
except Exception:  # pragma: no cover
    genai = None  # type: ignore
    class ResourceExhausted(Exception): ...  # type: ignore


@dataclass
class _Msg:
    content: str


class GeminiLLM:
    def __init__(
        self,
        api_key: Optional[str] = None,
        model: Optional[str] = None,
        temperature: float = 0.2,
        max_output_tokens: int = 1024,
        system: Optional[str] = None,
    ) -> None:
        key = api_key or os.getenv("GEMINI_API_KEY") or os.getenv("GOOGLE_API_KEY")
        if not key:
            raise EnvironmentError("GEMINI_API_KEY/GOOGLE_API_KEY is missing.")
        if genai is None:
            raise ImportError("google-generativeai is not installed. Run: pip install google-generativeai")

        genai.configure(api_key=key)

        self.model_name = (model or os.getenv("GEMINI_MODEL") or "gemini-1.5-flash").strip()
        self.temperature = float(os.getenv("GEMINI_TEMPERATURE", str(temperature)))
        self.max_output_tokens = int(os.getenv("GEMINI_MAX_TOKENS", str(max_output_tokens)))
        self.system = system

    def invoke(self, prompt: str) -> _Msg:
        model = genai.GenerativeModel(self.model_name, system_instruction=self.system)
        resp = model.generate_content(
            prompt,
            generation_config=dict(
                temperature=self.temperature,
                max_output_tokens=self.max_output_tokens,
            ),
        )
        text = getattr(resp, "text", None)
        if not text:
            cand = getattr(resp, "candidates", None)
            if cand and len(cand) and getattr(cand[0], "content", None):
                parts = cand[0].content.parts
                if parts and hasattr(parts[0], "text"):
                    text = parts[0].text

        return _Msg(content=text or "")
