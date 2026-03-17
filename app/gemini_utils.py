import re
from typing import Iterable, Optional, Tuple

import google.genai as genai
from google.genai import types as genai_types

from .config import GEMINI_API_KEY, GEMINI_MODEL


FALLBACK_GEMINI_MODELS = (
    "gemini-2.0-flash",
    "gemini-flash-latest",
)

gemini_client = genai.Client(api_key=GEMINI_API_KEY)


def _extract_text_from_gemini_response(resp) -> str:
    text = getattr(resp, "output_text", "") or getattr(resp, "text", "")
    if text:
        return text.strip()

    if hasattr(resp, "candidates") and resp.candidates:
        parts = resp.candidates[0].content.parts
        return "".join(getattr(p, "text", "") for p in parts if getattr(p, "text", None)).strip()

    return ""


def _model_candidates(primary: Optional[str] = None) -> Iterable[str]:
    seen = set()
    for name in (primary, GEMINI_MODEL, *FALLBACK_GEMINI_MODELS):
        if not name or name in seen:
            continue
        seen.add(name)
        yield name


def gemini_text(prompt: str, model: Optional[str] = None) -> str:
    last_error = None
    for candidate in _model_candidates(model):
        try:
            resp = gemini_client.models.generate_content(
                model=candidate,
                contents=[prompt],
            )
            text = _extract_text_from_gemini_response(resp)
            if text:
                return text
        except Exception as exc:
            last_error = exc

    if last_error:
        raise last_error
    raise RuntimeError("Gemini returned an empty response")


def analyze_exercise_video(file_path: str) -> Tuple[str, str]:
    """
    Returns (label, ex_type)
    label - exercise | talking_head | unclear
    ex_type - squats | pushups | plank | running | jumping | stretching | other
    """
    with open(file_path, "rb") as f:
        video_bytes = f.read()

    video_part = genai_types.Part.from_bytes(
        data=video_bytes,
        mime_type="video/mp4",
    )

    prompt_text = (
        "Посмотри это видео. Определи, делает ли человек физическое упражнение.\n"
        "Если да, то отнеси упражнение к одной из категорий:\n"
        "- squats (приседания)\n"
        "- pushups (отжимания)\n"
        "- plank (планка)\n"
        "- running (бег или быстрая ходьба на месте)\n"
        "- jumping (прыжки, выпрыгивания)\n"
        "- stretching (растяжка, йога)\n"
        "- other (другое упражнение)\n\n"
        "Верни ответ в формате:\n"
        "label: exercise|talking_head|unclear\n"
        "type: squats|pushups|plank|running|jumping|stretching|other\n"
        "Без объяснений, только две строки."
    )

    resp = None
    last_error = None
    for candidate in _model_candidates():
        try:
            resp = gemini_client.models.generate_content(
                model=candidate,
                contents=[video_part, prompt_text],
            )
            break
        except Exception as exc:
            last_error = exc

    if resp is None:
        if last_error:
            raise last_error
        raise RuntimeError("Gemini video analysis failed")

    text = _extract_text_from_gemini_response(resp).lower().strip()

    label_match = re.search(r"label:\s*([a-z_]+)", text)
    type_match = re.search(r"type:\s*([a-z_]+)", text)

    label = label_match.group(1) if label_match else "unclear"
    ex_type = type_match.group(1) if type_match else "other"

    if label not in ("exercise", "talking_head", "unclear"):
        label = "unclear"

    if ex_type not in ("squats", "pushups", "plank", "running", "jumping", "stretching", "other"):
        ex_type = "other"

    return label, ex_type
