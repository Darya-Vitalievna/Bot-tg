import re
from typing import Tuple, Optional
import google.genai as genai
from google.genai import types as genai_types

from .config import GEMINI_API_KEY, GEMINI_MODEL

# Фолбэк-модель, которая была в старой рабочей версии бота
FALLBACK_GEMINI_MODEL = "gemini-2.0-flash"

gemini_client = genai.Client(api_key=GEMINI_API_KEY)


def _extract_text_from_gemini_response(resp) -> str:
    text = getattr(resp, "output_text", "") or getattr(resp, "text", "")
    if text:
        return text.strip()

    if hasattr(resp, "candidates") and resp.candidates:
        parts = resp.candidates[0].content.parts
        return "".join(getattr(p, "text", "") for p in parts if getattr(p, "text", None)).strip()

    return ""


def gemini_text(prompt: str, model: Optional[str] = None) -> str:
    """Текстовый запрос к Gemini.

    model:
      - None: сначала GEMINI_MODEL из config, при ошибке - FALLBACK_GEMINI_MODEL
      - str: использовать конкретную модель (без фолбэка)
    """
    if model:
        resp = gemini_client.models.generate_content(
            model=model,
            contents=[prompt],
        )
        return _extract_text_from_gemini_response(resp)

    # 1) Пытаемся основной моделью из конфига
    try:
        resp = gemini_client.models.generate_content(
            model=GEMINI_MODEL,
            contents=[prompt],
        )
        return _extract_text_from_gemini_response(resp)
    except Exception:
        # 2) Если в конфиге указана недоступная/ошибочная модель - пробуем фолбэк
        resp = gemini_client.models.generate_content(
            model=FALLBACK_GEMINI_MODEL,
            contents=[prompt],
        )
        return _extract_text_from_gemini_response(resp)


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

    # Для видео используем модель из конфига, а если она упадет - фолбэк
    try:
        resp = gemini_client.models.generate_content(
            model=GEMINI_MODEL,
            contents=[video_part, prompt_text],
        )
    except Exception:
        resp = gemini_client.models.generate_content(
            model=FALLBACK_GEMINI_MODEL,
            contents=[video_part, prompt_text],
        )

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
