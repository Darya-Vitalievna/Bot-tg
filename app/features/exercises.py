import logging
import re
import os
import datetime
import requests
from collections import defaultdict
from typing import Dict, Any, List, Tuple

from aiogram import Dispatcher, types

from ..keyboards import BTN_EXERCISE, BTN_STATS, build_main_kb
from ..storage import user_state, exercise_log
from ..gemini_utils import analyze_exercise_video


def parse_exercise(text: str) -> Tuple[str, int, str] | None:
    pattern = r"^\s*(?P<name>[^\d]+?)\s+(?P<num>\d+)(\s+(?P<unit>\S+))?\s*$"
    m = re.match(pattern, text, flags=re.IGNORECASE)
    if not m:
        return None

    name = m.group("name").strip()
    amount = int(m.group("num"))
    unit = (m.group("unit") or "").strip().lower()

    if not unit:
        unit = "раз"
    else:
        if unit.startswith("сек"):
            unit = "секунд"
        elif unit.startswith("мин"):
            unit = "минут"
        elif unit.startswith("час"):
            unit = "часов"
        elif unit in ("раз", "раза", "разы", "повторений", "повторения"):
            unit = "раз"

    return name, amount, unit


def normalize_user_exercise_type(name: str) -> str:
    name = name.lower().strip()
    mapping = {
        "присед": "squats",
        "приседания": "squats",
        "отжимания": "pushups",
        "отжим": "pushups",
        "бег": "running",
        "планка": "plank",
        "йога": "stretching",
        "растяж": "stretching",
        "прыжки": "jumping",
        "выпрыг": "jumping",
    }

    for key, value in mapping.items():
        if key in name:
            return value

    return "unknown"


def save_exercise(user_id: int, name: str, amount: int, unit: str) -> None:
    date_str = datetime.date.today().isoformat()
    rec = {
        "name": name,
        "amount": amount,
        "unit": unit,
        "timestamp": datetime.datetime.now().isoformat(timespec="seconds"),
    }
    exercise_log[user_id][date_str].append(rec)


def format_exercise_stats(user_id: int) -> str:
    if user_id not in exercise_log or not exercise_log[user_id]:
        return "Пока нет ни одной записи с упражнениями."

    user_days = exercise_log[user_id]
    today_str = datetime.date.today().isoformat()

    today_totals = defaultdict(int)
    for rec in user_days.get(today_str, []):
        key = (rec["name"], rec["unit"])
        today_totals[key] += rec["amount"]

    total_totals = defaultdict(int)
    for _, recs in user_days.items():
        for rec in recs:
            key = (rec["name"], rec["unit"])
            total_totals[key] += rec["amount"]

    lines: List[str] = []
    lines.append(f"Сегодня ({today_str}):")
    if today_totals:
        for (name, unit), amount in today_totals.items():
            lines.append(f"- {name} - {amount} {unit}")
    else:
        lines.append("- пока нет упражнений за сегодня")

    lines.append("")
    lines.append("Всего за всё время:")
    for (name, unit), amount in total_totals.items():
        lines.append(f"- {name} - {amount} {unit}")

    return "\n".join(lines)


def _ex_step(user_id: int) -> str | None:
    st = user_state.get(user_id)
    if not st:
        return None
    return st.get("step")


def register(dp: Dispatcher, bot_token: str, bot) -> None:
    @dp.message_handler(lambda m: m.text == BTN_EXERCISE)
    async def handle_exercise_button(message: types.Message):
        user_id = message.from_user.id
        user_state[user_id] = {"step": "waiting_exercise_text"}
        await message.answer(
            "Напиши, что ты сделал(а).\n"
            "Формат: упражнение + количество + (если нужно) время.\n"
            "Примеры:\n"
            "- отжимания 15\n"
            "- планка 30 секунд\n"
            "- бег 10 минут",
        )

    @dp.message_handler(lambda m: m.text == BTN_STATS)
    async def handle_stats(message: types.Message):
        user_id = message.from_user.id
        await message.answer(format_exercise_stats(user_id), reply_markup=build_main_kb())

    @dp.message_handler(content_types=["video", "video_note", "document", "animation"])
    async def handle_exercise_video(message: types.Message):
        user_id = message.from_user.id
        state = user_state.get(user_id)

        if not state or state.get("step") != "waiting_exercise_video":
            return

        name = state.get("exercise_name")
        amount = state.get("exercise_amount")
        unit = state.get("exercise_unit")

        if not (name and amount and unit):
            await message.answer("Что-то пошло не так. Нажми Сделал упражнение и начни заново.", reply_markup=build_main_kb())
            user_state[user_id] = None
            return

        file_id = None
        if message.video:
            file_id = message.video.file_id
        elif message.video_note:
            file_id = message.video_note.file_id
        elif message.document and (message.document.mime_type or "").startswith("video/"):
            file_id = message.document.file_id
        elif message.animation:
            file_id = message.animation.file_id

        if not file_id:
            await message.answer("Файл не похож на видео. Отправь именно видео.", reply_markup=build_main_kb())
            user_state[user_id] = None
            return

        tg_file = await bot.get_file(file_id)
        file_url = f"https://api.telegram.org/file/bot{bot_token}/{tg_file.file_path}"

        tmp_path = "temp_exercise_video.mp4"
        try:
            r = requests.get(file_url, timeout=60)
            r.raise_for_status()
            with open(tmp_path, "wb") as f:
                f.write(r.content)
        except Exception as e:
            logging.exception("Ошибка при скачивании видео из Telegram: %s", e)
            await message.answer("Не удалось скачать видео. Попробуй отправить ещё раз.", reply_markup=build_main_kb())
            user_state[user_id] = None
            return

        await message.answer("Смотрю видео, определяю, что на нём...")

        try:
            label, ex_type = analyze_exercise_video(tmp_path)
        except Exception as e:
            logging.exception("Ошибка при анализе видео в Gemini: %s", e)
            await message.answer(
                "Ошибка при анализе видео. Я не засчитаю упражнение.\n"
                "Попробуй снять покороче и так, чтобы было хорошо видно движение.",
                reply_markup=build_main_kb(),
            )
            user_state[user_id] = None
        else:
            expected = normalize_user_exercise_type(name)

            if label != "exercise":
                await message.answer("По видео не видно упражнения. Сними так, чтобы было видно всё тело.", reply_markup=build_main_kb())
                user_state[user_id] = None
            elif expected != "unknown" and ex_type != expected:
                await message.answer(
                    f"Ты написал: {name}\n"
                    f"А на видео Gemini видит: {ex_type}\n"
                    "Я не могу засчитать это упражнение.",
                    reply_markup=build_main_kb(),
                )
                user_state[user_id] = None
            else:
                save_exercise(user_id, name, amount, unit)
                await message.answer(f"Записал: {name} - {amount} {unit}", reply_markup=build_main_kb())
                user_state[user_id] = None
        finally:
            try:
                os.remove(tmp_path)
            except OSError:
                pass

    # ✅ ВАЖНО: текстовый роутер упражнений матчится ТОЛЬКО на шаге waiting_exercise_text
    @dp.message_handler(
        lambda m: _ex_step(m.from_user.id) == "waiting_exercise_text",
        content_types=["text"],
    )
    async def handle_exercise_text_router(message: types.Message):
        user_id = message.from_user.id
        state = user_state.get(user_id)
        if not state:
            return

        parsed = parse_exercise(message.text.strip())
        if not parsed:
            await message.answer(
                "Я не понял формат.\n"
                "Примеры:\n"
                "- отжимания 15\n"
                "- планка 30 секунд\n"
                "- бег 10 минут"
            )
            return

        name, amount, unit = parsed
        user_state[user_id] = {
            "step": "waiting_exercise_video",
            "exercise_name": name,
            "exercise_amount": amount,
            "exercise_unit": unit,
        }

        await message.answer(
            f"Я понял так:\n{name} - {amount} {unit}\n"
            "Теперь пришли видео этого упражнения. Без видео я не засчитываю."
        )
