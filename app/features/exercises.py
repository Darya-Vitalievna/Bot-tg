import datetime
import logging
import os
import re
from collections import defaultdict
from typing import Any, Dict, List, Tuple

import requests
from aiogram import Dispatcher, types

from .. import db_help
from ..gemini_utils import analyze_exercise_video
from ..keyboards import BTN_EXERCISE, BTN_STATS, build_main_kb
from ..storage import exercise_log, user_state


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
    records = db_help.list_exercise_records(user_id)
    book_records = db_help.list_book_records(user_id, limit=1000)

    today = datetime.date.today()
    today_str = today.isoformat()
    month_key = today.strftime("%Y-%m")
    month_label = today.strftime("%m.%Y")

    month_records = [rec for rec in records if str(rec.get("created_at") or "").startswith(month_key)]
    month_book_records = [rec for rec in book_records if str(rec.get("created_at") or "").startswith(month_key)]

    if not month_records and not month_book_records:
        return f"Статистика за {month_label}\n\nПока нет записей за текущий месяц."

    today_totals = defaultdict(int)
    month_totals = defaultdict(int)
    recent_lines: List[str] = []

    for index, rec in enumerate(month_records):
        created_at = str(rec.get("created_at") or "")
        created_day = created_at[:10]
        key = (str(rec.get("exercise_name") or ""), str(rec.get("unit") or ""))
        amount = int(rec.get("amount") or 0)
        month_totals[key] += amount
        if created_day == today_str:
            today_totals[key] += amount
        if index < 5:
            recent_lines.append(
                f"- {created_at[:16].replace('T', ' ')}: {rec['exercise_name']} - {rec['amount']} {rec['unit']}"
            )

    lines: List[str] = [f"Статистика за {month_label}", ""]
    lines.append("Упражнения сегодня:")
    if today_totals:
        for (name, unit), amount in today_totals.items():
            lines.append(f"- {name} - {amount} {unit}")
    else:
        lines.append("- пока нет упражнений за сегодня")

    lines.append("")
    lines.append("Упражнения за текущий месяц:")
    if month_totals:
        for (name, unit), amount in month_totals.items():
            lines.append(f"- {name} - {amount} {unit}")
    else:
        lines.append("- пока нет записей за текущий месяц")

    lines.append("")
    lines.append("Последние упражнения:")
    if recent_lines:
        lines.extend(recent_lines)
    else:
        lines.append("- пока нет упражнений за текущий месяц")

    lines.append("")
    lines.append("Книги за текущий месяц:")
    if month_book_records:
        for rec in month_book_records[:5]:
            created_at = str(rec.get("created_at") or "")[:16].replace("T", " ")
            lines.append(f"- {created_at}: {rec['book_name']}")
    else:
        lines.append("- пока нет книг за текущий месяц")

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
            await message.answer(
                "Что-то пошло не так. Нажми Сделал упражнение и начни заново.",
                reply_markup=build_main_kb(),
            )
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
            await message.answer(
                "Не удалось скачать видео. Попробуй отправить ещё раз.",
                reply_markup=build_main_kb(),
            )
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
                await message.answer(
                    "По видео не видно упражнения. Сними так, чтобы было видно всё тело.",
                    reply_markup=build_main_kb(),
                )
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
                db_help.save_exercise_record(
                    user_id=user_id,
                    exercise_name=name,
                    amount=amount,
                    unit=unit,
                    video_file_id=file_id,
                    username=message.from_user.username,
                    full_name=message.from_user.full_name,
                )
                await message.answer(f"Записал: {name} - {amount} {unit}", reply_markup=build_main_kb())
                user_state[user_id] = None
        finally:
            try:
                os.remove(tmp_path)
            except OSError:
                pass

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
