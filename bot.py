import logging
import re
import os
import datetime
import requests
from collections import defaultdict
from typing import Dict, Any, List, Tuple

from aiogram import Bot, Dispatcher, executor, types
from aiogram.types import ReplyKeyboardMarkup, KeyboardButton

# ✔ Gemini
import google.genai as genai
from google.genai import types as genai_types

# ================== НАСТРОЙКИ ==================

TELEGRAM_BOT_TOKEN = "8210404148:AAHmpRufaYWl2PxJj0tb7BlXX6T7bXDcd_E"
GEMINI_API_KEY = "AIzaSyCUBXSrQSfEkHSN6s-c4pg0d-VkGhema5U"
GEMINI_MODEL = "gemini-2.0-flash"

print("Использую модель Gemini:", GEMINI_MODEL)

# ================== ИНИЦИАЛИЗАЦИЯ ==================

logging.basicConfig(level=logging.INFO)

bot = Bot(token=TELEGRAM_BOT_TOKEN)
dp = Dispatcher(bot)

gemini_client = genai.Client(api_key=GEMINI_API_KEY)

# Кнопки
BTN_BOOK = "📘 Прочитал"
BTN_EXERCISE = "🏋 Сделал упражнение"
BTN_STATS = "📊 Статистика"

kb_main = ReplyKeyboardMarkup(resize_keyboard=True)
kb_main.add(KeyboardButton(BTN_BOOK))
kb_main.add(KeyboardButton(BTN_EXERCISE))
kb_main.add(KeyboardButton(BTN_STATS))

# user_state[user_id] = {...}
user_state: Dict[int, Dict[str, Any]] = {}

# exercise_log[user_id][date_str] = [ {name, amount, unit, timestamp}, ... ]
exercise_log: Dict[int, Dict[str, List[Dict[str, Any]]]] = {}

# ================== ВСПОМОГАТЕЛЬНЫЕ ФУНКЦИИ ДЛЯ GEMINI ==================

def _extract_text_from_gemini_response(resp) -> str:
    """
    Универсально достать текст из ответа Gemini (для разных версий библиотеки).
    """
    text = getattr(resp, "output_text", "") or getattr(resp, "text", "")
    if text:
        return text.strip()

    if hasattr(resp, "candidates") and resp.candidates:
        parts = resp.candidates[0].content.parts
        return "".join(getattr(p, "text", "") for p in parts if getattr(p, "text", None)).strip()

    return ""


def gemini_text(prompt: str) -> str:
    """Обёртка для простых текстовых запросов к Gemini."""
    resp = gemini_client.models.generate_content(
        model=GEMINI_MODEL,
        contents=[prompt],
    )
    return _extract_text_from_gemini_response(resp)


# ================== ВСПОМОГАТЕЛЬНЫЕ ФУНКЦИИ (КНИГИ) ==================


def generate_questions_for_book(book_name: str) -> List[str]:
    """
    Сгенерировать 3 простых, но важных вопроса по книге.
    """
    prompt = f"""Сгенерируй 3 простых, но важных вопросов по содержанию книги «{book_name}».

Требования к вопросам:
- вопросы должны быть по ключевым моментам сюжета (не по мелким деталям);
- один вопрос должен отражать начало книги, один — середину, один — ближе к финалу;
- вопросы должны быть короткими, понятными и направленными на проверку факта чтения;
- не спрашивай даты, имена второстепенных персонажей, номера глав, цитаты;
- избегай философских и аналитических вопросов — только фактические события.

Выведи только 3 строки — 3 вопроса, без нумерации и без лишнего текста.
"""

    text = gemini_text(prompt)
    lines = [line.strip() for line in text.split("\n") if line.strip()]
    if len(lines) >= 3:
        return lines[:3]
    raise ValueError("Не удалось сгенерировать 3 корректных вопроса по книге")


def check_user_answers(book_name: str, questions: List[str], answers: List[str]) -> str:
    """
    Проверка ответов по книге. Возвращает уже готовый текст результата.
    """
    q1, q2, q3 = questions
    a1, a2, a3 = answers

    prompt = f"""Вот 3 вопроса по книге «{book_name}» и ответы пользователя.

Твоя задача — максимально доброжелательно оценить ответы.

Очень важно:
- НЕ докапывайся до мелочей.
- Если пользователь отвечает общими словами, но смысл события передан правильно — считай ответ ПРАВИЛЬНЫМ.
- Не требуй точных цитат, имён, дат, второстепенных персонажей или мелких деталей.
- Ошибкой считается только ситуация, когда ответ полностью не соответствует содержанию книги.

Вопросы и ответы:

Вопрос 1: {q1}
Ответ 1: {a1}

Вопрос 2: {q2}
Ответ 2: {a2}

Вопрос 3: {q3}
Ответ 3: {a3}

1) Для каждого ответа определи одно слово:
- «правильно»
- «частично»
- «неправильно».

«Правильно» = ключевая мысль совпадает с содержанием книги, даже если нет деталей.
«Частично» = ответ содержит правильные элементы, но пропущены некоторые ключевые моменты.
«Неправильно» = ответ не связан с содержанием книги или противоречит ей.

2) На основе трёх оценок выбери один общий итог:
- если большинство ответов правильные → вердикт 1:
  «Да, всё верно, ты действительно прочитал книгу! Отличная работа!»
- если большинство ответов частично правильные → вердикт 2:
  «Ты ответил частично верно. Возможно, стоит немного перечитать — но ты молодец, так держать!»
- если большинство ответов неправильные → вердикт 3:
  «Похоже, стоит перечитать книгу. Но ты огромный молодец, что проходишь проверку!»

Верни результат строго в виде:

Результаты по книге «{book_name}»:

Ответы:
• Вопрос 1 — правильно/частично/неправильно
• Вопрос 2 — правильно/частично/неправильно
• Вопрос 3 — правильно/частично/неправильно

Итог:
[ОДНА из трёх фраз вердикта выше, без дополнительных комментариев]

Не добавляй никаких объяснений или рассуждений, только этот блок.
"""

    return gemini_text(prompt)


# ================== ВСПОМОГАТЕЛЬНЫЕ ФУНКЦИИ (УПРАЖНЕНИЯ) ==================


def parse_exercise(text: str) -> Tuple[str, int, str] | None:
    """
    Парсим строку вида:
    'отжимания 15'
    'планка 30 секунд'
    'бег 10 минут'
    Возвращаем (name, amount, unit) или None.
    """
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
        "йога": "yoga",
        "прыжки": "jumping",
    }

    for key, value in mapping.items():
        if key in name:
            return value

    return "unknown"


def save_exercise(user_id: int, name: str, amount: int, unit: str) -> None:
    """Сохраняем упражнение в exercise_log по датам."""
    date_str = datetime.date.today().isoformat()
    rec = {
        "name": name,
        "amount": amount,
        "unit": unit,
        "timestamp": datetime.datetime.now().isoformat(timespec="seconds"),
    }
    user_days = exercise_log.setdefault(user_id, {})
    day_list = user_days.setdefault(date_str, [])
    day_list.append(rec)


def format_exercise_stats(user_id: int) -> str:
    """Формируем текст статистики по упражнениям."""
    if user_id not in exercise_log or not exercise_log[user_id]:
        return "Пока нет ни одной записи с упражнениями."

    user_days = exercise_log[user_id]
    today_str = datetime.date.today().isoformat()

    # Суммы за сегодня
    today_totals = defaultdict(int)
    for rec in user_days.get(today_str, []):
        key = (rec["name"], rec["unit"])
        today_totals[key] += rec["amount"]

    # Суммы за всё время
    total_totals = defaultdict(int)
    for day, recs in user_days.items():
        for rec in recs:
            key = (rec["name"], rec["unit"])
            total_totals[key] += rec["amount"]

    lines: List[str] = []

    lines.append(f"📅 Сегодня ({today_str}):")
    if today_totals:
        for (name, unit), amount in today_totals.items():
            lines.append(f"• {name} — {amount} {unit}")
    else:
        lines.append("• пока нет упражнений за сегодня")

    lines.append("")
    lines.append("📊 Всего за всё время:")
    for (name, unit), amount in total_totals.items():
        lines.append(f"• {name} — {amount} {unit}")

    return "\n".join(lines)


# ================== ВИДЕО: ВЫДЕЛИТЬ ТИП УПРАЖНЕНИЯ ==================


def extract_exercise_type_from_video(file_path: str) -> Tuple[str, str]:
    """
    Возвращает (label, ex_type):

    label:
        - exercise
        - talking_head
        - unclear

    ex_type:
        - squats / pushups / plank / running / jumping / stretching / other
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
        "label: [exercise/talking_head/unclear]\n"
        "type: [squats/pushups/plank/running/jumping/stretching/other]\n"
        "Без объяснений, только две строки."
    )

    resp = gemini_client.models.generate_content(
        model=GEMINI_MODEL,
        contents=[video_part, prompt_text],
    )

    text = _extract_text_from_gemini_response(resp).lower()
    # На всякий случай уберём пробелы по краям
    text = text.strip()

    # Пример ответа:
    # label: exercise
    # type: squats
    label_match = re.search(r"label:\s*([a-z_]+)", text)
    type_match = re.search(r"type:\s*([a-z_]+)", text)

    label = label_match.group(1) if label_match else "unclear"
    ex_type = type_match.group(1) if type_match else "other"

    return label, ex_type


def classify_video_exercise(file_path: str) -> Tuple[str, str]:
    """
    Обёртка над extract_exercise_type_from_video:
    гарантируем, что label всегда один из трёх.
    """
    label, ex_type = extract_exercise_type_from_video(file_path)

    if label not in ("exercise", "talking_head", "unclear"):
        label = "unclear"

    return label, ex_type


# ================== ХЕНДЛЕРЫ ==================


@dp.message_handler(commands=["start"])
async def handle_start(message: types.Message):
    user_id = message.from_user.id
    user_state[user_id] = None

    text = (
        "Привет! Я могу:\n"
        f"• проверить, читал ли ты книгу (кнопка «{BTN_BOOK}»);\n"
        f"• записать твоё физическое упражнение (кнопка «{BTN_EXERCISE}»);\n"
        f"• показать статистику по упражнениям (кнопка «{BTN_STATS}»).\n\n"
        "Выбери действие ниже."
    )
    await message.answer(text, reply_markup=kb_main)


@dp.message_handler(lambda m: m.text == BTN_BOOK)
async def handle_book_button(message: types.Message):
    user_id = message.from_user.id
    user_state[user_id] = {"step": "waiting_book"}
    await message.answer("Введи название книги.")


@dp.message_handler(lambda m: m.text == BTN_EXERCISE)
async def handle_exercise_button(message: types.Message):
    user_id = message.from_user.id
    user_state[user_id] = {"step": "waiting_exercise_text"}
    await message.answer(
        "Напиши, что ты сделал(а).\n\n"
        "Формат: упражнение + количество + (если нужно) время.\n"
        "Примеры:\n"
        "• отжимания 15\n"
        "• планка 30 секунд\n"
        "• бег 10 минут",
        reply_markup=kb_main,
    )


@dp.message_handler(lambda m: m.text == BTN_STATS)
async def handle_stats(message: types.Message):
    user_id = message.from_user.id
    text = format_exercise_stats(user_id)
    await message.answer(text, reply_markup=kb_main)


@dp.message_handler(content_types=["video", "video_note", "document", "animation"])
async def handle_exercise_video(message: types.Message):
    """Обрабатываем видео только если мы его ждём для упражнения."""
    user_id = message.from_user.id
    state = user_state.get(user_id)

    if not state or state.get("step") != "waiting_exercise_video":
        return  # видео пришло не в тот момент — игнорируем

    name = state.get("exercise_name")
    amount = state.get("exercise_amount")
    unit = state.get("exercise_unit")

    if not (name and amount and unit):
        await message.answer(
            "Что-то пошло не так с разбором упражнения. "
            "Попробуй начать заново: нажми «🏋 Сделал упражнение».",
            reply_markup=kb_main,
        )
        user_state[user_id] = None
        return

    # 1. Получаем file_id из сообщения
    file_id = None
    if message.video:
        file_id = message.video.file_id
    elif message.video_note:
        file_id = message.video_note.file_id
    elif message.document and message.document.mime_type.startswith("video/"):
        file_id = message.document.file_id
    elif message.animation:  # гифка / mp4-анимация
        file_id = message.animation.file_id

    if not file_id:
        await message.answer(
            "Я вижу файл, но он не похож на видео. Попробуй отправить именно видео, "
            "а не документ.",
            reply_markup=kb_main,
        )
        user_state[user_id] = None
        return

    # 2. Узнаём file_path у Telegram
    tg_file = await bot.get_file(file_id)
    file_url = f"https://api.telegram.org/file/bot{TELEGRAM_BOT_TOKEN}/{tg_file.file_path}"

    # 3. Скачиваем видео во временный файл
    tmp_path = "temp_exercise_video.mp4"
    try:
        r = requests.get(file_url)
        r.raise_for_status()
        with open(tmp_path, "wb") as f:
            f.write(r.content)
    except Exception as e:
        logging.exception("Ошибка при скачивании видео из Telegram: %s", e)
        await message.answer(
            "Не удалось скачать видео с серверов Telegram. "
            "Попробуй отправить его ещё раз.",
            reply_markup=kb_main,
        )
        user_state[user_id] = None
        return

    await message.answer("Смотрю видео, определяю, что на нём...")

    # 4. Классифицируем видео через Gemini
    try:
        label, ex_type = classify_video_exercise(tmp_path)
    except Exception as e:
        logging.exception("Ошибка при анализе видео в Gemini: %s", e)
        await message.answer(
            "Ошибка при анализе видео. Я не засчитаю упражнение.\n"
            "Попробуй снять покороче и так, чтобы было хорошо видно движение.",
            reply_markup=kb_main,
        )
        user_state[user_id] = None
    else:
        expected = normalize_user_exercise_type(name)

        # ❌ 1. Если НЕ упражнение — не засчитываем
        if label != "exercise":
            await message.answer(
                "По видео не видно упражнения.\n"
                "Попробуй снять так, чтобы было видно всё тело.",
                reply_markup=kb_main,
            )
            user_state[user_id] = None
        # ❌ 2. Тип упражнения не совпал
        elif ex_type != expected and expected != "unknown":
            await message.answer(
                f"Ты написал: {name}\n"
                f"А на видео Gemini видит: {ex_type}\n\n"
                "Я не могу засчитать это упражнение.",
                reply_markup=kb_main,
            )
            user_state[user_id] = None
        else:
            # ✅ 3. Всё правильно — засчитываем
            save_exercise(user_id, name, amount, unit)
            await message.answer(
                f"✅ Я записал: {name} — {amount} {unit}\n"
                "💪 Отлично поработал!",
                reply_markup=kb_main,
            )
            user_state[user_id] = None
    finally:
        try:
            os.remove(tmp_path)
        except OSError:
            pass


@dp.message_handler(content_types=["text"])
async def handle_text(message: types.Message):
    user_id = message.from_user.id
    state = user_state.get(user_id)

    # 1) Ждём название книги
    if state and state.get("step") == "waiting_book":
        book_name = message.text.strip()
        if not book_name:
            await message.answer("Пожалуйста, введи название книги текстом.")
            return

        await message.answer(f"Генерирую вопросы по книге «{book_name}»...")

        try:
            questions = generate_questions_for_book(book_name)
        except Exception as e:
            logging.exception("Ошибка при генерации вопросов по книге: %s", e)
            await message.answer(
                "Не удалось сгенерировать вопросы по этой книге. "
                "Попробуй ещё раз позже или с другой книгой.",
                reply_markup=kb_main,
            )
            user_state[user_id] = None
            return

        user_state[user_id] = {
            "step": "asking_questions",
            "book": book_name,
            "questions": questions,
            "answers": [],
            "current_q": 0,
        }

        await message.answer(
            "Сейчас я задам 3 вопроса по этой книге. Отвечай так, как помнишь."
        )
        await message.answer(f"Вопрос 1:\n{questions[0]}")
        return

    # 2) Вопросы по книге
    if state and state.get("step") == "asking_questions":
        book_name = state["book"]
        questions: List[str] = state["questions"]
        answers: List[str] = state["answers"]
        current_q: int = state["current_q"]

        user_answer = message.text.strip()
        answers.append(user_answer)
        current_q += 1
        state["current_q"] = current_q

        if current_q < len(questions):
            next_q_num = current_q + 1
            await message.answer(f"Вопрос {next_q_num}:\n{questions[current_q]}")
            return

        await message.answer("Спасибо! Проверяю твои ответы...")

        try:
            result_text = check_user_answers(book_name, questions, answers)
        except Exception as e:
            logging.exception("Ошибка при проверке ответов по книге: %s", e)
            await message.answer(
                "Во время проверки ответов произошла ошибка. "
                "Попробуй, пожалуйста, ещё раз позже.",
                reply_markup=kb_main,
            )
            user_state[user_id] = None
            return

        await message.answer(result_text, reply_markup=kb_main)
        user_state[user_id] = None
        return

    # 3) Описание упражнения (текст)
    if state and state.get("step") == "waiting_exercise_text":
        parsed = parse_exercise(message.text.strip())
        if not parsed:
            await message.answer(
                "Я не понял формат.\n\n"
                "Примеры:\n"
                "• отжимания 15\n"
                "• планка 30 секунд\n"
                "• бег 10 минут",
                reply_markup=kb_main,
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
            f"Я понял так:\n{name} — {amount} {unit}\n\n"
            "Теперь пришли ВИДЕО этого упражнения. Без видео я не засчитываю.",
            reply_markup=kb_main,
        )
        return

    # Всё остальное — вне сценария
    await message.answer(
        "Чтобы начать, выбери действие на клавиатуре:\n"
        f"• «{BTN_BOOK}» — про книги\n"
        f"• «{BTN_EXERCISE}» — упражнения\n"
        f"• «{BTN_STATS}» — статистика",
        reply_markup=kb_main,
    )


# ================== ЗАПУСК ==================

if __name__ == "__main__":
    if TELEGRAM_BOT_TOKEN == "YOUR_TELEGRAM_BOT_TOKEN":
        logging.warning("Не забудь вставить TELEGRAM_BOT_TOKEN и GEMINI_API_KEY!")
    executor.start_polling(dp, skip_updates=True)

