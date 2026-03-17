import logging
import re
from typing import List

from aiogram import Dispatcher, types

from .. import db_help
from ..gemini_utils import gemini_text
from ..keyboards import BTN_BOOK, build_main_kb
from ..storage import user_state


def _normalize_question_line(line: str) -> str:
    line = line.strip()
    line = re.sub(r"^\s*[-*•]\s*", "", line)
    line = re.sub(r"^\s*\d+[.)]\s*", "", line)
    return line.strip(" \"'")


def _extract_questions(text: str) -> List[str]:
    questions: List[str] = []
    for raw_line in text.splitlines():
        line = _normalize_question_line(raw_line)
        if not line:
            continue
        if "вопрос" in line.lower() and ":" in line:
            line = line.split(":", 1)[1].strip()
        if "?" not in line:
            continue
        if line not in questions:
            questions.append(line)
        if len(questions) == 3:
            return questions
    return questions


def generate_questions_for_book(book_name: str) -> List[str]:
    prompt = (
        f'Сгенерируй 3 простых, но важных вопроса по содержанию книги "{book_name}".\n\n'
        "Требования к вопросам:\n"
        "- вопросы должны быть по ключевым моментам сюжета, а не по мелким деталям\n"
        "- один вопрос должен отражать начало книги, один середину, один ближе к финалу\n"
        "- вопросы должны быть короткими, понятными и проверять факт чтения\n"
        "- не спрашивай даты, имена второстепенных персонажей, номера глав и цитаты\n"
        "- избегай философских и аналитических вопросов, только фактические события\n\n"
        "Верни только 3 вопроса, каждый на новой строке, без нумерации и без пояснений."
    )

    text = gemini_text(prompt)
    questions = _extract_questions(text)
    if len(questions) >= 3:
        return questions[:3]

    retry_text = gemini_text(
        prompt
        + "\n\n"
        + "Верни ровно три коротких вопроса. Каждый вопрос должен заканчиваться знаком вопроса. Без заголовков и лишнего текста."
    )
    questions = _extract_questions(retry_text)
    if len(questions) >= 3:
        return questions[:3]

    raise ValueError("Не удалось сгенерировать 3 корректных вопроса по книге")


def check_user_answers(book_name: str, questions: List[str], answers: List[str]) -> str:
    q1, q2, q3 = questions
    a1, a2, a3 = answers

    prompt = f"""Вот 3 вопроса по книге "{book_name}" и ответы пользователя.

Твоя задача - максимально доброжелательно оценить ответы.

Очень важно:
- НЕ докапывайся до мелочей
- Если пользователь отвечает общими словами, но смысл события передан правильно - считай ответ ПРАВИЛЬНЫМ
- Не требуй точных цитат, имен, дат, второстепенных персонажей или мелких деталей
- Ошибкой считается только ситуация, когда ответ полностью не соответствует содержанию книги

Вопросы и ответы:

Вопрос 1: {q1}
Ответ 1: {a1}

Вопрос 2: {q2}
Ответ 2: {a2}

Вопрос 3: {q3}
Ответ 3: {a3}

1) Для каждого ответа определи одно слово:
- правильно
- частично
- неправильно

2) На основе трех оценок выбери один общий итог:
- если большинство ответов правильные -> вердикт 1:
  Да, все верно, ты действительно прочитал книгу! Отличная работа!
- если большинство ответов частично правильные -> вердикт 2:
  Ты ответил частично верно. Возможно, стоит немного перечитать - но ты молодец, так держать!
- если большинство ответов неправильные -> вердикт 3:
  Похоже, стоит перечитать книгу. Но ты огромный молодец, что проходишь проверку!

Верни результат строго в виде:

Результаты по книге "{book_name}":

Ответы:
- Вопрос 1 - правильно или частично или неправильно
- Вопрос 2 - правильно или частично или неправильно
- Вопрос 3 - правильно или частично или неправильно

Итог:
[ОДНА из трех фраз вердикта выше]

Не добавляй никаких объяснений или рассуждений, только этот блок.
"""
    return gemini_text(prompt)


def _books_step(user_id: int) -> str | None:
    st = user_state.get(user_id)
    if not st:
        return None
    return st.get("step")


def register(dp: Dispatcher) -> None:
    @dp.message_handler(lambda m: m.text == BTN_BOOK)
    async def handle_book_button(message: types.Message):
        user_id = message.from_user.id
        user_state[user_id] = {"step": "waiting_book"}
        await message.answer("Введи название книги.")

    @dp.message_handler(
        lambda m: _books_step(m.from_user.id) in ("waiting_book", "asking_questions"),
        content_types=["text"],
    )
    async def handle_books_text_router(message: types.Message):
        user_id = message.from_user.id
        state = user_state.get(user_id)
        if not state:
            return

        if state.get("step") == "waiting_book":
            book_name = message.text.strip()
            if not book_name:
                await message.answer("Пожалуйста, введи название книги текстом.")
                return

            await message.answer(f"Генерирую вопросы по книге {book_name}...")

            try:
                questions = generate_questions_for_book(book_name)
            except Exception as e:
                logging.exception("Ошибка при генерации вопросов по книге: %s", e)
                await message.answer("Не удалось сгенерировать вопросы. Попробуй позже.")
                user_state[user_id] = None
                return

            user_state[user_id] = {
                "step": "asking_questions",
                "book": book_name,
                "questions": questions,
                "answers": [],
                "current_q": 0,
            }

            await message.answer("Сейчас я задам 3 вопроса. Отвечай так, как помнишь.")
            await message.answer(f"Вопрос 1:\n{questions[0]}")
            return

        if state.get("step") == "asking_questions":
            book_name = state["book"]
            questions: List[str] = state["questions"]
            answers: List[str] = state["answers"]
            current_q: int = state["current_q"]

            answers.append(message.text.strip())
            current_q += 1
            state["current_q"] = current_q

            if current_q < len(questions):
                await message.answer(f"Вопрос {current_q + 1}:\n{questions[current_q]}")
                return

            await message.answer("Спасибо! Проверяю твои ответы...")

            try:
                result_text = check_user_answers(book_name, questions, answers)
            except Exception as e:
                logging.exception("Ошибка при проверке ответов по книге: %s", e)
                await message.answer("Во время проверки произошла ошибка. Попробуй позже.")
                user_state[user_id] = None
                return

            db_help.save_book_record(
                user_id=user_id,
                book_name=book_name,
                questions=questions,
                answers=answers,
                result_text=result_text,
                username=message.from_user.username,
                full_name=message.from_user.full_name,
            )
            await message.answer(result_text, reply_markup=build_main_kb())
            user_state[user_id] = None
