import logging
from typing import List
from aiogram import Dispatcher, types

from ..gemini_utils import gemini_text
from ..keyboards import BTN_BOOK, build_main_kb
from ..storage import user_state


def generate_questions_for_book(book_name: str) -> List[str]:
    prompt = f"""Сгенерируй 3 простых, но важных вопросов по содержанию книги "{book_name}".

Требования к вопросам:
- вопросы должны быть по ключевым моментам сюжета (не по мелким деталям)
- один вопрос должен отражать начало книги, один - середину, один - ближе к финалу
- вопросы должны быть короткими, понятными и направленными на проверку факта чтения
- не спрашивай даты, имена второстепенных персонажей, номера глав, цитаты
- избегай философских и аналитических вопросов - только фактические события

Выведи только 3 строки - 3 вопроса, без нумерации и без лишнего текста.
"""
    text = gemini_text(prompt)
    lines = [line.strip() for line in text.split("\n") if line.strip()]
    if len(lines) >= 3:
        return lines[:3]
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

    # ✅ ВАЖНО: этот хендлер теперь матчится ТОЛЬКО когда у пользователя шаг книги
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

            await message.answer(result_text, reply_markup=build_main_kb())
            user_state[user_id] = None
            return
