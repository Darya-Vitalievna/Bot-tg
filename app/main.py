import logging

from aiogram import Bot, Dispatcher, executor, types
from aiogram.dispatcher.handler import SkipHandler

from .config import TELEGRAM_BOT_TOKEN
from .db_help import init_help_db
from .keyboards import (
    build_main_kb,
    BTN_BOOK,
    BTN_EXERCISE,
    BTN_STATS,
    BTN_HELP,
    BTN_HELP_REQUEST,
    BTN_HELP_ANSWER,
    BTN_MY_REQUESTS,
    BTN_APPLY_EXPERT,
    BTN_ADMIN_EXPERTS,
    BTN_BACK_MAIN,
)

from .features import books, exercises, help_expert


logging.basicConfig(level=logging.INFO)
log = logging.getLogger(__name__)

bot = Bot(token=TELEGRAM_BOT_TOKEN, parse_mode="HTML")
dp = Dispatcher(bot)

init_help_db()

# КРИТИЧНО: регистрируем help_expert ПЕРВЫМ
help_expert.register(dp, bot)

# Эти два не трогаем, просто регистрируем после help_expert
books.register(dp)
exercises.register(dp, TELEGRAM_BOT_TOKEN, bot)


HELP_TEXTS = {
    BTN_HELP,
    BTN_HELP_REQUEST,
    BTN_HELP_ANSWER,
    BTN_MY_REQUESTS,
    BTN_APPLY_EXPERT,
    BTN_ADMIN_EXPERTS,
    BTN_BACK_MAIN,
}


@dp.message_handler(commands=["start"])
async def cmd_start(message: types.Message):
    await message.answer("Выбери действие", reply_markup=build_main_kb())


@dp.message_handler()
async def fallback(message: types.Message):
    text = (message.text or "").strip()

    # Подсистему "Помощь эксперта" не трогаем здесь
    if text in HELP_TEXTS:
        raise SkipHandler()

    await message.answer("Выбери действие", reply_markup=build_main_kb())


if __name__ == "__main__":
    if not TELEGRAM_BOT_TOKEN:
        raise RuntimeError("TELEGRAM_BOT_TOKEN пустой. Заполни его в app/config.py.")
    executor.start_polling(dp, skip_updates=True)
