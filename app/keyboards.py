from aiogram.types import (
    ReplyKeyboardMarkup, KeyboardButton,
    InlineKeyboardMarkup, InlineKeyboardButton
)

# ===== Главное меню =====
BTN_BOOK = "📘 Прочитал"
BTN_EXERCISE = "🏋 Сделал упражнение"
BTN_STATS = "📊 Статистика"
BTN_HELP = "🆘 Помощь эксперта"

def build_main_kb() -> ReplyKeyboardMarkup:
    kb = ReplyKeyboardMarkup(resize_keyboard=True)
    kb.add(KeyboardButton(BTN_BOOK))
    kb.add(KeyboardButton(BTN_EXERCISE))
    kb.add(KeyboardButton(BTN_STATS))
    kb.add(KeyboardButton(BTN_HELP))
    return kb


# ===== Подсистема "Помощь эксперта" =====
BTN_HELP_REQUEST = "📝 Запросить помощь"
BTN_HELP_ANSWER = "📥 Ответить на запросы"
BTN_MY_REQUESTS = "📁 Мои запросы"
BTN_APPLY_EXPERT = "🧑‍🏫 Стать экспертом"
BTN_BACK_MAIN = "⬅ В меню"

# Админ-управление экспертами
BTN_ADMIN_EXPERTS = "👑 Управление экспертами"
BTN_ADMIN_ADD_EXPERT = "➕ Добавить эксперта"
BTN_ADMIN_REMOVE_EXPERT = "➖ Удалить эксперта"
BTN_ADMIN_LIST_EXPERTS = "📋 Список экспертов"
BTN_ADMIN_BACK = "⬅ Назад"

# 5 гильдий (как ты просила)
GUILDS = [
    "📚 Учёба / Профориентация",
    "💪 Тренировки / Питание",
    "🧠 Психология / Отношения",
    "💼 Бизнес / Продукты",
    "🎨 Контент / Креатив",
]

def build_help_menu_kb(is_expert: bool, is_admin: bool) -> ReplyKeyboardMarkup:
    kb = ReplyKeyboardMarkup(resize_keyboard=True)
    kb.add(KeyboardButton(BTN_HELP_REQUEST))
    kb.add(KeyboardButton(BTN_MY_REQUESTS))

    if is_expert:
        kb.add(KeyboardButton(BTN_HELP_ANSWER))

    kb.add(KeyboardButton(BTN_APPLY_EXPERT))

    if is_admin:
        kb.add(KeyboardButton(BTN_ADMIN_EXPERTS))

    kb.add(KeyboardButton(BTN_BACK_MAIN))
    return kb

def build_guilds_kb() -> ReplyKeyboardMarkup:
    kb = ReplyKeyboardMarkup(resize_keyboard=True)
    for g in GUILDS:
        kb.add(KeyboardButton(g))
    kb.add(KeyboardButton(BTN_ADMIN_BACK))
    return kb

def build_yes_no_kb() -> ReplyKeyboardMarkup:
    kb = ReplyKeyboardMarkup(resize_keyboard=True)
    kb.add(KeyboardButton("✅ Да"), KeyboardButton("❌ Нет"))
    return kb

def build_admin_experts_kb() -> ReplyKeyboardMarkup:
    kb = ReplyKeyboardMarkup(resize_keyboard=True)
    kb.add(KeyboardButton(BTN_ADMIN_ADD_EXPERT))
    kb.add(KeyboardButton(BTN_ADMIN_REMOVE_EXPERT))
    kb.add(KeyboardButton(BTN_ADMIN_LIST_EXPERTS))
    kb.add(KeyboardButton(BTN_ADMIN_BACK))
    return kb

def inline_my_request_actions(request_id: int) -> InlineKeyboardMarkup:
    kb = InlineKeyboardMarkup()
    kb.add(InlineKeyboardButton("🔎 Открыть", callback_data=f"req_open:{request_id}"))
    kb.add(InlineKeyboardButton("✅ Закрыть", callback_data=f"req_close:{request_id}"))
    return kb

def inline_queue_actions(request_id: int) -> InlineKeyboardMarkup:
    kb = InlineKeyboardMarkup()
    kb.add(InlineKeyboardButton("🔎 Открыть", callback_data=f"queue_open:{request_id}"))
    kb.add(InlineKeyboardButton("✍ Ответить", callback_data=f"queue_reply:{request_id}"))
    return kb
