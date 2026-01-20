import logging
import re
from typing import Optional, List, Dict, Any

from aiogram import types
from aiogram.dispatcher import Dispatcher
from aiogram.types import InlineKeyboardMarkup, InlineKeyboardButton

from .. import keyboards as kb
from .. import db_help as db
from ..storage import user_state

log = logging.getLogger(__name__)

def register(dp: Dispatcher, bot):
    # init db
    try:
        init_fn = getattr(db, "init_help_db", None)
        if callable(init_fn):
            init_fn()
    except Exception:
        log.exception("init_help_db failed")

    # ----------------- helpers -----------------
    def set_step(uid: int, step: Optional[str]):
        user_state.setdefault(uid, {})
        user_state[uid]["step"] = step

    def get_step(uid: int) -> Optional[str]:
        return (user_state.get(uid) or {}).get("step")

    def parse_first_int(text: str) -> Optional[int]:
        m = re.search(r"\b(\d{1,10})\b", text or "")
        return int(m.group(1)) if m else None

    async def safe_notify(user_id: int, text: str, reply_markup=None) -> None:
        try:
            await bot.send_message(int(user_id), text, reply_markup=reply_markup)
        except Exception:
            pass

    # Buttons
    BTN_HELP = getattr(kb, "BTN_HELP", "🆘 Помощь эксперта")
    BTN_HELP_REQUEST = getattr(kb, "BTN_HELP_REQUEST", "📝 Запросить помощь")
    BTN_HELP_ANSWER = getattr(kb, "BTN_HELP_ANSWER", "📥 Ответить на запросы")
    BTN_MY_REQUESTS = getattr(kb, "BTN_MY_REQUESTS", "📁 Мои запросы")
    BTN_APPLY_EXPERT = getattr(kb, "BTN_APPLY_EXPERT", "🧑‍🏫 Стать экспертом")
    BTN_BACK_MAIN = getattr(kb, "BTN_BACK_MAIN", "⬅ В меню")
    BTN_ADMIN_EXPERTS = getattr(kb, "BTN_ADMIN_EXPERTS", None)
    BTN_ADMIN_DELETE_REQUEST = getattr(kb, "BTN_ADMIN_DELETE_REQUEST", "🗑 Удалить запрос")

    GUILDS = getattr(kb, "GUILDS", ["Гильдия 1", "Гильдия 2", "Гильдия 3", "Гильдия 4", "Гильдия 5"])
    build_main_kb = getattr(kb, "build_main_kb", None)

    def guild_name(guild_id: int) -> str:
        if 1 <= guild_id <= len(GUILDS):
            return GUILDS[guild_id - 1]
        return f"Гильдия {guild_id}"

    def is_any_menu_button(text: str) -> bool:
        t = (text or "").strip()
        base = {BTN_HELP, BTN_HELP_REQUEST, BTN_HELP_ANSWER, BTN_MY_REQUESTS, BTN_APPLY_EXPERT, BTN_BACK_MAIN}
        if BTN_ADMIN_EXPERTS:
            base.add(BTN_ADMIN_EXPERTS)
        return t in base

    def main_kb():
        if callable(build_main_kb):
            return build_main_kb()
        kb_main = types.ReplyKeyboardMarkup(resize_keyboard=True)
        kb_main.add(types.KeyboardButton(getattr(kb, "BTN_BOOK", "📘 Прочитал")))
        kb_main.add(types.KeyboardButton(getattr(kb, "BTN_EXERCISE", "🏋 Сделал упражнение")))
        kb_main.add(types.KeyboardButton(getattr(kb, "BTN_STATS", "📊 Статистика")))
        kb_main.add(types.KeyboardButton(BTN_HELP))
        return kb_main

    # DB functions (safe references)
    is_expert = getattr(db, "is_expert", lambda _uid: False)
    is_admin = getattr(db, "is_admin", lambda _uid: False)

    create_help_request = getattr(db, "create_help_request", None)
    list_open_requests = getattr(db, "list_open_requests", None)
    list_user_requests = getattr(db, "list_user_requests", None)
    get_request = getattr(db, "get_request", None)

    list_request_answers = getattr(db, "list_answers_for_request", None)
    if not callable(list_request_answers):
        list_request_answers = getattr(db, "list_request_answers", None)

    try_save_answer = getattr(db, "try_save_answer", None)
    if not callable(try_save_answer):
        try_save_answer = getattr(db, "add_answer", None)

    close_request = getattr(db, "close_request", None)
    is_request_closed = getattr(db, "is_request_closed", None)

    get_pending_application = getattr(db, "get_pending_application", None)
    create_expert_application = getattr(db, "create_expert_application", None)
    set_application_status = getattr(db, "set_application_status", None)

    list_experts = getattr(db, "list_experts", None)
    add_expert = getattr(db, "add_expert", None)

    # ----------------- HELP MENU -----------------
    @dp.message_handler(lambda m: m.chat.type == "private" and (m.text or "").strip() == BTN_HELP)
    async def open_help_menu(message: types.Message):
        uid = message.from_user.id
        user_state[uid] = {}

        expert_flag = bool(is_expert(uid))
        admin_flag = bool(is_admin(uid))

        kb_menu = types.ReplyKeyboardMarkup(resize_keyboard=True)
        kb_menu.add(types.KeyboardButton(BTN_HELP_REQUEST))
        if expert_flag:
            kb_menu.add(types.KeyboardButton(BTN_HELP_ANSWER))
        kb_menu.add(types.KeyboardButton(BTN_MY_REQUESTS))
        kb_menu.add(types.KeyboardButton(BTN_APPLY_EXPERT))
        if admin_flag and BTN_ADMIN_EXPERTS:
            kb_menu.add(types.KeyboardButton(BTN_ADMIN_EXPERTS))
        kb_menu.add(types.KeyboardButton(BTN_BACK_MAIN))

        await message.answer("Меню помощи эксперта:", reply_markup=kb_menu)

    @dp.message_handler(lambda m: m.chat.type == "private" and (m.text or "").strip() == BTN_BACK_MAIN)
    async def back_main(message: types.Message):
        uid = message.from_user.id
        user_state[uid] = {}
        await message.answer("Выбери действие", reply_markup=main_kb())

    # ----------------- CREATE HELP REQUEST (student) -----------------
    @dp.message_handler(lambda m: m.chat.type == "private" and (m.text or "").strip() == BTN_HELP_REQUEST)
    async def start_help_request(message: types.Message):
        uid = message.from_user.id
        user_state.setdefault(uid, {})
        user_state[uid]["hr"] = {}
        set_step(uid, "hr_choose_guild")

        ikb = InlineKeyboardMarkup(row_width=1)
        for i, g in enumerate(GUILDS, start=1):
            ikb.add(InlineKeyboardButton(g, callback_data=f"hr_guild:{i}"))

        await message.answer("Выбери гильдию:", reply_markup=ikb)

    @dp.callback_query_handler(lambda c: c.data and c.data.startswith("hr_guild:"))
    async def cb_choose_guild(call: types.CallbackQuery):
        uid = call.from_user.id
        try:
            if get_step(uid) != "hr_choose_guild":
                await call.answer()
                return

            guild_id = int(call.data.split(":", 1)[1])
            user_state.setdefault(uid, {})
            user_state[uid].setdefault("hr", {})
            user_state[uid]["hr"]["guild_id"] = guild_id

            set_step(uid, "hr_essence")
            await call.message.answer("Коротко опиши суть проблемы:")
            await call.answer()
        except Exception:
            await call.answer()

    @dp.message_handler(lambda m: m.chat.type == "private" and get_step(m.from_user.id) == "hr_essence")
    async def hr_essence(message: types.Message):
        uid = message.from_user.id
        if is_any_menu_button(message.text):
            user_state[uid] = {}
            await open_help_menu(message)
            return

        user_state[uid]["hr"]["essence"] = (message.text or "").strip()
        set_step(uid, "hr_since")
        await message.answer("Как давно это длится?")

    @dp.message_handler(lambda m: m.chat.type == "private" and get_step(m.from_user.id) == "hr_since")
    async def hr_since(message: types.Message):
        uid = message.from_user.id
        if is_any_menu_button(message.text):
            user_state[uid] = {}
            await open_help_menu(message)
            return

        user_state[uid]["hr"]["since"] = (message.text or "").strip()
        set_step(uid, "hr_tried")
        await message.answer("Что ты уже пробовал(а)?")

    @dp.message_handler(lambda m: m.chat.type == "private" and get_step(m.from_user.id) == "hr_tried")
    async def hr_tried(message: types.Message):
        uid = message.from_user.id
        if is_any_menu_button(message.text):
            user_state[uid] = {}
            await open_help_menu(message)
            return

        user_state[uid]["hr"]["tried"] = (message.text or "").strip()
        set_step(uid, "hr_result")
        await message.answer("Какой результат ты хочешь получить?")

    @dp.message_handler(lambda m: m.chat.type == "private" and get_step(m.from_user.id) == "hr_result")
    async def hr_result(message: types.Message):
        uid = message.from_user.id
        if is_any_menu_button(message.text):
            user_state[uid] = {}
            await open_help_menu(message)
            return

        data = (user_state.get(uid) or {}).get("hr") or {}
        guild_id = int(data.get("guild_id", 0) or 0)
        essence = (data.get("essence") or "").strip()
        since = (data.get("since") or "").strip()
        tried = (data.get("tried") or "").strip()
        result = (message.text or "").strip()

        user_state[uid] = {}

        if not (guild_id and essence and since and tried and result and callable(create_help_request)):
            await message.answer("Не удалось создать запрос. Попробуй заново.", reply_markup=main_kb())
            return

        try:
            rid = create_help_request(uid, guild_id, essence, since, tried, result)
        except Exception:
            log.exception("create_help_request failed")
            await message.answer("Не удалось отправить запрос. Попробуй позже.", reply_markup=main_kb())
            return

        await message.answer(f"✅ Запрос отправлен. Номер: #{rid}", reply_markup=main_kb())

        # ✅ notify ALL experts immediately
        try:
            if callable(list_experts):
                experts = list_experts() or []
            else:
                experts = []

            if experts:
                ikb = InlineKeyboardMarkup()
                ikb.add(InlineKeyboardButton("Открыть запрос", callback_data=f"ex_open:{rid}"))

                text = (
                    f"🆘 Новый запрос #{rid}\n"
                    f"{guild_name(guild_id)}\n"
                    f"Суть: {essence[:80]}"
                )
                for e in experts:
                    ex_id = int(e.get("user_id") or 0)
                    if ex_id:
                        await safe_notify(ex_id, text, reply_markup=ikb)
        except Exception:
            pass

    # ----------------- MY REQUESTS (student) -----------------
    @dp.message_handler(lambda m: m.chat.type == "private" and (m.text or "").strip() == BTN_MY_REQUESTS)
    async def my_requests(message: types.Message):
        uid = message.from_user.id

        if not callable(list_user_requests):
            await message.answer("Модуль базы не готов: list_user_requests отсутствует.", reply_markup=main_kb())
            return

        try:
            rows = list_user_requests(uid, limit=10)
        except Exception:
            log.exception("list_user_requests failed")
            await message.answer("Не удалось загрузить запросы. Попробуй позже.", reply_markup=main_kb())
            return

        if not rows:
            await message.answer("У тебя пока нет запросов.", reply_markup=main_kb())
            return

        await message.answer("Твои запросы:", reply_markup=main_kb())
        for r in rows:
            rid = int(r["request_id"])
            g_id = int(r.get("guild_id") or 0)
            essence = (r.get("problem_essence") or "").strip()
            short = essence[:80] + ("..." if len(essence) > 80 else "")
            created = (r.get("created_at") or "")[:19].replace("T", " ")
            answers_count = int(r.get("answers_count") or 0)
            closed = int(r.get("is_closed") or 0) == 1

            ikb = InlineKeyboardMarkup()
            ikb.add(InlineKeyboardButton("Открыть", callback_data=f"my_open:{rid}"))

            text = (
                f"Запрос #{rid}\n"
                f"{guild_name(g_id)}\n"
                f"Суть: {short}\n"
                f"Ответов: {answers_count}\n"
                f"Дата: {created}\n"
                f"Статус: {'закрыт' if closed else 'активен'}"
            )
            await message.answer(text, reply_markup=ikb)

    @dp.callback_query_handler(lambda c: c.data and c.data.startswith("my_open:"))
    async def cb_my_open(call: types.CallbackQuery):
        uid = call.from_user.id
        try:
            rid = int(call.data.split(":", 1)[1])

            if not callable(get_request):
                await call.answer()
                return

            req = get_request(rid)
            if not req or int(req.get("student_user_id", 0)) != int(uid):
                await call.answer()
                return

            closed = bool(is_request_closed(rid)) if callable(is_request_closed) else bool(int(req.get("is_closed") or 0) == 1)
            answers = list_request_answers(rid) if callable(list_request_answers) else []

            text = (
                f"Запрос #{rid}\n"
                f"{guild_name(int(req.get('guild_id') or 0))}\n\n"
                f"Суть:\n{req.get('problem_essence') or ''}\n\n"
                f"Как давно:\n{req.get('problem_since') or ''}\n\n"
                f"Что пробовал:\n{req.get('tried_actions') or ''}\n\n"
                f"Хочу результат:\n{req.get('desired_result') or ''}\n\n"
                f"Статус: {'закрыт' if closed else 'активен'}"
            )
            await call.message.answer(text)

            if answers:
                for a in answers:
                    ans_txt = a.get("answer_text") if isinstance(a, dict) else a["answer_text"]
                    await call.message.answer(f"Ответ эксперта:\n{ans_txt}")

            if not closed and callable(close_request):
                ikb = InlineKeyboardMarkup()
                ikb.add(InlineKeyboardButton("Закрыть запрос", callback_data=f"my_close:{rid}"))
                await call.message.answer("Действия:", reply_markup=ikb)

            await call.answer()
        except Exception:
            await call.answer()

    @dp.callback_query_handler(lambda c: c.data and c.data.startswith("my_close:"))
    async def cb_my_close(call: types.CallbackQuery):
        uid = call.from_user.id
        try:
            rid = int(call.data.split(":", 1)[1])
            if not callable(close_request):
                await call.message.answer("Функция закрытия недоступна.", reply_markup=main_kb())
                await call.answer()
                return

            ok = bool(close_request(rid, uid))
            await call.message.answer("✅ Запрос закрыт." if ok else "⚠️ Не удалось закрыть запрос.", reply_markup=main_kb())
            await call.answer()
        except Exception:
            await call.answer()

    # ----------------- EXPERT QUEUE -----------------
    @dp.message_handler(lambda m: m.chat.type == "private" and (m.text or "").strip() == BTN_HELP_ANSWER)
    async def expert_queue(message: types.Message):
        uid = message.from_user.id
        if not is_expert(uid):
            await message.answer("У тебя нет роли эксперта.", reply_markup=main_kb())
            return

        if not callable(list_open_requests):
            await message.answer("Модуль базы не готов: list_open_requests отсутствует.", reply_markup=main_kb())
            return

        try:
            rows = list_open_requests(limit=10)
        except Exception:
            log.exception("list_open_requests failed")
            await message.answer("Не удалось загрузить запросы. Попробуй позже.", reply_markup=main_kb())
            return

        if not rows:
            await message.answer("Активных запросов нет.", reply_markup=main_kb())
            return

        await message.answer("Активные запросы:", reply_markup=main_kb())
        for r in rows:
            rid = int(r.get("request_id") or 0)
            g_id = int(r.get("guild_id") or 0)
            essence = (r.get("problem_essence") or "").strip()
            short = essence[:80] + ("..." if len(essence) > 80 else "")
            created = (r.get("created_at") or "")[:19].replace("T", " ")
            answers_count = int(r.get("answers_count") or 0)

            ikb = InlineKeyboardMarkup()
            ikb.add(InlineKeyboardButton("Открыть", callback_data=f"ex_open:{rid}"))

            text = (
                f"Запрос #{rid}\n"
                f"{guild_name(g_id)}\n"
                f"Суть: {short}\n"
                f"Ответов: {answers_count}\n"
                f"Дата: {created}"
            )
            await message.answer(text, reply_markup=ikb)

    @dp.callback_query_handler(lambda c: c.data and c.data.startswith("ex_open:"))
    async def cb_ex_open(call: types.CallbackQuery):
        uid = call.from_user.id
        try:
            if not is_expert(uid):
                await call.answer()
                return

            rid = int(call.data.split(":", 1)[1])
            if not callable(get_request):
                await call.message.answer("Запрос не найден.")
                await call.answer()
                return

            req = get_request(rid)
            if not req:
                await call.message.answer("Запрос не найден.")
                await call.answer()
                return

            # По ТЗ: админ может удалить запрос из ленты экспертов (is_deleted=1).
            # Эксперт не должен видеть/открывать такие запросы.
            if int(req.get("is_deleted") or 0) == 1:
                await call.message.answer("⚠️ Запрос не найден или удалён.")
                await call.answer()
                return

            closed = bool(is_request_closed(rid)) if callable(is_request_closed) else bool(int(req.get("is_closed") or 0) == 1)
            answers = list_request_answers(rid) if callable(list_request_answers) else []

            text_msg = (
                f"Запрос #{rid}\n"
                f"{guild_name(int(req.get('guild_id') or 0))}\n\n"
                f"Суть:\n{req.get('problem_essence') or ''}\n\n"
                f"Как давно:\n{req.get('problem_since') or ''}\n\n"
                f"Что пробовал:\n{req.get('tried_actions') or ''}\n\n"
                f"Нужный результат:\n{req.get('desired_result') or ''}\n\n"
                f"Статус: {'закрыт' if closed else 'активен'}"
            )

            await call.message.answer(text_msg)

            # Важно: эксперт (не автор запроса) НЕ должен видеть ответы других экспертов.
            # Автор видит ответы в разделе «📁 Мои запросы».
            if answers:
                await call.message.answer(
                    f"Ответов: {len(answers)}. Ответы видит только автор запроса."
                )

            if not closed:
                ikb = InlineKeyboardMarkup()
                ikb.add(InlineKeyboardButton("Ответить", callback_data=f"ex_answer:{rid}"))
                await call.message.answer("Действия:", reply_markup=ikb)

            await call.answer()
        except Exception:
            await call.answer()

    # ✅ 3-step expert answer (Причина -> Быстрое -> Качественное)
    @dp.callback_query_handler(lambda c: c.data and c.data.startswith("ex_answer:"))
    async def cb_ex_answer(call: types.CallbackQuery):
        uid = call.from_user.id
        try:
            if not is_expert(uid):
                await call.answer()
                return

            rid = int(call.data.split(":", 1)[1])
            if callable(is_request_closed) and is_request_closed(rid):
                await call.message.answer("Запрос уже закрыт.")
                await call.answer()
                return

            user_state.setdefault(uid, {})
            user_state[uid]["ex"] = {"rid": rid, "cause": None, "quick": None, "quality": None}
            set_step(uid, "ex_cause")

            await call.message.answer("Причина проблемы:")
            await call.answer()
        except Exception:
            await call.answer()

    @dp.message_handler(lambda m: m.chat.type == "private" and get_step(m.from_user.id) in {"ex_cause", "ex_quick", "ex_quality"})
    async def ex_answer_steps(message: types.Message):
        uid = message.from_user.id
        if is_any_menu_button(message.text):
            user_state[uid] = {}
            await open_help_menu(message)
            return

        if not is_expert(uid):
            user_state[uid] = {}
            await message.answer("У тебя нет роли эксперта.", reply_markup=main_kb())
            return

        st = user_state.get(uid) or {}
        ex = st.get("ex") or {}
        rid = int(ex.get("rid") or 0)
        if not rid:
            user_state[uid] = {}
            await message.answer("Не удалось продолжить ответ. Открой запрос заново.", reply_markup=main_kb())
            return

        if callable(is_request_closed) and is_request_closed(rid):
            user_state[uid] = {}
            await message.answer("Запрос уже закрыт.", reply_markup=main_kb())
            return

        txt = (message.text or "").strip()
        if not txt:
            await message.answer("Напиши ответ текстом.")
            return

        step = get_step(uid)

        if step == "ex_cause":
            ex["cause"] = txt
            st["ex"] = ex
            set_step(uid, "ex_quick")
            await message.answer("Быстрое решение:")
            return

        if step == "ex_quick":
            ex["quick"] = txt
            st["ex"] = ex
            set_step(uid, "ex_quality")
            await message.answer("Качественное решение:")
            return

        if step == "ex_quality":
            ex["quality"] = txt
            st["ex"] = ex

            answer_text = (
                f"Причина проблемы: {ex.get('cause')}\n\n"
                f"Быстрое решение: {ex.get('quick')}\n\n"
                f"Качественное решение: {ex.get('quality')}"
            )

            user_state[uid] = {}

            if not callable(try_save_answer):
                await message.answer("Не удалось отправить ответ. Попробуй позже.", reply_markup=main_kb())
                return

            # сохранить (один ответ от эксперта на запрос)
            ok = False
            try:
                ok = bool(try_save_answer(uid, rid, answer_text, message.from_user.username, message.from_user.full_name))
            except TypeError:
                try:
                    ok = bool(try_save_answer(rid, uid, answer_text))
                except Exception:
                    ok = False
            except Exception:
                ok = False

            if not ok:
                await message.answer("Ответ не принят (возможно, ты уже отвечал(а) или запрос закрыт).", reply_markup=main_kb())
                return

            # уведомление пользователю в личку
            delivered = False
            try:
                if callable(get_request):
                    req = get_request(rid)
                    student_id = int((req or {}).get("student_user_id") or 0)
                    if student_id:
                        await safe_notify(student_id, f"📩 Ответ эксперта по запросу #{rid}:\n\n{answer_text}")
                        delivered = True
            except Exception:
                delivered = False

            await message.answer(
                f"✅ Ответ отправлен пользователю по запросу #{rid}."
                + ("" if delivered else "\n⚠️ Уведомление пользователю не доставлено (если он не запускал бота/запретил сообщения)."),
                reply_markup=main_kb()
            )

    # ----------------- APPLY EXPERT (with inline approve/reject) -----------------
    @dp.message_handler(lambda m: m.chat.type == "private" and (m.text or "").strip() == BTN_APPLY_EXPERT)
    async def apply_expert(message: types.Message):
        uid = message.from_user.id

        if is_expert(uid):
            await message.answer("Ты уже эксперт.", reply_markup=main_kb())
            return

        try:
            if callable(get_pending_application) and get_pending_application(uid):
                await message.answer("Твоя заявка уже на рассмотрении. Дождись решения администратора.", reply_markup=main_kb())
                return
        except Exception:
            log.exception("get_pending_application failed")

        user_state.setdefault(uid, {})
        user_state[uid]["apply"] = {"q1": None, "q2": None, "q3": None}
        set_step(uid, "apply_q1")
        await message.answer("Почему ты хочешь стать экспертом?")

    @dp.message_handler(lambda m: m.chat.type == "private" and get_step(m.from_user.id) in {"apply_q1", "apply_q2", "apply_q3"})
    async def apply_steps(message: types.Message):
        uid = message.from_user.id
        if is_any_menu_button(message.text):
            user_state[uid] = {}
            await open_help_menu(message)
            return

        if is_expert(uid):
            user_state[uid] = {}
            await message.answer("Ты уже эксперт.", reply_markup=main_kb())
            return

        txt = (message.text or "").strip()
        if not txt:
            await message.answer("Напиши ответ текстом.")
            return

        apply = (user_state.get(uid) or {}).get("apply") or {}
        step = get_step(uid)

        if step == "apply_q1":
            apply["q1"] = txt
            set_step(uid, "apply_q2")
            await message.answer("В чем твоя компетенция и опыт?")
            return

        if step == "apply_q2":
            apply["q2"] = txt
            set_step(uid, "apply_q3")
            await message.answer("Сколько времени готов(а) уделять в неделю?")
            return

        if step == "apply_q3":
            apply["q3"] = txt

            about_text = (
                f"1) Почему хочу стать экспертом:\n{apply['q1']}\n\n"
                f"2) Компетенция и опыт:\n{apply['q2']}\n\n"
                f"3) Время в неделю:\n{apply['q3']}"
            )

            username = message.from_user.username
            full_name = (message.from_user.full_name or "").strip() or None

            app_id = None
            if callable(create_expert_application):
                try:
                    app_id = create_expert_application(uid, username, full_name, about_text)
                except Exception:
                    log.exception("create_expert_application failed")
                    user_state[uid] = {}
                    await message.answer("Не удалось отправить заявку. Попробуй позже.", reply_markup=main_kb())
                    return

                if app_id == -1:
                    user_state[uid] = {}
                    await message.answer("Твоя заявка уже на рассмотрении. Дождись решения администратора.", reply_markup=main_kb())
                    return

            user_state[uid] = {}

            admin_ids = getattr(db, "ADMIN_USER_IDS", None)
            if not admin_ids:
                from ..config import ADMIN_USER_IDS as admin_ids  # noqa

            ikb = InlineKeyboardMarkup(row_width=2)
            ikb.add(
                InlineKeyboardButton("✅ Принять", callback_data=f"adm_app_accept:{uid}:{app_id or 0}"),
                InlineKeyboardButton("❌ Отклонить", callback_data=f"adm_app_reject:{uid}:{app_id or 0}"),
            )

            text = (
                "🧑‍🏫 Новая заявка в эксперты\n"
                f"{full_name or '(без имени)'}"
                f"{(' @' + username) if username else ''}\n"
                f"user_id: {uid}\n"
                f"application_id: {app_id if app_id is not None else '(n/a)'}\n\n"
                f"{about_text}"
            )

            for admin_id in (admin_ids or []):
                await safe_notify(int(admin_id), text, reply_markup=ikb)

            await message.answer("✅ Заявка отправлена администраторам.", reply_markup=main_kb())

    @dp.callback_query_handler(lambda c: c.data and c.data.startswith("adm_app_accept:"))
    async def adm_app_accept(call: types.CallbackQuery):
        admin_id = call.from_user.id
        if not is_admin(admin_id):
            await call.answer("Нет доступа", show_alert=True)
            return

        try:
            _, uid_str, app_id_str = call.data.split(":")
            uid = int(uid_str)
            app_id = int(app_id_str)

            # берём данные заявителя из expert_applications, чтобы корректно сохранить username/full_name в experts
            applicant_username = None
            applicant_full_name = None
            try:
                conn = _db_connect_fallback()
                cur = conn.cursor()
                cur.execute("""
                    SELECT username, full_name
                    FROM expert_applications
                    WHERE application_id=?
                    LIMIT 1
                """, (int(app_id),))
                row = cur.fetchone()
                conn.close()
                if row:
                    try:
                        applicant_username = row[0]
                        applicant_full_name = row[1]
                    except Exception:
                        pass
            except Exception:
                pass

            # добавляем в эксперты
            if callable(add_expert):
                add_expert(uid, admin_id, applicant_username, applicant_full_name)

            # статус заявки
            if callable(set_application_status) and app_id > 0:
                set_application_status(app_id, "approved")
            else:
                try:
                    _set_app_status(app_id, "approved")
                except Exception:
                    pass

            await safe_notify(uid, "✅ Заявка одобрена. Ты добавлен(а) в эксперты.")

            # убираем кнопки под сообщением
            try:
                await call.message.edit_reply_markup()
            except Exception:
                pass

            # и пишем в чат админа (не только всплывашку)
            try:
                await call.message.answer(f"✅ Принято: заявка #{app_id} (user_id: {uid}).")
            except Exception:
                pass

            await call.answer("Принято")
        except Exception:
            await call.answer("Ошибка", show_alert=True)

    @dp.callback_query_handler(lambda c: c.data and c.data.startswith("adm_app_reject:"))
    async def adm_app_reject(call: types.CallbackQuery):
        admin_id = call.from_user.id
        if not is_admin(admin_id):
            await call.answer("Нет доступа", show_alert=True)
            return

        try:
            _, uid_str, app_id_str = call.data.split(":")
            uid = int(uid_str)
            app_id = int(app_id_str)

            if callable(set_application_status) and app_id > 0:
                set_application_status(app_id, "rejected")
            else:
                try:
                    _set_app_status(app_id, "rejected")
                except Exception:
                    pass

            await safe_notify(uid, "❌ Заявка отклонена. Ты можешь подать новую заявку.")

            try:
                await call.message.edit_reply_markup()
            except Exception:
                pass

            try:
                await call.message.answer(f"❌ Отклонено: заявка #{app_id} (user_id: {uid}).")
            except Exception:
                pass

            await call.answer("Отклонено")
        except Exception:
            await call.answer("Ошибка", show_alert=True)


    # ===================== ADMIN: Управление экспертами (ТОЛЬКО ДОБАВЛЕНО) =====================

    # Тексты кнопок админ-меню (берем из keyboards.py если есть, иначе дефолты по ТЗ)
    BTN_ADMIN_MENU = BTN_ADMIN_EXPERTS or "⚙️ Управление экспертами"
    BTN_ADM_LIST = getattr(kb, "BTN_ADMIN_LIST_EXPERTS", "📋 Список экспертов")
    BTN_ADM_REMOVE = getattr(kb, "BTN_ADMIN_REMOVE_EXPERT", "🗑 Удалить эксперта")
    BTN_ADM_APPS = getattr(kb, "BTN_ADMIN_APPS", "📥 Заявки в эксперты")  # в keyboards.py может не быть
    BTN_ADM_DELETE_REQUEST = getattr(kb, "BTN_ADMIN_DELETE_REQUEST", "🗑 Удалить запрос")
    BTN_ADM_BACK = getattr(kb, "BTN_ADMIN_BACK", "⬅ Назад")

    def admin_experts_kb() -> types.ReplyKeyboardMarkup:
        k = types.ReplyKeyboardMarkup(resize_keyboard=True)
        k.add(types.KeyboardButton(BTN_ADM_LIST))
        k.add(types.KeyboardButton(BTN_ADM_REMOVE))
        k.add(types.KeyboardButton(BTN_ADM_APPS))
        k.add(types.KeyboardButton(BTN_ADM_DELETE_REQUEST))
        k.add(types.KeyboardButton(BTN_ADM_BACK))
        return k

    def _db_connect_fallback():
        # пытаемся использовать внутренний коннект из db_help.py
        conn_fn = getattr(db, "_connect", None)
        if callable(conn_fn):
            return conn_fn()
        # fallback: прямой коннект (если вдруг _connect нет)
        import sqlite3
        from ..config import DB_PATH_HELP
        return sqlite3.connect(DB_PATH_HELP)

    def _fetch_pending_apps(limit: int = 50):
        conn = _db_connect_fallback()
        try:
            conn.row_factory = getattr(conn, "row_factory", None) or None
        except Exception:
            pass
        cur = conn.cursor()
        cur.execute("""
            SELECT application_id, user_id, username, full_name, about_text, status, created_at
            FROM expert_applications
            WHERE status='pending'
            ORDER BY application_id ASC
            LIMIT ?
        """, (int(limit),))
        rows = cur.fetchall()
        conn.close()
        # rows может быть list[Row] или list[tuple]
        out = []
        for r in rows:
            if isinstance(r, dict):
                out.append(r)
            else:
                # sqlite3.Row поддерживает доступ по ключам, tuple - нет
                try:
                    out.append(dict(r))
                except Exception:
                    out.append({
                        "application_id": r[0],
                        "user_id": r[1],
                        "username": r[2],
                        "full_name": r[3],
                        "about_text": r[4],
                        "status": r[5],
                        "created_at": r[6],
                    })
        return out

    def _set_app_status(app_id: int, status: str) -> bool:
        conn = _db_connect_fallback()
        cur = conn.cursor()
        cur.execute("UPDATE expert_applications SET status=? WHERE application_id=?", (status, int(app_id)))
        ok = cur.rowcount > 0
        conn.commit()
        conn.close()
        return ok

    async def _format_expert_line(n: int, user_id: int) -> str:
        """
        Формат по ТЗ:
        Имя (@username) [user_id]
        @username [user_id]
        [user_id]
        """
        try:
            chat = await bot.get_chat(int(user_id))
            uname = getattr(chat, "username", None)
            fname = getattr(chat, "full_name", None) or getattr(chat, "title", None)
        except Exception:
            uname = None
            fname = None

        uname_part = f"@{uname}" if uname else None
        if fname and uname_part:
            return f"{n}. {fname} ({uname_part}) [{user_id}]"
        if uname_part:
            return f"{n}. {uname_part} [{user_id}]"
        return f"{n}. [{user_id}]"

    @dp.message_handler(lambda m: m.chat.type == "private" and (m.text or "").strip() in {BTN_ADMIN_MENU, "⚙️ Управление экспертами", "👑 Управление экспертами"})
    async def admin_experts_menu(message: types.Message):
        uid = message.from_user.id
        if not is_admin(uid):
            await message.answer("⚠️ Нет доступа.", reply_markup=main_kb())
            return
        # полный сброс (как требование state.clear() по смыслу)
        user_state[uid] = {}
        await message.answer("⚙️ Управление экспертами:", reply_markup=admin_experts_kb())

    @dp.message_handler(lambda m: m.chat.type == "private" and (m.text or "").strip() == BTN_ADM_BACK)
    async def admin_back_to_help(message: types.Message):
        uid = message.from_user.id
        if not is_admin(uid):
            await message.answer("⚠️ Нет доступа.", reply_markup=main_kb())
            return
        user_state[uid] = {}
        await open_help_menu(message)

    @dp.message_handler(lambda m: m.chat.type == "private" and (m.text or "").strip() == BTN_ADM_LIST)
    async def admin_list_experts(message: types.Message):
        uid = message.from_user.id
        if not is_admin(uid):
            await message.answer("⚠️ Нет доступа.", reply_markup=main_kb())
            return

        # получаем список экспертов (из db_help.list_experts если есть, иначе напрямую SQL)
        experts = []
        list_fn = getattr(db, "list_experts", None)
        if callable(list_fn):
            try:
                experts = list_fn() or []
            except Exception:
                experts = []

        if not experts:
            # fallback SQL
            try:
                conn = _db_connect_fallback()
                cur = conn.cursor()
                cur.execute("SELECT user_id FROM experts ORDER BY user_id ASC")
                experts = [{"user_id": int(r[0])} for r in cur.fetchall()]
                conn.close()
            except Exception:
                experts = []

        if not experts:
            await message.answer("Экспертов нет.", reply_markup=admin_experts_kb())
            return

        # стабильная сортировка (по user_id ASC)
        ids = sorted({int(e.get("user_id")) for e in experts if e.get("user_id") is not None})

        # сохраняем порядок (чтобы удаление по номеру было корректно)
        user_state.setdefault(uid, {})
        user_state[uid]["adm_expert_ids"] = ids

        lines = []
        for i, ex_id in enumerate(ids, start=1):
            lines.append(await _format_expert_line(i, ex_id))

        await message.answer("📋 Список экспертов:\n" + "\n".join(lines), reply_markup=admin_experts_kb())

    @dp.message_handler(lambda m: m.chat.type == "private" and (m.text or "").strip() == BTN_ADM_REMOVE)
    async def admin_remove_expert_start(message: types.Message):
        uid = message.from_user.id
        if not is_admin(uid):
            await message.answer("⚠️ Нет доступа.", reply_markup=main_kb())
            return

        # чтобы удалить по номеру — сначала убедимся что есть актуальный список
        user_state.setdefault(uid, {})
        ids = user_state[uid].get("adm_expert_ids")
        if not ids:
            # сформируем список тихо
            try:
                list_fn = getattr(db, "list_experts", None)
                ex = list_fn() if callable(list_fn) else []
                ids = sorted({int(e.get("user_id")) for e in (ex or []) if e.get("user_id") is not None})
            except Exception:
                ids = []
            user_state[uid]["adm_expert_ids"] = ids

        set_step(uid, "adm_remove_expert_number")
        await message.answer("Введи номер эксперта из списка (например 6) или 'отмена'.", reply_markup=admin_experts_kb())

    @dp.message_handler(lambda m: m.chat.type == "private" and get_step(m.from_user.id) == "adm_remove_expert_number")
    async def admin_remove_expert_number(message: types.Message):
        uid = message.from_user.id
        if not is_admin(uid):
            user_state[uid] = {}
            await message.answer("⚠️ Нет доступа.", reply_markup=main_kb())
            return

        txt = (message.text or "").strip().lower()
        if txt in {"отмена", "cancel"}:
            user_state[uid] = {}
            await message.answer("Ок, отменено.", reply_markup=admin_experts_kb())
            return

        ids = (user_state.get(uid) or {}).get("adm_expert_ids") or []
        try:
            k = int(re.search(r"\d+", txt).group(0))
        except Exception:
            await message.answer("Нужен номер (цифра) или 'отмена'.", reply_markup=admin_experts_kb())
            return

        if k < 1 or k > len(ids):
            await message.answer("Неверный номер. Открой список экспертов и попробуй снова.", reply_markup=admin_experts_kb())
            return

        ex_id = int(ids[k - 1])

        # удалить эксперта
        ok = False
        rm_fn = getattr(db, "remove_expert", None)
        if callable(rm_fn):
            try:
                ok = bool(rm_fn(ex_id))
            except Exception:
                ok = False
        else:
            try:
                conn = _db_connect_fallback()
                cur = conn.cursor()
                cur.execute("DELETE FROM experts WHERE user_id=?", (ex_id,))
                ok = cur.rowcount > 0
                conn.commit()
                conn.close()
            except Exception:
                ok = False

        user_state[uid] = {}
        if ok:
            await message.answer(f"✅ Эксперт удалён: [{ex_id}]", reply_markup=admin_experts_kb())
        else:
            await message.answer("⚠️ Не удалось удалить эксперта.", reply_markup=admin_experts_kb())

    @dp.message_handler(lambda m: m.chat.type == "private" and (m.text or "").strip() == BTN_ADM_APPS)
    async def admin_list_applications(message: types.Message):
        uid = message.from_user.id
        if not is_admin(uid):
            await message.answer("⚠️ Нет доступа.", reply_markup=main_kb())
            return

        try:
            apps = _fetch_pending_apps(limit=50)
        except Exception:
            apps = []

        if not apps:
            await message.answer("Активных заявок нет.", reply_markup=admin_experts_kb())
            return

        for a in apps:
            app_id = int(a.get("application_id") or 0)
            u_id = int(a.get("user_id") or 0)
            uname = a.get("username")
            fname = a.get("full_name")
            about = a.get("about_text") or ""
            created = (a.get("created_at") or "")[:19].replace("T", " ")

            ikb = InlineKeyboardMarkup(row_width=2)
            ikb.add(
                InlineKeyboardButton("✅ Принять", callback_data=f"adm_app_accept:{u_id}:{app_id}"),
                InlineKeyboardButton("❌ Отклонить", callback_data=f"adm_app_reject:{u_id}:{app_id}"),
            )

            header = "🧑‍🏫 Заявка в эксперты"
            name_line = (fname or "").strip()
            if uname:
                name_line = (name_line + " @" + str(uname).lstrip("@")).strip() if name_line else "@" + str(uname).lstrip("@")

            text = (
                f"{header}\n"
                f"{name_line if name_line else '(без имени)'}\n"
                f"user_id: {u_id}\n"
                f"application_id: {app_id}\n"
                f"Дата: {created}\n\n"
                f"{about}"
            )
            await message.answer(text, reply_markup=ikb)

    # ----------------- ADMIN: DELETE REQUEST (удаление из ленты экспертов) -----------------
    @dp.message_handler(lambda m: m.chat.type == "private" and (m.text or "").strip() == BTN_ADM_DELETE_REQUEST)
    async def admin_delete_request_start(message: types.Message):
        uid = message.from_user.id
        if not is_admin(uid):
            await message.answer("⚠️ Нет доступа.", reply_markup=main_kb())
            return

        user_state.setdefault(uid, {})
        set_step(uid, "adm_delete_request_number")
        await message.answer("Введи номер запроса (например 15) или 'отмена'.", reply_markup=admin_experts_kb())

    @dp.message_handler(lambda m: m.chat.type == "private" and get_step(m.from_user.id) == "adm_delete_request_number")
    async def admin_delete_request_number(message: types.Message):
        uid = message.from_user.id
        if not is_admin(uid):
            user_state[uid] = {}
            await message.answer("⚠️ Нет доступа.", reply_markup=main_kb())
            return

        txt = (message.text or "").strip()
        if txt.lower() in {"отмена", "cancel"}:
            user_state[uid] = {}
            await message.answer("Ок, отменено.", reply_markup=admin_experts_kb())
            return

        rid = parse_first_int(txt)
        if not rid:
            await message.answer("Нужен номер запроса (цифра) или 'отмена'.", reply_markup=admin_experts_kb())
            return

        if not callable(get_request):
            user_state[uid] = {}
            await message.answer("⚠️ Запрос не найден.", reply_markup=admin_experts_kb())
            return

        req = get_request(int(rid))
        if not req:
            user_state[uid] = {}
            await message.answer("⚠️ Запрос не найден.", reply_markup=admin_experts_kb())
            return

        # сохраняем id для подтверждения
        user_state.setdefault(uid, {})
        user_state[uid]["adm_del_rid"] = int(rid)
        set_step(uid, "adm_delete_request_confirm")

        created = (req.get("created_at") or "")[:19].replace("T", " ")
        closed = bool(int(req.get("is_closed") or 0) == 1)

        card = (
            f"Запрос #{rid}\n"
            f"{guild_name(int(req.get('guild_id') or 0))}\n\n"
            f"Суть:\n{req.get('problem_essence') or ''}\n\n"
            f"Как давно:\n{req.get('problem_since') or ''}\n\n"
            f"Что пробовал:\n{req.get('tried_actions') or ''}\n\n"
            f"Нужный результат:\n{req.get('desired_result') or ''}\n\n"
            f"Дата: {created}\n"
            f"Статус: {'закрыт' if closed else 'активен'}\n\n"
            "Удалить запрос из ленты экспертов? У пользователя в 'Мои запросы' он останется."
        )

        ikb = InlineKeyboardMarkup(row_width=2)
        ikb.add(
            InlineKeyboardButton("✅ Да", callback_data=f"adm_delreq_yes:{rid}"),
            InlineKeyboardButton("❌ Нет", callback_data=f"adm_delreq_no:{rid}")
        )

        await message.answer(card, reply_markup=ikb)

    @dp.callback_query_handler(lambda c: c.data and c.data.startswith("adm_delreq_no:"))
    async def admin_delete_request_no(call: types.CallbackQuery):
        uid = call.from_user.id
        if not is_admin(uid):
            await call.answer("Нет доступа", show_alert=True)
            return

        user_state[uid] = {}
        await call.message.answer("Ок, отменено.", reply_markup=admin_experts_kb())
        try:
            await call.message.edit_reply_markup()
        except Exception:
            pass
        await call.answer()

    @dp.callback_query_handler(lambda c: c.data and c.data.startswith("adm_delreq_yes:"))
    async def admin_delete_request_yes(call: types.CallbackQuery):
        uid = call.from_user.id
        if not is_admin(uid):
            await call.answer("Нет доступа", show_alert=True)
            return

        try:
            rid = int(call.data.split(":", 1)[1])
        except Exception:
            await call.answer("Ошибка", show_alert=True)
            return

        mark_deleted = getattr(db, "mark_request_deleted", None)
        ok = bool(mark_deleted(rid)) if callable(mark_deleted) else False

        user_state[uid] = {}

        if ok:
            await call.message.answer(f"✅ Запрос #{rid} удалён из ленты экспертов.", reply_markup=admin_experts_kb())
        else:
            await call.message.answer(f"⚠️ Не удалось удалить запрос #{rid}.", reply_markup=admin_experts_kb())

        try:
            await call.message.edit_reply_markup()
        except Exception:
            pass
        await call.answer()
