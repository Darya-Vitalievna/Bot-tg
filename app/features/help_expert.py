import logging
import sqlite3
from typing import Optional, List, Dict, Any

from aiogram import Dispatcher, types
from aiogram.types import InlineKeyboardMarkup, InlineKeyboardButton

from .. import db_help as db
from ..storage import user_state
from .. import keyboards as kb

log = logging.getLogger(__name__)


def _get_step(user_id: int) -> Optional[str]:
    st = user_state.get(user_id) or {}
    return st.get("step")


def _set_step(user_id: int, step: Optional[str]) -> None:
    user_state.setdefault(user_id, {})
    if step is None:
        user_state[user_id].pop("step", None)
    else:
        user_state[user_id]["step"] = step


def _clear_state(user_id: int) -> None:
    user_state[user_id] = {}


def _guild_name(guild_id: int) -> str:
    GUILDS = getattr(kb, "GUILDS", ["Гильдия 1", "Гильдия 2", "Гильдия 3", "Гильдия 4", "Гильдия 5"])
    if 1 <= int(guild_id) <= len(GUILDS):
        return GUILDS[int(guild_id) - 1]
    return f"Гильдия {guild_id}"


def _main_kb():
    if callable(getattr(kb, "build_main_kb", None)):
        return kb.build_main_kb()
    k = types.ReplyKeyboardMarkup(resize_keyboard=True)
    k.add(types.KeyboardButton(getattr(kb, "BTN_BOOK", "📘 Прочитал")))
    k.add(types.KeyboardButton(getattr(kb, "BTN_EXERCISE", "🏋 Сделал упражнение")))
    k.add(types.KeyboardButton(getattr(kb, "BTN_STATS", "📊 Статистика")))
    k.add(types.KeyboardButton(getattr(kb, "BTN_HELP", "🆘 Помощь эксперта")))
    return k


def _help_menu_kb(uid: int):
    BTN_HELP_REQUEST = getattr(kb, "BTN_HELP_REQUEST", "📝 Запросить помощь")
    BTN_MY_REQUESTS = getattr(kb, "BTN_MY_REQUESTS", "📁 Мои запросы")
    BTN_APPLY_EXPERT = getattr(kb, "BTN_APPLY_EXPERT", "🧑‍🏫 Стать экспертом")
    BTN_HELP_ANSWER = getattr(kb, "BTN_HELP_ANSWER", "📥 Ответить на запросы")
    BTN_BACK_MAIN = getattr(kb, "BTN_BACK_MAIN", "⬅ В меню")
    BTN_ADMIN_EXPERTS = getattr(kb, "BTN_ADMIN_EXPERTS", "⚙️ Управление экспертами")
    BTN_ADMIN_DELETE_REQ = getattr(kb, "BTN_ADMIN_DELETE_REQ", "🗑 Удалить запрос")

    expert_flag = bool(getattr(db, "is_expert", lambda _x: False)(uid))
    admin_flag = bool(getattr(db, "is_admin", lambda _x: False)(uid))

    k = types.ReplyKeyboardMarkup(resize_keyboard=True)
    k.add(types.KeyboardButton(BTN_HELP_REQUEST))
    k.add(types.KeyboardButton(BTN_MY_REQUESTS))
    k.add(types.KeyboardButton(BTN_APPLY_EXPERT))
    if expert_flag:
        k.add(types.KeyboardButton(BTN_HELP_ANSWER))
    if admin_flag:
        k.add(types.KeyboardButton(BTN_ADMIN_EXPERTS))
        k.add(types.KeyboardButton(BTN_ADMIN_DELETE_REQ))
    k.add(types.KeyboardButton(BTN_BACK_MAIN))
    return k


def _is_any_menu_button(text: str) -> bool:
    t = (text or "").strip()
    base = {
        getattr(kb, "BTN_HELP", "🆘 Помощь эксперта"),
        getattr(kb, "BTN_HELP_REQUEST", "📝 Запросить помощь"),
        getattr(kb, "BTN_MY_REQUESTS", "📁 Мои запросы"),
        getattr(kb, "BTN_APPLY_EXPERT", "🧑‍🏫 Стать экспертом"),
        getattr(kb, "BTN_HELP_ANSWER", "📥 Ответить на запросы"),
        getattr(kb, "BTN_BACK_MAIN", "⬅ В меню"),
        getattr(kb, "BTN_ADMIN_EXPERTS", "⚙️ Управление экспертами"),
        getattr(kb, "BTN_ADMIN_DELETE_REQ", "🗑 Удалить запрос"),
        # admin submenu buttons:
        "📋 Список экспертов",
        "🗑 Удалить эксперта",
        "📥 Заявки в эксперты",
        "⬅ Назад",
    }
    return t in base


def _resolve_db_path_for_direct_sql() -> Optional[str]:
    try:
        if hasattr(db, "_resolve_db_path"):
            return db._resolve_db_path()  # type: ignore
    except Exception:
        pass
    return None


def _set_application_status_direct(app_id: int, status: str) -> None:
    path = _resolve_db_path_for_direct_sql()
    if not path:
        return
    try:
        conn = sqlite3.connect(path)
        cur = conn.cursor()
        cur.execute("UPDATE expert_applications SET status=? WHERE application_id=?", (status, int(app_id)))
        conn.commit()
        conn.close()
    except Exception:
        pass


def _find_latest_pending_app_id_for_user(uid: int) -> Optional[int]:
    path = _resolve_db_path_for_direct_sql()
    if not path:
        return None
    try:
        conn = sqlite3.connect(path)
        cur = conn.cursor()
        cur.execute(
            "SELECT application_id FROM expert_applications WHERE user_id=? AND status='pending' ORDER BY application_id DESC LIMIT 1",
            (int(uid),),
        )
        row = cur.fetchone()
        conn.close()
        if row:
            return int(row[0])
    except Exception:
        pass
    return None


def _delete_request_from_feed_direct(request_id: int) -> bool:
    # is_deleted=1
    path = _resolve_db_path_for_direct_sql()
    if not path:
        return False
    try:
        conn = sqlite3.connect(path)
        cur = conn.cursor()
        # если колонки is_deleted нет - операция невозможна
        cur.execute("PRAGMA table_info(help_requests)")
        cols = [r[1] for r in cur.fetchall()]
        if "is_deleted" not in cols:
            conn.close()
            return False

        cur.execute("UPDATE help_requests SET is_deleted=1 WHERE request_id=?", (int(request_id),))
        ok = cur.rowcount > 0
        conn.commit()
        conn.close()
        return ok
    except Exception:
        return False


def register(dp: Dispatcher, bot) -> None:
    BTN_HELP = getattr(kb, "BTN_HELP", "🆘 Помощь эксперта")
    BTN_BACK_MAIN = getattr(kb, "BTN_BACK_MAIN", "⬅ В меню")

    BTN_HELP_REQUEST = getattr(kb, "BTN_HELP_REQUEST", "📝 Запросить помощь")
    BTN_MY_REQUESTS = getattr(kb, "BTN_MY_REQUESTS", "📁 Мои запросы")
    BTN_HELP_ANSWER = getattr(kb, "BTN_HELP_ANSWER", "📥 Ответить на запросы")
    BTN_APPLY_EXPERT = getattr(kb, "BTN_APPLY_EXPERT", "🧑‍🏫 Стать экспертом")

    BTN_ADMIN_EXPERTS = getattr(kb, "BTN_ADMIN_EXPERTS", "⚙️ Управление экспертами")
    BTN_ADMIN_DELETE_REQ = getattr(kb, "BTN_ADMIN_DELETE_REQ", "🗑 Удалить запрос")

    # admin submenu buttons (фиксируем строго, чтобы не ломать клавиатуры)
    A_BTN_LIST = "📋 Список экспертов"
    A_BTN_DEL = "🗑 Удалить эксперта"
    A_BTN_APPS = "📥 Заявки в эксперты"
    A_BTN_BACK = "⬅ Назад"

    def _admin_menu_kb():
        k = types.ReplyKeyboardMarkup(resize_keyboard=True)
        k.add(types.KeyboardButton(A_BTN_LIST))
        k.add(types.KeyboardButton(A_BTN_DEL))
        k.add(types.KeyboardButton(A_BTN_APPS))
        k.add(types.KeyboardButton(A_BTN_BACK))
        return k

    async def _open_help_menu(message: types.Message):
        uid = message.from_user.id
        _clear_state(uid)
        await message.answer("Меню помощи эксперта:", reply_markup=_help_menu_kb(uid))

    # ---- OPEN HELP MENU ----
    @dp.message_handler(lambda m: m.chat.type == "private" and (m.text or "").strip() == BTN_HELP)
    async def open_help_menu(message: types.Message):
        await _open_help_menu(message)

    # ---- BACK MAIN ----
    @dp.message_handler(lambda m: m.chat.type == "private" and (m.text or "").strip() == BTN_BACK_MAIN)
    async def back_main(message: types.Message):
        uid = message.from_user.id
        _clear_state(uid)
        await message.answer("Выбери действие.", reply_markup=_main_kb())

    # =========================
    # 1) ADMIN: Управление экспертами (полностью)
    # =========================

    @dp.message_handler(lambda m: m.chat.type == "private" and (m.text or "").strip() == BTN_ADMIN_EXPERTS)
    async def admin_open(message: types.Message):
        uid = message.from_user.id
        if not getattr(db, "is_admin", lambda _x: False)(uid):
            await message.answer("⚠️ Нет доступа.")
            await _open_help_menu(message)
            return
        _clear_state(uid)
        _set_step(uid, "admin_menu")
        await message.answer("⚙️ Управление экспертами:", reply_markup=_admin_menu_kb())

    @dp.message_handler(lambda m: m.chat.type == "private" and (m.text or "").strip() == A_BTN_BACK)
    async def admin_back(message: types.Message):
        uid = message.from_user.id
        _clear_state(uid)
        await _open_help_menu(message)

    @dp.message_handler(lambda m: m.chat.type == "private" and (m.text or "").strip() == A_BTN_LIST)
    async def admin_list_experts(message: types.Message):
        uid = message.from_user.id
        if not getattr(db, "is_admin", lambda _x: False)(uid):
            await message.answer("⚠️ Нет доступа.")
            await _open_help_menu(message)
            return

        fn = getattr(db, "list_experts", None)
        if not callable(fn):
            await message.answer("⚠️ Список экспертов недоступен.")
            return

        experts = fn() or []
        if not experts:
            await message.answer("Список экспертов пуст.")
            return

        lines = []
        for i, e in enumerate(experts, start=1):
            eid = e.get("user_id")
            uname = e.get("username") or ""
            name = e.get("full_name") or ""
            label = ""
            if uname:
                label = f"@{uname}"
            elif name:
                label = name
            lines.append(f"{i}) {label} [{eid}]".strip())

        await message.answer("\n".join(lines))

    @dp.message_handler(lambda m: m.chat.type == "private" and (m.text or "").strip() == A_BTN_DEL)
    async def admin_del_expert_start(message: types.Message):
        uid = message.from_user.id
        if not getattr(db, "is_admin", lambda _x: False)(uid):
            await message.answer("⚠️ Нет доступа.")
            await _open_help_menu(message)
            return
        user_state.setdefault(uid, {})
        _set_step(uid, "admin_del_expert_wait")
        await message.answer("Введи номер эксперта из списка (например 6) или 'отмена'.")

    @dp.message_handler(lambda m: m.chat.type == "private" and _get_step(m.from_user.id) == "admin_del_expert_wait")
    async def admin_del_expert_wait(message: types.Message):
        uid = message.from_user.id

        if _is_any_menu_button(message.text):
            # по требованию стабильности - любые меню кнопки сбрасывают
            _clear_state(uid)
            await _open_help_menu(message)
            return

        txt = (message.text or "").strip().lower()
        if txt == "отмена":
            _clear_state(uid)
            await message.answer("Ок, отменено.", reply_markup=_admin_menu_kb())
            _set_step(uid, "admin_menu")
            return

        try:
            k = int((message.text or "").strip())
        except Exception:
            await message.answer("Введи номер (целое число) или 'отмена'.")
            return

        list_fn = getattr(db, "list_experts", None)
        del_fn = getattr(db, "remove_expert", None)
        if not callable(list_fn) or not callable(del_fn):
            _clear_state(uid)
            await message.answer("⚠️ Удаление эксперта недоступно.", reply_markup=_admin_menu_kb())
            _set_step(uid, "admin_menu")
            return

        experts = list_fn() or []
        if k < 1 or k > len(experts):
            await message.answer("Такого номера нет. Сначала посмотри список экспертов.")
            return

        target = experts[k - 1]
        target_id = int(target.get("user_id"))
        ok = bool(del_fn(target_id))

        _clear_state(uid)
        _set_step(uid, "admin_menu")
        if ok:
            await message.answer(f"✅ Эксперт удалён: [{target_id}].", reply_markup=_admin_menu_kb())
        else:
            await message.answer("⚠️ Не удалось удалить эксперта.", reply_markup=_admin_menu_kb())

    @dp.message_handler(lambda m: m.chat.type == "private" and (m.text or "").strip() == A_BTN_APPS)
    async def admin_pending_apps(message: types.Message):
        uid = message.from_user.id
        if not getattr(db, "is_admin", lambda _x: False)(uid):
            await message.answer("⚠️ Нет доступа.")
            await _open_help_menu(message)
            return

        # Показываем pending заявки из БД (как в ТЗ), если таблица есть
        path = _resolve_db_path_for_direct_sql()
        if not path:
            await message.answer("⚠️ Заявки недоступны.")
            return

        try:
            conn = sqlite3.connect(path)
            cur = conn.cursor()
            cur.execute(
                "SELECT application_id, user_id, username, full_name, about_text, created_at "
                "FROM expert_applications WHERE status='pending' ORDER BY application_id ASC"
            )
            rows = cur.fetchall()
            conn.close()
        except Exception:
            await message.answer("⚠️ Не удалось прочитать заявки.")
            return

        if not rows:
            await message.answer("Заявок pending нет.")
            return

        for (app_id, user_id, username, full_name, about_text, created_at) in rows:
            ikb = InlineKeyboardMarkup(row_width=2)
            ikb.add(
                InlineKeyboardButton("✅ Принять", callback_data=f"adm_app_accept:{int(user_id)}:{int(app_id)}"),
                InlineKeyboardButton("❌ Отклонить", callback_data=f"adm_app_reject:{int(user_id)}:{int(app_id)}"),
            )
            text = (
                f"Заявка #{app_id}\n"
                f"{(full_name or '(без имени)')}\n"
                f"{('@' + username) if username else ''}\n"
                f"user_id: {user_id}\n"
                f"Дата: {created_at or ''}\n\n"
                f"{about_text or ''}"
            )
            await message.answer(text, reply_markup=ikb)

    # =========================
    # 2) ADMIN: Удалить запрос (is_deleted=1) — по ТЗ
    # =========================

    @dp.message_handler(lambda m: m.chat.type == "private" and (m.text or "").strip() == BTN_ADMIN_DELETE_REQ)
    async def admin_delete_request_start(message: types.Message):
        uid = message.from_user.id
        if not getattr(db, "is_admin", lambda _x: False)(uid):
            await message.answer("⚠️ Нет доступа.")
            await _open_help_menu(message)
            return

        _clear_state(uid)
        _set_step(uid, "admin_delreq_wait")
        await message.answer("Введи номер запроса (например 15) или 'отмена'.")

    @dp.message_handler(lambda m: m.chat.type == "private" and _get_step(m.from_user.id) == "admin_delreq_wait")
    async def admin_delete_request_wait(message: types.Message):
        uid = message.from_user.id

        if _is_any_menu_button(message.text):
            _clear_state(uid)
            await _open_help_menu(message)
            return

        txt = (message.text or "").strip().lower()
        if txt == "отмена":
            _clear_state(uid)
            await message.answer("Ок, отменено.", reply_markup=_help_menu_kb(uid))
            return

        try:
            rid = int((message.text or "").strip())
        except Exception:
            await message.answer("Введи номер запроса (целое число) или 'отмена'.")
            return

        get_req = getattr(db, "get_request", None)
        if not callable(get_req):
            _clear_state(uid)
            await message.answer("⚠️ Не удалось открыть запрос.", reply_markup=_help_menu_kb(uid))
            return

        req = get_req(rid)
        if not req:
            await message.answer("⚠️ Запрос не найден.")
            return

        essence = (req.get("problem_essence") or "")[:80]
        gid = int(req.get("guild_id") or 0)
        created = req.get("created_at") or ""
        is_closed = int(req.get("is_closed") or 0)

        preview = (
            f"Запрос #{rid}\n"
            f"Гильдия: {_guild_name(gid)}\n"
            f"Суть: {essence}\n"
            f"Дата: {created}\n"
            f"Статус: {'закрыт' if is_closed else 'активен'}\n\n"
            "Удалить запрос из ленты экспертов?\n"
            "У пользователя в 'Мои запросы' он останется."
        )

        user_state.setdefault(uid, {})
        user_state[uid]["admin_del_rid"] = rid

        ikb = InlineKeyboardMarkup(row_width=2)
        ikb.add(
            InlineKeyboardButton("✅ Да", callback_data="adm_delreq_yes"),
            InlineKeyboardButton("❌ Нет", callback_data="adm_delreq_no"),
        )
        await message.answer(preview, reply_markup=ikb)

    @dp.callback_query_handler(lambda c: c.data in {"adm_delreq_yes", "adm_delreq_no"})
    async def admin_delete_request_confirm(call: types.CallbackQuery):
        uid = call.from_user.id
        if not getattr(db, "is_admin", lambda _x: False)(uid):
            await call.answer("Нет доступа", show_alert=True)
            return

        if call.data == "adm_delreq_no":
            user_state.get(uid, {}).pop("admin_del_rid", None)
            _set_step(uid, None)
            await call.message.answer("Ок, отменено.", reply_markup=_help_menu_kb(uid))
            await call.answer()
            return

        rid = int((user_state.get(uid) or {}).get("admin_del_rid") or 0)
        if not rid:
            await call.message.answer("⚠️ Не удалось определить запрос.")
            await call.answer()
            return

        ok = _delete_request_from_feed_direct(rid)
        user_state.get(uid, {}).pop("admin_del_rid", None)
        _set_step(uid, None)

        await call.message.answer(
            f"✅ Запрос #{rid} удалён из ленты экспертов." if ok else "⚠️ Не удалось удалить запрос.",
            reply_markup=_help_menu_kb(uid)
        )
        await call.answer()

    # =========================
    # 3) CREATE HELP REQUEST (student) — без изменений механики
    # =========================

    @dp.message_handler(lambda m: m.chat.type == "private" and (m.text or "").strip() == BTN_HELP_REQUEST)
    async def start_help_request(message: types.Message):
        uid = message.from_user.id
        user_state.setdefault(uid, {})
        user_state[uid]["hr"] = {}
        _set_step(uid, "hr_choose_guild")

        GUILDS = getattr(kb, "GUILDS", ["Гильдия 1", "Гильдия 2", "Гильдия 3", "Гильдия 4", "Гильдия 5"])
        ikb = InlineKeyboardMarkup(row_width=1)
        for i, g in enumerate(GUILDS, start=1):
            ikb.add(InlineKeyboardButton(g, callback_data=f"hr_guild:{i}"))
        await message.answer("Выбери гильдию:", reply_markup=ikb)

    @dp.callback_query_handler(lambda c: c.data and c.data.startswith("hr_guild:"))
    async def hr_choose_guild(call: types.CallbackQuery):
        uid = call.from_user.id
        try:
            guild_id = int(call.data.split(":", 1)[1])
        except Exception:
            await call.answer()
            return

        user_state.setdefault(uid, {})
        user_state[uid].setdefault("hr", {})
        user_state[uid]["hr"]["guild_id"] = guild_id
        _set_step(uid, "hr_q1")

        await call.message.answer("Коротко опиши суть проблемы:")
        await call.answer()

    @dp.message_handler(lambda m: m.chat.type == "private" and _get_step(m.from_user.id) in {"hr_q1", "hr_q2", "hr_q3", "hr_q4"})
    async def hr_questions(message: types.Message):
        uid = message.from_user.id

        if _is_any_menu_button(message.text):
            _clear_state(uid)
            await _open_help_menu(message)
            return

        txt = (message.text or "").strip()
        if not txt:
            await message.answer("Напиши ответ текстом.")
            return

        st = user_state.get(uid) or {}
        hr = st.get("hr") or {}
        step = _get_step(uid)

        if step == "hr_q1":
            hr["problem_essence"] = txt
            st["hr"] = hr
            _set_step(uid, "hr_q2")
            await message.answer("Как давно это длится?")
            return

        if step == "hr_q2":
            hr["problem_since"] = txt
            st["hr"] = hr
            _set_step(uid, "hr_q3")
            await message.answer("Что ты уже пробовал(а)?")
            return

        if step == "hr_q3":
            hr["tried_actions"] = txt
            st["hr"] = hr
            _set_step(uid, "hr_q4")
            await message.answer("Какой результат ты хочешь получить?")
            return

        if step == "hr_q4":
            hr["desired_result"] = txt
            st["hr"] = hr

            try:
                create_help_request = getattr(db, "create_help_request", None)
                if not callable(create_help_request):
                    raise RuntimeError("create_help_request not found")

                rid = create_help_request(
                    uid,
                    int(hr.get("guild_id") or 0),
                    hr.get("problem_essence") or "",
                    hr.get("problem_since") or "",
                    hr.get("tried_actions") or "",
                    hr.get("desired_result") or "",
                )
            except Exception:
                log.exception("create_help_request failed")
                _clear_state(uid)
                await message.answer("Не удалось отправить запрос. Попробуй позже.", reply_markup=_help_menu_kb(uid))
                return

            _clear_state(uid)
            await message.answer(f"✅ Запрос отправлен. Номер: #{rid}", reply_markup=_help_menu_kb(uid))
            return

    # =========================
    # 4) MY REQUESTS (student) — без изменений механики
    # =========================

    @dp.message_handler(lambda m: m.chat.type == "private" and (m.text or "").strip() == BTN_MY_REQUESTS)
    async def my_requests(message: types.Message):
        uid = message.from_user.id
        _clear_state(uid)

        list_user_requests = getattr(db, "list_user_requests", None)
        if not callable(list_user_requests):
            await message.answer("Функция 'Мои запросы' недоступна.", reply_markup=_help_menu_kb(uid))
            return

        items = list_user_requests(uid, limit=20) or []
        if not items:
            await message.answer("У тебя пока нет запросов.", reply_markup=_help_menu_kb(uid))
            return

        for r in items:
            rid = int(r.get("request_id") or 0)
            gid = int(r.get("guild_id") or 0)
            essence = (r.get("problem_essence") or "").strip()
            short = essence[:80] + ("…" if len(essence) > 80 else "")
            created = r.get("created_at") or ""
            is_closed = int(r.get("is_closed") or 0)
            status = "закрыт" if is_closed else "активен"
            answers_count = int(r.get("answers_count") or 0)

            text = (
                f"Запрос #{rid}\n"
                f"Гильдия: {_guild_name(gid)}\n"
                f"Суть: {short}\n"
                f"Ответов: {answers_count}\n"
                f"Дата: {created}\n"
                f"Статус: {status}"
            )
            ikb = InlineKeyboardMarkup()
            ikb.add(InlineKeyboardButton("Открыть", callback_data=f"my_open:{rid}"))
            await message.answer(text, reply_markup=ikb)

    @dp.callback_query_handler(lambda c: c.data and c.data.startswith("my_open:"))
    async def my_open(call: types.CallbackQuery):
        uid = call.from_user.id
        rid = int(call.data.split(":", 1)[1])

        get_request = getattr(db, "get_request", None)
        if not callable(get_request):
            await call.answer()
            return

        r = get_request(rid)
        if not r or int(r.get("student_user_id") or 0) != int(uid):
            await call.message.answer("⚠️ Нет доступа.")
            await call.answer()
            return

        answers_fn = getattr(db, "list_answers_for_request", None)
        answers = answers_fn(rid) if callable(answers_fn) else []
        answers_count = len(answers or [])

        is_closed = int(r.get("is_closed") or 0)
        status = "закрыт" if is_closed else "активен"

        full = (
            f"Запрос #{rid}\n"
            f"Гильдия: {_guild_name(int(r.get('guild_id') or 0))}\n\n"
            f"Суть:\n{r.get('problem_essence') or ''}\n\n"
            f"Как давно:\n{r.get('problem_since') or ''}\n\n"
            f"Что пробовал(а):\n{r.get('tried_actions') or ''}\n\n"
            f"Результат:\n{r.get('desired_result') or ''}\n\n"
            f"Статус: {status}\n"
            f"Ответы экспертов: {answers_count}"
        )

        ikb = InlineKeyboardMarkup(row_width=1)
        ikb.add(InlineKeyboardButton(f"Ответы ({answers_count})", callback_data=f"my_answers:{rid}"))
        if not is_closed:
            ikb.add(InlineKeyboardButton("Закрыть запрос", callback_data=f"my_close:{rid}"))
        await call.message.answer(full, reply_markup=ikb)
        await call.answer()

    @dp.callback_query_handler(lambda c: c.data and c.data.startswith("my_answers:"))
    async def my_answers(call: types.CallbackQuery):
        uid = call.from_user.id
        rid = int(call.data.split(":", 1)[1])

        get_request = getattr(db, "get_request", None)
        if not callable(get_request):
            await call.answer()
            return
        r = get_request(rid)
        if not r or int(r.get("student_user_id") or 0) != int(uid):
            await call.message.answer("⚠️ Нет доступа.")
            await call.answer()
            return

        answers_fn = getattr(db, "list_answers_for_request", None)
        answers = answers_fn(rid) if callable(answers_fn) else []
        if not answers:
            await call.message.answer("Пока нет ответов.")
            await call.answer()
            return

        ikb = InlineKeyboardMarkup(row_width=1)
        lines = []
        for i, a in enumerate(answers, start=1):
            aid = int(a.get("answer_id") or 0)
            dt = a.get("created_at") or ""
            uname = a.get("expert_username") or ""
            label = f"Ответ #{i} — {dt}" + (f" — @{uname}" if uname else "")
            lines.append(label)
            ikb.add(InlineKeyboardButton(f"Открыть ответ #{i}", callback_data=f"my_answer_open:{aid}:{rid}"))

        await call.message.answer("\n".join(lines), reply_markup=ikb)
        await call.answer()

    @dp.callback_query_handler(lambda c: c.data and c.data.startswith("my_answer_open:"))
    async def my_answer_open(call: types.CallbackQuery):
        uid = call.from_user.id
        _, aid_str, rid_str = call.data.split(":")
        aid = int(aid_str)
        rid = int(rid_str)

        get_request = getattr(db, "get_request", None)
        if not callable(get_request):
            await call.answer()
            return
        r = get_request(rid)
        if not r or int(r.get("student_user_id") or 0) != int(uid):
            await call.message.answer("⚠️ Нет доступа.")
            await call.answer()
            return

        answers_fn = getattr(db, "list_answers_for_request", None)
        answers = answers_fn(rid) if callable(answers_fn) else []
        found = None
        for a in (answers or []):
            if int(a.get("answer_id") or 0) == aid:
                found = a
                break

        if not found:
            await call.message.answer("Ответ не найден.")
            await call.answer()
            return

        await call.message.answer(found.get("answer_text") or "")
        await call.answer()

    @dp.callback_query_handler(lambda c: c.data and c.data.startswith("my_close:"))
    async def my_close(call: types.CallbackQuery):
        uid = call.from_user.id
        rid = int(call.data.split(":", 1)[1])

        close_request = getattr(db, "close_request", None)
        if not callable(close_request):
            await call.answer()
            return

        ok = False
        try:
            ok = bool(close_request(rid, uid))
        except Exception:
            ok = False

        await call.message.answer("✅ Запрос закрыт." if ok else "⚠️ Не удалось закрыть запрос.")
        await call.answer()

    # =========================
    # 5) EXPERT: Ответить на запросы + 3 шага ответа + уведомление пользователю
    # =========================

    @dp.message_handler(lambda m: m.chat.type == "private" and (m.text or "").strip() == BTN_HELP_ANSWER)
    async def expert_list_requests(message: types.Message):
        uid = message.from_user.id
        _clear_state(uid)

        if not getattr(db, "is_expert", lambda _x: False)(uid):
            await message.answer("У тебя нет роли эксперта.", reply_markup=_help_menu_kb(uid))
            return

        list_open_requests = getattr(db, "list_open_requests", None)
        if not callable(list_open_requests):
            await message.answer("Функция списка запросов недоступна.", reply_markup=_help_menu_kb(uid))
            return

        items = list_open_requests(limit=10) or []
        if not items:
            await message.answer("Нет активных запросов.", reply_markup=_help_menu_kb(uid))
            return

        for r in items:
            rid = int(r.get("request_id") or 0)
            gid = int(r.get("guild_id") or 0)
            essence = (r.get("problem_essence") or "").strip()
            short = essence[:80] + ("…" if len(essence) > 80 else "")
            answers_count = int(r.get("answers_count") or 0)
            created = r.get("created_at") or ""
            text = (
                f"Запрос #{rid}\n"
                f"Гильдия: {_guild_name(gid)}\n"
                f"Суть: {short}\n"
                f"Ответов: {answers_count}\n"
                f"Дата: {created}"
            )
            ikb = InlineKeyboardMarkup()
            ikb.add(InlineKeyboardButton("Открыть", callback_data=f"ex_open:{rid}"))
            await message.answer(text, reply_markup=ikb)

    @dp.callback_query_handler(lambda c: c.data and c.data.startswith("ex_open:"))
    async def ex_open(call: types.CallbackQuery):
        uid = call.from_user.id
        rid = int(call.data.split(":", 1)[1])

        if not getattr(db, "is_expert", lambda _x: False)(uid):
            await call.answer()
            return

        get_request = getattr(db, "get_request", None)
        if not callable(get_request):
            await call.answer()
            return

        r = get_request(rid)
        if not r:
            await call.message.answer("⚠️ Запрос не найден.")
            await call.answer()
            return

        is_closed = int(r.get("is_closed") or 0)
        status = "закрыт" if is_closed else "активен"

        answers_fn = getattr(db, "list_answers_for_request", None)
        answers = answers_fn(rid) if callable(answers_fn) else []
        answers_count = len(answers or [])

        txt = (
            f"Запрос #{rid}\n"
            f"Гильдия: {_guild_name(int(r.get('guild_id') or 0))}\n\n"
            f"Суть:\n{r.get('problem_essence') or ''}\n\n"
            f"Как давно:\n{r.get('problem_since') or ''}\n\n"
            f"Что пробовал(а):\n{r.get('tried_actions') or ''}\n\n"
            f"Результат:\n{r.get('desired_result') or ''}\n\n"
            f"Статус: {status}\n"
            f"Ответов: {answers_count}"
        )

        ikb = InlineKeyboardMarkup(row_width=1)
        if not is_closed:
            ikb.add(InlineKeyboardButton("Ответить", callback_data=f"ex_answer:{rid}"))
        await call.message.answer(txt, reply_markup=ikb)
        await call.answer()

    @dp.callback_query_handler(lambda c: c.data and c.data.startswith("ex_answer:"))
    async def ex_answer_start(call: types.CallbackQuery):
        uid = call.from_user.id
        if not getattr(db, "is_expert", lambda _x: False)(uid):
            await call.answer()
            return

        rid = int(call.data.split(":", 1)[1])

        if callable(getattr(db, "is_request_closed", None)) and db.is_request_closed(rid):
            await call.message.answer("⚠️ Запрос закрыт пользователем. Ответы больше не принимаются.")
            await call.answer()
            return

        user_state.setdefault(uid, {})
        user_state[uid]["ex"] = {"rid": rid, "cause": None, "quick": None, "quality": None}
        _set_step(uid, "ex_q1")
        await call.message.answer("Причина проблемы:")
        await call.answer()

    @dp.message_handler(lambda m: m.chat.type == "private" and _get_step(m.from_user.id) in {"ex_q1", "ex_q2", "ex_q3"})
    async def ex_answer_steps(message: types.Message):
        uid = message.from_user.id

        if _is_any_menu_button(message.text):
            _clear_state(uid)
            await _open_help_menu(message)
            return

        if not getattr(db, "is_expert", lambda _x: False)(uid):
            _clear_state(uid)
            await message.answer("У тебя нет роли эксперта.", reply_markup=_help_menu_kb(uid))
            return

        st = user_state.get(uid) or {}
        ex = st.get("ex") or {}
        rid = int(ex.get("rid") or 0)
        if not rid:
            _clear_state(uid)
            await message.answer("Не удалось продолжить ответ. Открой запрос заново.", reply_markup=_help_menu_kb(uid))
            return

        if callable(getattr(db, "is_request_closed", None)) and db.is_request_closed(rid):
            _clear_state(uid)
            await message.answer("⚠️ Запрос закрыт пользователем. Ответы больше не принимаются.", reply_markup=_help_menu_kb(uid))
            return

        txt = (message.text or "").strip()
        if not txt:
            await message.answer("Напиши ответ текстом.")
            return

        step = _get_step(uid)

        if step == "ex_q1":
            ex["cause"] = txt
            st["ex"] = ex
            _set_step(uid, "ex_q2")
            await message.answer("Быстрое решение:")
            return

        if step == "ex_q2":
            ex["quick"] = txt
            st["ex"] = ex
            _set_step(uid, "ex_q3")
            await message.answer("Качественное решение:")
            return

        if step == "ex_q3":
            ex["quality"] = txt
            st["ex"] = ex

            answer_text = (
                f"Причина проблемы: {ex.get('cause')}\n\n"
                f"Быстрое решение: {ex.get('quick')}\n\n"
                f"Качественное решение: {ex.get('quality')}"
            )

            add_answer = getattr(db, "try_save_answer", None)
            if not callable(add_answer):
                add_answer = getattr(db, "add_answer", None)

            ok = False
            try:
                ok = bool(add_answer(uid, rid, answer_text, message.from_user.username, message.from_user.full_name))
            except TypeError:
                try:
                    ok = bool(add_answer(uid, rid, answer_text))
                except Exception:
                    ok = False
            except Exception:
                ok = False

            _clear_state(uid)

            if not ok:
                await message.answer("⚠️ Ответ не принят (возможно, ты уже отвечал(а) на этот запрос или он закрыт).", reply_markup=_help_menu_kb(uid))
                return

            # уведомление студенту (фикс: не молчим, если Telegram не дал отправить)
            delivered = False
            student_id = 0
            try:
                req = getattr(db, "get_request", None)(rid)
                student_id = int((req or {}).get("student_user_id") or 0)
                if student_id:
                    try:
                        await bot.send_message(student_id, f"📩 Ответ эксперта по запросу #{rid}:\n\n{answer_text}")
                        delivered = True
                    except Exception:
                        delivered = False
            except Exception:
                delivered = False

            await message.answer(
                f"✅ Ответ отправлен пользователю по запросу #{rid}."
                + ("" if delivered else "\n⚠️ Уведомление пользователю не доставлено (возможно, пользователь не запускал бота или запретил сообщения)."),
                reply_markup=_help_menu_kb(uid)
            )
            return

    # =========================
    # 6) APPLY EXPERT + approve/reject callbacks (как у тебя работало)
    # =========================

    @dp.message_handler(lambda m: m.chat.type == "private" and (m.text or "").strip() == BTN_APPLY_EXPERT)
    async def apply_expert(message: types.Message):
        uid = message.from_user.id

        if getattr(db, "is_expert", lambda _x: False)(uid):
            await message.answer("⚠️ Ты уже эксперт.", reply_markup=_help_menu_kb(uid))
            return

        get_pending = getattr(db, "get_pending_application", None)
        if callable(get_pending):
            try:
                if get_pending(uid):
                    await message.answer("⚠️ Твоя заявка уже на рассмотрении. Дождись решения администратора.", reply_markup=_help_menu_kb(uid))
                    return
            except Exception:
                pass

        user_state.setdefault(uid, {})
        user_state[uid]["apply"] = {"q1": None, "q2": None, "q3": None}
        _set_step(uid, "apply_q1")
        await message.answer("Почему ты хочешь стать экспертом?")

    @dp.message_handler(lambda m: m.chat.type == "private" and _get_step(m.from_user.id) in {"apply_q1", "apply_q2", "apply_q3"})
    async def apply_steps(message: types.Message):
        uid = message.from_user.id

        if _is_any_menu_button(message.text):
            _clear_state(uid)
            await _open_help_menu(message)
            return

        if getattr(db, "is_expert", lambda _x: False)(uid):
            _clear_state(uid)
            await message.answer("⚠️ Ты уже эксперт.", reply_markup=_help_menu_kb(uid))
            return

        txt = (message.text or "").strip()
        if not txt:
            await message.answer("Напиши ответ текстом.")
            return

        st = user_state.get(uid) or {}
        apply = st.get("apply") or {}
        step = _get_step(uid)

        if step == "apply_q1":
            apply["q1"] = txt
            st["apply"] = apply
            _set_step(uid, "apply_q2")
            await message.answer("В чем твоя компетенция и опыт?")
            return

        if step == "apply_q2":
            apply["q2"] = txt
            st["apply"] = apply
            _set_step(uid, "apply_q3")
            await message.answer("Сколько времени готов(а) уделять в неделю?")
            return

        if step == "apply_q3":
            apply["q3"] = txt
            st["apply"] = apply

            about_text = (
                f"1) Почему хочу стать экспертом:\n{apply.get('q1')}\n\n"
                f"2) Компетенция и опыт:\n{apply.get('q2')}\n\n"
                f"3) Время в неделю:\n{apply.get('q3')}"
            )

            username = message.from_user.username
            full_name = (message.from_user.full_name or "").strip() or None

            app_id = None
            create_app = getattr(db, "create_expert_application", None)
            if callable(create_app):
                try:
                    app_id = create_app(uid, username, full_name, about_text)
                except Exception:
                    log.exception("create_expert_application failed")
                    _clear_state(uid)
                    await message.answer("Не удалось отправить заявку. Попробуй позже.", reply_markup=_help_menu_kb(uid))
                    return

                if app_id == -1:
                    _clear_state(uid)
                    await message.answer("⚠️ Твоя заявка уже на рассмотрении. Дождись решения администратора.", reply_markup=_help_menu_kb(uid))
                    return

            _clear_state(uid)

            ikb = InlineKeyboardMarkup(row_width=2)
            ikb.add(
                InlineKeyboardButton("✅ Принять", callback_data=f"adm_app_accept:{uid}:{app_id or 0}"),
                InlineKeyboardButton("❌ Отклонить", callback_data=f"adm_app_reject:{uid}:{app_id or 0}"),
            )

            admin_ids = getattr(db, "ADMIN_USER_IDS", None)
            if not admin_ids:
                try:
                    from ..config import ADMIN_USER_IDS as admin_ids  # noqa
                except Exception:
                    admin_ids = []

            text = (
                "🧑‍🏫 Новая заявка в эксперты\n"
                f"{(full_name or '(без имени)')}\n"
                f"{('@' + username) if username else ''}\n"
                f"user_id: {uid}\n"
                f"application_id: {app_id if app_id is not None else '(n/a)'}\n\n"
                f"{about_text}"
            )

            for admin_id in (admin_ids or []):
                try:
                    await bot.send_message(int(admin_id), text, reply_markup=ikb)
                except Exception:
                    pass

            await message.answer("✅ Заявка отправлена администраторам.", reply_markup=_help_menu_kb(uid))
            return

    @dp.callback_query_handler(lambda c: c.data and c.data.startswith("adm_app_accept:"))
    async def adm_app_accept(call: types.CallbackQuery):
        admin_id = call.from_user.id
        if not getattr(db, "is_admin", lambda _x: False)(admin_id):
            await call.answer("Нет доступа", show_alert=True)
            return

        _, uid_str, app_id_str = call.data.split(":")
        uid = int(uid_str)
        app_id = int(app_id_str)

        ok, _msg = getattr(db, "add_expert", lambda *_: (False, "err"))(uid, admin_id)

        real_app_id = app_id if app_id > 0 else (_find_latest_pending_app_id_for_user(uid) or 0)
        if real_app_id > 0:
            _set_application_status_direct(real_app_id, "approved")

        try:
            await bot.send_message(uid, "✅ Заявка одобрена. Ты добавлен(а) в эксперты.")
        except Exception:
            pass

        try:
            await call.message.edit_reply_markup()
        except Exception:
            pass

        await call.answer("Принято")

    @dp.callback_query_handler(lambda c: c.data and c.data.startswith("adm_app_reject:"))
    async def adm_app_reject(call: types.CallbackQuery):
        admin_id = call.from_user.id
        if not getattr(db, "is_admin", lambda _x: False)(admin_id):
            await call.answer("Нет доступа", show_alert=True)
            return

        _, uid_str, app_id_str = call.data.split(":")
        uid = int(uid_str)
        app_id = int(app_id_str)

        real_app_id = app_id if app_id > 0 else (_find_latest_pending_app_id_for_user(uid) or 0)
        if real_app_id > 0:
            _set_application_status_direct(real_app_id, "rejected")

        try:
            await bot.send_message(uid, "❌ Заявка отклонена. Ты можешь подать новую заявку.")
        except Exception:
            pass

        try:
            await call.message.edit_reply_markup()
        except Exception:
            pass

        await call.answer("Отклонено")
