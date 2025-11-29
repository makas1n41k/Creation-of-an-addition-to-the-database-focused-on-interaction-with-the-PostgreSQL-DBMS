import time
import psycopg

from model import Model
from view import View


class Controller:
    def __init__(self, model: Model, view: View):
        self.m = model
        self.v = view

    def run(self):
        self.v.info("Підключення до БД — OK.")
        while True:
            ch = self.v.main_menu()
            try:
                if   ch == "1": self.menu_users()
                elif ch == "2": self.menu_books()
                elif ch == "3": self.menu_activity()
                elif ch == "4": self.menu_impressions()
                elif ch == "5": self.menu_generate()
                elif ch == "6": self.menu_searches()
                elif ch == "0": break
            except psycopg.errors.ForeignKeyViolation as e:
                self.v.err(f"Порушення зовнішнього ключа (FK). Операцію скасовано. ({e.sqlstate or '—'}: {e})")
            except psycopg.errors.UniqueViolation as e:
                self.v.err(f"Порушення унікальності (username/tg_handle/title тощо). ({e.sqlstate or '—'}: {e})")
            except psycopg.Error as e:
                self.v.err(f"Помилка БД (SQLSTATE={e.sqlstate or '—'}; {e.__class__.__name__}: {e})")
            except Exception as e:
                self.v.err(f"Непередбачена помилка: {e}")

    # ===== Допоміжні методи вибору сутностей (БЕЗ введення ID) =====

    def _select_user_interactive(self):
        """Інтерактивний вибір користувача за full_name / username."""
        self.v.info("Пошук користувача (за повним ім'ям та/або логіном).")
        full = self.v.ask_like("Шаблон повного імені (LIKE, можна порожньо): ")
        uname = self.v.ask_like("Шаблон логіна (LIKE, можна порожньо): ")
        rows = self.m.users_search_simple(full, uname)
        if not rows:
            self.v.warn("Користувачів не знайдено.")
            return None
        user = self.v.choose_from_rows(rows, ["full_name", "username", "tg_handle", "created_at"])
        if user:
            self.v.info(f"Обрано користувача: {user['full_name']} ({user['username']})")
        return user

    def _select_book_interactive(self):
        """Інтерактивний вибір книги за title / author / genre."""
        self.v.info("Пошук книги (за назвою / автором / жанром).")
        title = self.v.ask_like("Шаблон назви (LIKE, можна порожньо): ")
        author = self.v.ask_like("Шаблон автора (LIKE, можна порожньо): ")
        genre = self.v.ask_like("Шаблон жанру (LIKE, можна порожньо): ")
        rows = self.m.books_search_simple(title, author, genre)
        if not rows:
            self.v.warn("Книг не знайдено.")
            return None
        book = self.v.choose_from_rows(rows, ["title", "author", "genre", "created_at"])
        if book:
            self.v.info(f"Обрано книгу: «{book['title']}» ({book['author']}, {book['genre']})")
        return book

    def _select_activity_for_user(self, user_id: int):
        """Вибір книги з Activity для конкретного користувача."""
        rows = self.m.activity_for_user(user_id)
        if not rows:
            self.v.warn("У цього користувача немає записів Activity.")
            return None
        act = self.v.choose_from_rows(rows, ["title", "author", "genre"])
        if act:
            self.v.info(f"Обрано пару Activity: {act['full_name']} — «{act['title']}»")
        return act

    def _select_impression_for_user(self, user_id: int):
        """Вибір конкретного відгуку (book_impressions) для користувача."""
        rows = self.m.impressions_for_user(user_id)
        if not rows:
            self.v.warn("У цього користувача немає відгуків (Book_Impressions).")
            return None
        impr = self.v.choose_from_rows(rows, ["title", "rating", "comment", "created_at"])
        if impr:
            self.v.info(f"Обрано відгук на «{impr['title']}» з оцінкою {impr['rating']}")
        return impr

    # ===== Users =====
    def menu_users(self):
        while True:
            ch = self.v.submenu_crud('Users')
            if ch == "1":
                self.v.show_rows(self.m.users_list())
            elif ch == "2":
                full = self.v.ask_str("Повне ім'я: ")
                uname = self.v.ask_str("Логін (унікальний): ")
                tg = self.v.ask_str("Telegram (@... або порожньо): ", allow_empty=True) or None
                uid = self.m.users_create(full, uname, tg)
                self.v.info(f"Додано user_id={uid}")
            elif ch == "3":
                # ОНОВЛЕННЯ БЕЗ ВВЕДЕННЯ ID: вибір користувача по full_name/username
                u = self._select_user_interactive()
                if not u:
                    continue
                full = self.v.ask_str(f"Повне ім'я [{u['full_name']}]: ", allow_empty=True) or u['full_name']
                uname = self.v.ask_str(f"Логін [{u['username']}]: ", allow_empty=True) or u['username']
                tg = self.v.ask_str(f"TG [{u['tg_handle'] or ''}]: ", allow_empty=True) or None
                cnt = self.m.users_update(u["user_id"], full, uname, tg)
                self.v.info(f"Оновлено рядків: {cnt}")
            elif ch == "4":
                # ВИДАЛЕННЯ БЕЗ ВВЕДЕННЯ ID
                u = self._select_user_interactive()
                if not u:
                    continue
                uid = u["user_id"]
                dep = self.m.count_activity_by_user(uid) + self.m.count_impressions_by_user(uid)
                if dep > 0:
                    self.v.err("Заборонено: є залежні activity/book_impressions.")
                else:
                    if not self.v.confirm(f"Дійсно видалити користувача {u['full_name']} ({u['username']})?"):
                        self.v.info("Видалення скасовано.")
                    else:
                        self.v.info(f"Видалено рядків: {self.m.users_delete(uid)}")
            elif ch == "0":
                break

    # ===== Books =====
    def menu_books(self):
        while True:
            ch = self.v.submenu_crud('Books')
            if ch == "1":
                self.v.show_rows(self.m.books_list())
            elif ch == "2":
                title = self.v.ask_str("Назва: ")
                author = self.v.ask_str("Автор: ")
                genre = self.v.ask_str("Жанр: ")
                bid = self.m.books_create(title, author, genre)
                self.v.info(f"Додано book_id={bid}")
            elif ch == "3":
                # ОНОВЛЕННЯ БЕЗ ВВЕДЕННЯ ID: вибір книги по назві/автору/жанру
                b = self._select_book_interactive()
                if not b:
                    continue
                title = self.v.ask_str(f"Назва [{b['title']}]: ", allow_empty=True) or b['title']
                author = self.v.ask_str(f"Автор [{b['author']}]: ", allow_empty=True) or b['author']
                genre = self.v.ask_str(f"Жанр [{b['genre']}]: ", allow_empty=True) or b['genre']
                cnt = self.m.books_update(b["book_id"], title, author, genre)
                self.v.info(f"Оновлено рядків: {cnt}")
            elif ch == "4":
                # ВИДАЛЕННЯ БЕЗ ВВЕДЕННЯ ID
                b = self._select_book_interactive()
                if not b:
                    continue
                bid = b["book_id"]
                dep = self.m.count_activity_by_book(bid) + self.m.count_impressions_by_book(bid)
                if dep > 0:
                    self.v.err("Заборонено: є залежні activity/book_impressions.")
                else:
                    if not self.v.confirm(f"Дійсно видалити книгу «{b['title']}» ({b['author']})?"):
                        self.v.info("Видалення скасовано.")
                    else:
                        self.v.info(f"Видалено рядків: {self.m.books_delete(bid)}")
            elif ch == "0":
                break

    # ===== Activity (PK: user_id, book_id) =====
    def menu_activity(self):
        while True:
            ch = self.v.submenu_crud('Activity (user_id, book_id)')
            if ch == "1":
                self.v.show_rows(self.m.activity_list())
            elif ch == "2":
                # ДОДАВАННЯ ПАРИ ЧЕРЕЗ ВИБІР КОРИСТУВАЧА І КНИГИ, БЕЗ ВВЕДЕННЯ ID
                u = self._select_user_interactive()
                if not u:
                    continue
                b = self._select_book_interactive()
                if not b:
                    continue
                uid = u["user_id"]
                bid = b["book_id"]
                added = self.m.activity_create(uid, bid)
                self.v.info("Додано." if added == 1 else "Така пара вже існує.")
            elif ch == "3":
                self.v.warn("Оновлення Activity не застосовується (PK — пара). Видаліть/створіть заново.")
            elif ch == "4":
                # ВИДАЛЕННЯ ПАРИ Activity БЕЗ ВВЕДЕННЯ ID
                u = self._select_user_interactive()
                if not u:
                    continue
                act = self._select_activity_for_user(u["user_id"])
                if not act:
                    continue
                uid = act["user_id"]
                bid = act["book_id"]
                dep = self.m.count_impressions_for_pair(uid, bid)
                if dep > 0:
                    self.v.err("Заборонено: є залежні book_impressions.")
                else:
                    if not self.v.confirm(f"Видалити Activity для {act['full_name']} — «{act['title']}»?"):
                        self.v.info("Видалення скасовано.")
                    else:
                        self.v.info(f"Видалено рядків: {self.m.activity_delete(uid, bid)}")
            elif ch == "0":
                break

    # ===== Book_Impressions =====
    def menu_impressions(self):
        while True:
            ch = self.v.submenu_impressions()
            if ch == "1":
                rows = self.m.impressions_list(); self.v.show_rows(rows)
            elif ch == "2":
                # ДОДАВАННЯ: спочатку обираємо користувача, потім книгу з його Activity
                self.v.info("Спочатку оберіть користувача та книгу (із наявних Activity).")
                u = self._select_user_interactive()
                if not u:
                    continue
                act = self._select_activity_for_user(u["user_id"])
                if not act:
                    continue
                uid = act["user_id"]
                bid = act["book_id"]
                rating = self.v.ask_decimal("Оцінка (0.0-5.0): ", 0.0, 5.0, 1)
                comment = self.v.ask_str("Коментар (може бути порожнім): ", allow_empty=True) or None
                rid = self.m.impressions_create(uid, bid, rating, comment)
                self.v.info(f"Додано rating_id={rid}")
            elif ch == "3":
                # ОНОВЛЕННЯ: вибір користувача, потім конкретного відгуку
                self.v.info("Оберіть користувача, чий відгук потрібно оновити.")
                u = self._select_user_interactive()
                if not u:
                    continue
                impr = self._select_impression_for_user(u["user_id"])
                if not impr:
                    continue
                rid = impr["rating_id"]
                rating = self.v.ask_decimal(f"Нова оцінка [{impr['rating']}]: ", 0.0, 5.0, 1)
                comment = self.v.ask_str(f"Новий коментар [{impr['comment'] or ''}]: ", allow_empty=True) or None
                cnt = self.m.impressions_update(rid, rating, comment)
                self.v.info(f"Оновлено рядків: {cnt}")
            elif ch == "4":
                # ВИДАЛЕННЯ: вибір користувача, потім конкретного відгуку
                self.v.info("Оберіть користувача, чий відгук потрібно видалити.")
                u = self._select_user_interactive()
                if not u:
                    continue
                impr = self._select_impression_for_user(u["user_id"])
                if not impr:
                    continue
                rid = impr["rating_id"]
                if not self.v.confirm(
                    f"Видалити відгук (rating_id={rid}) на «{impr['title']}» з оцінкою {impr['rating']}?"
                ):
                    self.v.info("Видалення скасовано.")
                else:
                    cnt = self.m.impressions_delete(rid)
                    self.v.info(f"Видалено рядків: {cnt}")
            elif ch == "5":  # ДЕМО без перевірки activity: можливі FK-помилки
                self.v.info("ДЕМО: додавання відгуку БЕЗ перевірки activity (можлива FK-помилка).")
                # тут теж НІЯКИХ ID з клавіатури — лише вибір по атрибутах
                u = self._select_user_interactive()
                if not u:
                    continue
                b = self._select_book_interactive()
                if not b:
                    continue

                uid = u["user_id"]
                bid = b["book_id"]
                rating = self.v.ask_decimal("Оцінка (0.0-5.0): ", 0.0, 5.0, 1)
                comment = self.v.ask_str("Коментар (може бути порожнім): ", allow_empty=True) or None

                # НІЯКОЇ перевірки activity_exists → FK або спрацює, або впаде (демо)
                rid = self.m.impressions_create(uid, bid, rating, comment)
                self.v.info(f"(демо) Додано rating_id={rid}")
            elif ch == "0":
                break


    # ===== Generation =====
    def menu_generate(self):
        while True:
            ch = self.v.submenu_generate_books()
            try:
                if ch == "1":
                    n = self.v.ask_int("К-сть Users (напр. 5000): ", 1)
                    self.v.info(f"Додано Users: {self.m.generate_users(n)}")
                elif ch == "2":
                    n = self.v.ask_int("К-сть Books (напр. 3000): ", 1)
                    self.v.info(f"Додано Books: {self.m.generate_books(n)}")
                elif ch == "3":
                    n = self.v.ask_int("Скільки Activity вставити (напр. 10000): ", 1)
                    self.v.info(f"Додано Activity: {self.m.generate_activity(n)}")
                elif ch == "4":
                    n = self.v.ask_int("Скільки Impressions вставити (напр. 5000): ", 1)
                    self.v.info(f"Додано Book_Impressions: {self.m.generate_impressions(n)}")
                elif ch == "5":
                    n = self.v.ask_int("Базове N (конвеєр 1→2→3→4): ", 1)
                    a = self.m.generate_users(n)
                    b = self.m.generate_books(n)
                    c = self.m.generate_activity(max(n, 1))
                    d = self.m.generate_impressions(max(n//2, 1))
                    self.v.info(f"OK: Users={a}, Books={b}, Activity={c}, Impr={d}")
                elif ch == "0":
                    break
            except psycopg.Error as e:
                self.v.err(f"Помилка генерації ({e.__class__.__name__}, SQLSTATE={e.sqlstate or '—'}): {e}")

    # ===== Searches (with timing) =====
    def timed(self, fn, *args):
        t0 = time.perf_counter()
        rows = fn(*args)
        ms = (time.perf_counter() - t0) * 1000.0
        return rows, ms

    def menu_searches(self):
        while True:
            ch = self.v.submenu_searches_books()
            if ch == "1":
                title = self.v.ask_like("Шаблон title (напр. %Book#12% або порожньо): ")
                author = self.v.ask_like("Шаблон author (або порожньо): ")
                genre = self.v.ask_like("Шаблон genre (або порожньо): ")
                rmin = self.v.ask_decimal_optional("Мін. rating (порожньо — без мін.): ")
                rmax = self.v.ask_decimal_optional("Макс. rating (порожньо — без макс.): ")
                d1 = self.v.ask_date_optional("Дата від (YYYY-MM-DD)")
                d2 = self.v.ask_date_optional("Дата до  (YYYY-MM-DD)")
                has_tg = self.v.ask_has_tg()

                rows, ms = self.timed(self.m.search_multientity,
                                      title, author, genre, rmin, rmax, d1, d2, has_tg)
                self.v.show_rows(rows); self.v.info(f"Час: {ms:.1f} мс")

            elif ch == "2":
                d1 = self.v.ask_date_optional("Дата від (YYYY-MM-DD)")
                d2 = self.v.ask_date_optional("Дата до  (YYYY-MM-DD)")
                thr = self.v.ask_int("Мін. кількість вражень у групі: ", 1)
                grp = self.v.ask_str("Групувати за 'author' або 'genre': ")
                rows, ms = self.timed(self.m.search_aggregate_ratings, d1, d2, thr, grp)
                self.v.show_rows(rows); self.v.info(f"Час: {ms:.1f} мс")

            elif ch == "3":
                g = self.v.ask_like("Жанр/шаблон жанру: ")
                d1 = self.v.ask_date_optional("Дата активності від (YYYY-MM-DD)")
                d2 = self.v.ask_date_optional("Дата активності до  (YYYY-MM-DD)")
                rows, ms = self.timed(self.m.search_users_no_tg_by_genre, g, d1, d2)
                self.v.show_rows(rows); self.v.info(f"Час: {ms:.1f} мс")

            elif ch == "0":
                break
