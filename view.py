from datetime import date
from decimal import Decimal, InvalidOperation


class View:
    # ===== Menus =====
    def main_menu(self) -> str:
        print("=== Головне меню ===")
        print("1) CRUD: Users")
        print("2) CRUD: Books")
        print("3) CRUD: Activity (user_id, book_id)")
        print("4) CRUD: Book_Impressions")
        print("5) Генерація даних")
        print("6) Пошуки (мультикритерій/агрегації) + час виконання")
        print("0) Вихід")
        return input("> ").strip()

    def submenu_crud(self, title:str) -> str:
        print(f"\n--- {title} ---")
        print("1) Перегляд (перші 50)")
        print("2) Додати")
        print("3) Оновити")
        print("4) Видалити")
        print("0) Назад")
        return input("> ").strip()

    def submenu_generate_books(self) -> str:
        print("\n--- Генерація ---")
        print("1) Users (N)")
        print("2) Books (N)")
        print("3) Activity (унікальні пари user×book)")
        print("4) Book_Impressions (із наявних Activity)")
        print("5) Конвеєр 1→2→3→4")
        print("0) Назад")
        return input("> ").strip()

    def submenu_searches_books(self) -> str:
        print("\n--- Пошуки ---")
        print("1) Мультикритерій: title/author/genre (LIKE) + rating(range) + дати + has_tg")
        print("2) Агрегація: середні оцінки по author/genre у вікні дат (мін. кількість)")
        print("3) Користувачі без TG, що взаємодіяли з жанром у вікні дат")
        print("0) Назад")
        return input("> ").strip()

    def submenu_impressions(self) -> str:
        print("\n--- Book_Impressions ---")
        print("1) Перегляд (перші 50)")
        print("2) Додати (З ПЕРЕВІРКОЮ activity)")
        print("3) Оновити")
        print("4) Видалити")
        print("5) Додати БЕЗ перевірки (ДЕМО FK-помилки з боку СУБД)")
        print("0) Назад")
        return input("> ").strip()

    # ===== Output =====
    def show_rows(self, rows:list[dict]):
        if not rows:
            print("(порожньо)"); return
        for r in rows: print(r)

    def info(self, msg:str): print(f"[ІНФО] {msg}")
    def warn(self, msg:str): print(f"[УВАГА] {msg}")
    def err(self, msg:str):  print(f"[ПОМИЛКА] {msg}")

    # ===== Input & validation =====
    def ask_str(self, prompt:str, allow_empty=False) -> str:
        while True:
            s = input(prompt).strip()
            if s or allow_empty: return s
            print("Поле не може бути порожнім.")

    def ask_like(self, prompt:str) -> str|None:
        s = input(prompt).strip()
        if not s:
            return None
        return s if ("%" in s or "_" in s) else f"%{s}%"

    def ask_int(self, prompt:str, min_val:int|None=None, max_val:int|None=None) -> int:
        while True:
            s = input(prompt).strip()
            try:
                v = int(s)
                if (min_val is None or v >= min_val) and (max_val is None or v <= max_val):
                    return v
            except ValueError:
                pass
            print("Введіть коректне ціле число" +
                  (f" (від {min_val})" if min_val is not None else "") +
                  (f" (до {max_val})" if max_val is not None else "") + ".")

    def ask_int_optional(self, prompt:str, min_val:int|None=None, max_val:int|None=None) -> int|None:
        s = input(prompt + " (Enter — пропустити): ").strip()
        if s == "":
            return None
        try:
            v = int(s)
            if (min_val is None or v >= min_val) and (max_val is None or v <= max_val):
                return v
        except ValueError:
            pass
        print("Некоректне ціле — фільтр пропущено.")
        return None

    def ask_decimal(self, prompt:str, min_val:float, max_val:float, scale:int=1) -> float:
        q = Decimal('1').scaleb(-scale)
        while True:
            s = input(prompt).strip().replace(",", ".")
            try:
                v = Decimal(s).quantize(q)
                if v >= Decimal(str(min_val)) and v <= Decimal(str(max_val)):
                    return float(v)
            except (InvalidOperation, ValueError):
                pass
            print(f"Введіть десяткове число у діапазоні [{min_val}; {max_val}] з кроком {q}.")

    def ask_decimal_optional(self, prompt:str, scale:int=1) -> float|None:
        q = Decimal('1').scaleb(-scale)
        s = input(prompt + f" (крок {q}, Enter — пропустити): ").strip().replace(",", ".")
        if s == "":
            return None
        try:
            v = Decimal(s).quantize(q)
            return float(v)
        except (InvalidOperation, ValueError):
            print("Некоректне десяткове — фільтр пропущено.")
            return None

    def ask_date_iso(self, prompt:str) -> str:
        while True:
            s = input(prompt).strip()
            try:
                y, m, d = s.split("-"); _ = date(int(y), int(m), int(d)); return s
            except Exception:
                print("Дата має бути у форматі YYYY-MM-DD.")

    def ask_date_optional(self, prompt:str) -> str|None:
        s = input(prompt + " (Enter — пропустити): ").strip()
        if s == "":
            return None
        try:
            y, m, d = s.split("-"); _ = date(int(y), int(m), int(d)); return s
        except Exception:
            print("Некоректна дата — фільтр пропущено.")
            return None

    def ask_has_tg(self) -> str|None:
        s = input("Фільтрувати за TG? [y=лише з TG / n=лише без TG / Enter=будь-що]: ").strip().lower()
        if s == "y": return "y"
        if s == "n": return "n"
        return None

    def confirm(self, prompt:str) -> bool:
        s = input(f"{prompt} [y/N]: ").strip().lower()
        return s in ("y", "yes", "д", "так")

    # ===== Вибір рядка зі списку (для роботи без введення ID) =====
    def choose_from_rows(self, rows:list[dict], label_fields:list[str]):
        """
        Дає користувачу обрати один рядок зі списку.
        label_fields — список полів, які будуть показані як опис варіанту.
        Повертає обраний dict або None (якщо скасовано).
        """
        if not rows:
            print("(порожньо)")
            return None

        if len(rows) == 1:
            r = rows[0]
            labels = ", ".join(f"{k}={r.get(k)!r}" for k in label_fields if k in r)
            print(f"Знайдено єдиний варіант: {labels}")
            return r

        print("=== Вибір із списку ===")
        for idx, r in enumerate(rows, start=1):
            labels = ", ".join(f"{k}={r.get(k)!r}" for k in label_fields if k in r)
            print(f"{idx}) {labels}")

        idx = self.ask_int("Оберіть номер рядка (0 — скасувати): ", 0, len(rows))
        if idx == 0:
            print("Скасовано користувачем.")
            return None
        return rows[idx - 1]
