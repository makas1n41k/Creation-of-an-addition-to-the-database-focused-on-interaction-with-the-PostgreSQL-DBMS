import os
try:
    from dotenv import load_dotenv
except ModuleNotFoundError:
    def load_dotenv(*args, **kwargs): return False  

from model import Model
from controller import Controller
from view import View


def build_dsn() -> str:
    dsn = os.getenv("DATABASE_URL")
    if not dsn:
        raise RuntimeError("ENV DATABASE_URL не задано. Створи .env або задай змінну середовища")
    return dsn


if __name__ == "__main__":
    load_dotenv()
    dsn = build_dsn()

    model = Model(dsn)
    view = View()

    if not model.ping():
        view.err("Нема підключення до БД. Перевір .env і доступність PostgreSQL.")
        raise SystemExit(1)

    Controller(model, view).run()
