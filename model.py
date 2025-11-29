# model.py / modul.py

import decimal
import psycopg
from psycopg.rows import dict_row

D = decimal.Decimal
KYIV_TZ = "Europe/Kiev"


class Model:
    def __init__(self, dsn: str):
        self._dsn = dsn

    def _conn(self):
        return psycopg.connect(self._dsn, row_factory=dict_row)

    @staticmethod
    def _ts(col: str, alias: str) -> str:
        return (
            f"to_char({col} AT TIME ZONE '{KYIV_TZ}', "
            f"'YYYY-MM-DD HH24:MI:SS') AS {alias}"
        )

    # ---------- Infra ----------

    def ping(self) -> bool:
        try:
            with self._conn() as conn, conn.cursor() as cur:
                cur.execute("SELECT 1;")
                cur.fetchone()
            return True
        except psycopg.Error:
            return False

    # ---------- Users ----------

    def users_list(self, limit=50, offset=0):
        sql = f"""
        SELECT user_id,
               full_name,
               username,
               tg_handle,
               {self._ts("created_at", "created_at")}
        FROM public."user"
        ORDER BY user_id
        LIMIT %s OFFSET %s;
        """
        with self._conn() as c, c.cursor() as cur:
            cur.execute(sql, (limit, offset))
            return cur.fetchall()

    def users_get(self, user_id: int):
        sql = f"""
        SELECT user_id,
               full_name,
               username,
               tg_handle,
               {self._ts("created_at", "created_at")}
        FROM public."user"
        WHERE user_id=%s;
        """
        with self._conn() as c, c.cursor() as cur:
            cur.execute(sql, (user_id,))
            return cur.fetchone()

    def users_search_simple(self, full_like: str | None, username_like: str | None,
                            limit=50, offset=0):
        """Пошук користувачів для інтерактивного вибору (без введення ID)."""
        where = []
        params: list = []
        if full_like:
            where.append("full_name ILIKE %s")
            params.append(full_like)
        if username_like:
            where.append("username ILIKE %s")
            params.append(username_like)
        where_sql = "WHERE " + " AND ".join(where) if where else ""
        sql = f"""
        SELECT user_id,
               full_name,
               username,
               tg_handle,
               {self._ts("created_at", "created_at")}
        FROM public."user"
        {where_sql}
        ORDER BY LOWER(username)
        LIMIT %s OFFSET %s;
        """
        params.extend([limit, offset])
        with self._conn() as c, c.cursor() as cur:
            cur.execute(sql, tuple(params))
            return cur.fetchall()

    def users_create(self, full_name: str, username: str, tg_handle: str | None) -> int:
        with self._conn() as c, c.cursor() as cur:
            cur.execute(
                """
                INSERT INTO public."user"(full_name, username, tg_handle)
                VALUES (%s,%s,%s)
                RETURNING user_id;
                """,
                (full_name, username, tg_handle),
            )
            new_id = cur.fetchone()["user_id"]
            c.commit()
            return new_id

    def users_update(self, user_id: int, full_name: str, username: str, tg_handle: str | None) -> int:
        with self._conn() as c, c.cursor() as cur:
            cur.execute(
                """
                UPDATE public."user"
                SET full_name=%s, username=%s, tg_handle=%s
                WHERE user_id=%s;
                """,
                (full_name, username, tg_handle, user_id),
            )
            c.commit()
            return cur.rowcount

    def users_delete(self, user_id: int) -> int:
        with self._conn() as c, c.cursor() as cur:
            cur.execute('DELETE FROM public."user" WHERE user_id=%s;', (user_id,))
            c.commit()
            return cur.rowcount

    def count_activity_by_user(self, user_id: int) -> int:
        return self._single_count("public.activity", "user_id=%s", (user_id,))

    def count_impressions_by_user(self, user_id: int) -> int:
        return self._single_count("public.book_impressions", "user_id=%s", (user_id,))

    # ---------- Books ----------

    def books_list(self, limit=50, offset=0):
        sql = f"""
        SELECT book_id,
               title,
               author,
               genre,
               {self._ts("created_at", "created_at")}
        FROM public.books
        ORDER BY book_id
        LIMIT %s OFFSET %s;
        """
        with self._conn() as c, c.cursor() as cur:
            cur.execute(sql, (limit, offset))
            return cur.fetchall()

    def books_get(self, book_id: int):
        sql = f"""
        SELECT book_id,
               title,
               author,
               genre,
               {self._ts("created_at", "created_at")}
        FROM public.books
        WHERE book_id=%s;
        """
        with self._conn() as c, c.cursor() as cur:
            cur.execute(sql, (book_id,))
            return cur.fetchone()

    def books_search_simple(self,
                            title_like: str | None,
                            author_like: str | None,
                            genre_like: str | None,
                            limit=50, offset=0):
        """Пошук книг для інтерактивного вибору (без введення ID)."""
        where = []
        params: list = []
        if title_like:
            where.append("title ILIKE %s")
            params.append(title_like)
        if author_like:
            where.append("author ILIKE %s")
            params.append(author_like)
        if genre_like:
            where.append("genre ILIKE %s")
            params.append(genre_like)
        where_sql = "WHERE " + " AND ".join(where) if where else ""
        sql = f"""
        SELECT book_id,
               title,
               author,
               genre,
               {self._ts("created_at", "created_at")}
        FROM public.books
        {where_sql}
        ORDER BY LOWER(title)
        LIMIT %s OFFSET %s;
        """
        params.extend([limit, offset])
        with self._conn() as c, c.cursor() as cur:
            cur.execute(sql, tuple(params))
            return cur.fetchall()

    def books_create(self, title: str, author: str, genre: str) -> int:
        with self._conn() as c, c.cursor() as cur:
            cur.execute(
                """
                INSERT INTO public.books(title, author, genre)
                VALUES (%s,%s,%s)
                RETURNING book_id;
                """,
                (title, author, genre),
            )
            new_id = cur.fetchone()["book_id"]
            c.commit()
            return new_id

    def books_update(self, book_id: int, title: str, author: str, genre: str) -> int:
        with self._conn() as c, c.cursor() as cur:
            cur.execute(
                """
                UPDATE public.books
                SET title=%s, author=%s, genre=%s
                WHERE book_id=%s;
                """,
                (title, author, genre, book_id),
            )
            c.commit()
            return cur.rowcount

    def books_delete(self, book_id: int) -> int:
        with self._conn() as c, c.cursor() as cur:
            cur.execute("DELETE FROM public.books WHERE book_id=%s;", (book_id,))
            c.commit()
            return cur.rowcount

    def count_activity_by_book(self, book_id: int) -> int:
        return self._single_count("public.activity", "book_id=%s", (book_id,))

    def count_impressions_by_book(self, book_id: int) -> int:
        return self._single_count("public.book_impressions", "book_id=%s", (book_id,))

    # ---------- Activity (без viewed_at) ----------

    def activity_list(self, limit=50, offset=0):
        sql = """
        SELECT a.user_id,
               u.username,
               u.full_name,
               a.book_id,
               b.title,
               b.author,
               b.genre
        FROM public.activity a
        JOIN public."user" u ON u.user_id = a.user_id
        JOIN public.books  b ON b.book_id = a.book_id
        ORDER BY a.user_id, a.book_id
        LIMIT %s OFFSET %s;
        """
        with self._conn() as c, c.cursor() as cur:
            cur.execute(sql, (limit, offset))
            return cur.fetchall()

    def activity_for_user(self, user_id: int, limit=50, offset=0):
        """Activity для конкретного користувача (для інтерактивного вибору книги)."""
        sql = """
        SELECT a.user_id,
               u.username,
               u.full_name,
               a.book_id,
               b.title,
               b.author,
               b.genre
        FROM public.activity a
        JOIN public."user" u ON u.user_id = a.user_id
        JOIN public.books  b ON b.book_id = a.book_id
        WHERE a.user_id = %s
        ORDER BY b.title, b.author
        LIMIT %s OFFSET %s;
        """
        with self._conn() as c, c.cursor() as cur:
            cur.execute(sql, (user_id, limit, offset))
            return cur.fetchall()

    def activity_exists(self, user_id: int, book_id: int) -> bool:
        with self._conn() as c, c.cursor() as cur:
            cur.execute(
                "SELECT 1 FROM public.activity WHERE user_id=%s AND book_id=%s;",
                (user_id, book_id),
            )
            return cur.fetchone() is not None

    def activity_create(self, user_id: int, book_id: int) -> int:
        with self._conn() as c, c.cursor() as cur:
            cur.execute(
                """
                INSERT INTO public.activity(user_id, book_id)
                VALUES (%s,%s)
                ON CONFLICT DO NOTHING;
                """,
                (user_id, book_id),
            )
            c.commit()
            return cur.rowcount

    def activity_delete(self, user_id: int, book_id: int) -> int:
        with self._conn() as c, c.cursor() as cur:
            cur.execute(
                "DELETE FROM public.activity WHERE user_id=%s AND book_id=%s;",
                (user_id, book_id),
            )
            c.commit()
            return cur.rowcount

    def count_impressions_for_pair(self, user_id: int, book_id: int) -> int:
        return self._single_count(
            "public.book_impressions", "user_id=%s AND book_id=%s", (user_id, book_id)
        )

    # ---------- Book_Impressions ----------

    def impressions_list(self, limit=50, offset=0):
        sql = f"""
        SELECT i.rating_id,
               i.user_id,
               u.username,
               i.book_id,
               b.title,
               i.rating,
               i.comment,
               {self._ts("i.created_at", "created_at")}
        FROM public.book_impressions i
        JOIN public."user" u ON u.user_id = i.user_id
        JOIN public.books  b ON b.book_id = i.book_id
        ORDER BY i.created_at DESC
        LIMIT %s OFFSET %s;
        """
        with self._conn() as c, c.cursor() as cur:
            cur.execute(sql, (limit, offset))
            return cur.fetchall()

    def impressions_for_user(self, user_id: int, limit=50, offset=0):
        """Список відгуків (book_impressions) для конкретного користувача."""
        sql = f"""
        SELECT i.rating_id,
               i.user_id,
               u.username,
               i.book_id,
               b.title,
               i.rating,
               i.comment,
               {self._ts("i.created_at", "created_at")}
        FROM public.book_impressions i
        JOIN public."user" u ON u.user_id = i.user_id
        JOIN public.books  b ON b.book_id = i.book_id
        WHERE i.user_id = %s
        ORDER BY i.created_at DESC, b.title
        LIMIT %s OFFSET %s;
        """
        with self._conn() as c, c.cursor() as cur:
            cur.execute(sql, (user_id, limit, offset))
            return cur.fetchall()

    def impressions_get(self, rating_id: int):
        sql = f"""
        SELECT rating_id,
               user_id,
               book_id,
               rating,
               comment,
               {self._ts("created_at", "created_at")}
        FROM public.book_impressions
        WHERE rating_id=%s;
        """
        with self._conn() as c, c.cursor() as cur:
            cur.execute(sql, (rating_id,))
            return cur.fetchone()

    def impressions_create(self, user_id: int, book_id: int, rating: float, comment: str | None) -> int:
        val = D(str(rating)).quantize(D("0.1"))
        with self._conn() as c, c.cursor() as cur:
            cur.execute(
                """
                INSERT INTO public.book_impressions(user_id, book_id, rating, comment)
                VALUES (%s,%s,%s,%s)
                RETURNING rating_id;
                """,
                (user_id, book_id, val, comment),
            )
            rid = cur.fetchone()["rating_id"]
            c.commit()
            return rid

    def impressions_update(self, rating_id: int, rating: float, comment: str | None) -> int:
        val = D(str(rating)).quantize(D("0.1"))
        with self._conn() as c, c.cursor() as cur:
            cur.execute(
                """
                UPDATE public.book_impressions
                SET rating=%s, comment=%s
                WHERE rating_id=%s;
                """,
                (val, comment, rating_id),
            )
            c.commit()
            return cur.rowcount

    def impressions_delete(self, rating_id: int) -> int:
        with self._conn() as c, c.cursor() as cur:
            cur.execute("DELETE FROM public.book_impressions WHERE rating_id=%s;", (rating_id,))
            c.commit()
            return cur.rowcount

    # ---------- Generation (SQL only) ----------

    def generate_users(self, n: int) -> int:
        sql = """
        WITH base AS (
          SELECT COALESCE(MAX(user_id), 0) AS u_base
          FROM public."user"
        ),
        gs AS (
          SELECT generate_series(1, %s) AS i
        )
        INSERT INTO public."user"(full_name, username, tg_handle, created_at)
        SELECT
          'User№'     || (b.u_base + gs.i)::text  AS full_name,
          'username№' || (b.u_base + gs.i)::text  AS username,
          CASE WHEN random() < 0.7
               THEN '@tg_handle№' || (b.u_base + gs.i)::text
               ELSE NULL
          END                                     AS tg_handle,
          NOW() - (random() * interval '365 days') AS created_at
        FROM gs
        CROSS JOIN base b;
        """
        with self._conn() as c, c.cursor() as cur:
            cur.execute(sql, (n,))
            c.commit()
            return cur.rowcount

    def generate_books(self, n: int) -> int:
        sql = """
        WITH base AS (
            SELECT COALESCE(MAX(book_id), 0) AS book_base
            FROM public.books
        ),
        params AS (
            SELECT
                book_base,
                ARRAY[
                    'Silent','Broken','Hidden','Lost','Bright',
                    'Dark','Red','Golden','Old','New'
                ] AS adjectives,
                ARRAY[
                    'City','Forest','World','Dream','River',
                    'House','Secret','Story','Road','Garden'
                ] AS nouns,
                ARRAY[
                    'Alan','Mira','John','Sara','Leo',
                    'Nina','Victor','Lena','Owen','Ira'
                ] AS author_first,
                ARRAY[
                    'Smith','Brown','Johnson','Miller','Davis',
                    'Clark','Moore','Taylor','Wilson','King'
                ] AS author_last,
                ARRAY[
                    'fantasy','sci-fi','mystery','non-fiction',
                    'romance','thriller'
                ] AS genres
            FROM base
        ),
        gs AS (
            SELECT generate_series(1, %s) AS i
        ),
        calc AS (
            SELECT
                gs.i,
                p.book_base,
                p.adjectives[
                    1 + ((gs.i - 1) %% array_length(p.adjectives, 1))
                ] AS adj,
                p.nouns[
                    1 + ((gs.i - 1) %% array_length(p.nouns, 1))
                ] AS noun,
                p.author_first[
                    1 + ((gs.i - 1) %% array_length(p.author_first, 1))
                ] AS af,
                p.author_last[
                    1 + (((gs.i - 1) * 3) %% array_length(p.author_last, 1))
                ] AS al,
                p.genres[
                    1 + ((gs.i - 1) %% array_length(p.genres, 1))
                ] AS genre
            FROM gs
            CROSS JOIN params p
        )
        INSERT INTO public.books(title, author, genre, created_at)
        SELECT
            'Book ' || adj || ' ' || noun || ' #' || (book_base + i)::text AS title,
            af || ' ' || al                                                 AS author,
            genre                                                           AS genre,
            NOW() - (random() * interval '365 days')                        AS created_at
        FROM calc;
        """
        with self._conn() as c, c.cursor() as cur:
            cur.execute(sql, (n,))
            c.commit()
            return cur.rowcount

    def generate_activity(self, n: int) -> int:
        sql_count = """
        WITH all_pairs AS (
          SELECT u.user_id, b.book_id
          FROM public."user" u
          CROSS JOIN public.books b
        ),
        missing AS (
          SELECT a.user_id, a.book_id
          FROM all_pairs a
          EXCEPT
          SELECT user_id, book_id FROM public.activity
        )
        SELECT COUNT(*) AS cnt FROM missing;
        """
        sql_insert = """
        WITH all_pairs AS (
          SELECT u.user_id, b.book_id
          FROM public."user" u
          CROSS JOIN public.books b
        ),
        missing AS (
          SELECT a.user_id, a.book_id
          FROM all_pairs a
          EXCEPT
          SELECT user_id, book_id FROM public.activity
        ),
        pick AS (
          SELECT user_id, book_id
          FROM missing
          ORDER BY random()
          LIMIT %s
        )
        INSERT INTO public.activity(user_id, book_id)
        SELECT user_id, book_id
        FROM pick;
        """
        with self._conn() as c, c.cursor() as cur:
            cur.execute(sql_count)
            available = cur.fetchone()["cnt"]
            if available < n:
                raise ValueError(
                    f"Недостатньо вільних пар user×book для {n} записів (є {available})."
                )
            cur.execute(sql_insert, (n,))
            c.commit()
            return cur.rowcount

    def generate_impressions(self, n: int) -> int:
        sql = """
        WITH picked AS (
            SELECT user_id, book_id
            FROM public.activity
            ORDER BY random()
            LIMIT %s
        )
        INSERT INTO public.book_impressions(user_id, book_id, rating, comment, created_at)
        SELECT
            p.user_id,
            p.book_id,
            ROUND(GREATEST(1.0, LEAST(5.0, 1.0 + random() * 4.0))::numeric, 1) AS rating,
            CASE WHEN random() < 0.5 THEN 'Nice' ELSE 'OK' END AS comment,
            NOW() - (random() * interval '365 days') AS created_at
        FROM picked p;
        """
        with self._conn() as c, c.cursor() as cur:
            cur.execute(sql, (n,))
            c.commit()
            return cur.rowcount

    # ---------- Searches ----------

    def search_multientity(
        self,
        title_like: str | None,
        author_like: str | None,
        genre_like: str | None,
        rating_min: float | None,
        rating_max: float | None,
        date_from: str | None,
        date_to: str | None,
        has_tg: str | None,
    ):
        where = []
        params: list = []

        if title_like:
            where.append("b.title ILIKE %s")
            params.append(title_like)
        if author_like:
            where.append("b.author ILIKE %s")
            params.append(author_like)
        if genre_like:
            where.append("b.genre ILIKE %s")
            params.append(genre_like)

        if rating_min is not None and rating_max is not None and rating_min > rating_max:
            rating_min, rating_max = rating_max, rating_min
        if rating_min is not None:
            where.append("i.rating >= %s")
            params.append(D(str(rating_min)))
        if rating_max is not None:
            where.append("i.rating <= %s")
            params.append(D(str(rating_max)))

        if date_from and date_to:
            where.append("i.created_at BETWEEN %s AND %s")
            params += [date_from, date_to]
        elif date_from:
            where.append("i.created_at >= %s")
            params.append(date_from)
        elif date_to:
            where.append("i.created_at <= %s")
            params.append(date_to)

        if has_tg == "y":
            where.append("u.tg_handle IS NOT NULL")
        elif has_tg == "n":
            where.append("u.tg_handle IS NULL")

        where_sql = "WHERE " + " AND ".join(where) if where else ""
        sql = f"""
        SELECT u.user_id,
               u.username,
               b.book_id,
               b.title,
               b.author,
               b.genre,
               i.rating,
               i.comment,
               {self._ts("i.created_at", "created_at")}
        FROM public.book_impressions i
        JOIN public."user" u ON u.user_id = i.user_id
        JOIN public.books  b ON b.book_id = i.book_id
        {where_sql}
        ORDER BY i.created_at DESC, LOWER(u.username), b.book_id;
        """
        with self._conn() as c, c.cursor() as cur:
            cur.execute(sql, tuple(params))
            return cur.fetchall()

    def search_aggregate_ratings(
        self,
        date_from: str | None,
        date_to: str | None,
        min_count: int,
        group_by: str,
    ):
        if group_by not in ("author", "genre"):
            group_by = "author"

        where = []
        params: list = []

        if date_from and date_to:
            where.append("i.created_at BETWEEN %s AND %s")
            params += [date_from, date_to]
        elif date_from:
            where.append("i.created_at >= %s")
            params.append(date_from)
        elif date_to:
            where.append("i.created_at <= %s")
            params.append(date_to)

        where_sql = "WHERE " + " AND ".join(where) if where else ""
        sql = f"""
        SELECT b.{group_by} AS grp,
               COUNT(*) AS cnt,
               ROUND(AVG(i.rating)::numeric, 2) AS avg_rating
        FROM public.book_impressions i
        JOIN public.books b ON b.book_id = i.book_id
        {where_sql}
        GROUP BY b.{group_by}
        HAVING COUNT(*) >= %s
        ORDER BY avg_rating DESC, cnt DESC;
        """
        params.append(min_count)

        with self._conn() as c, c.cursor() as cur:
            cur.execute(sql, tuple(params))
            return cur.fetchall()

    def search_users_no_tg_by_genre(
        self,
        genre_like: str | None,
        date_from: str | None,
        date_to: str | None,
    ):
        where = ["u.tg_handle IS NULL"]
        params: list = []

        if genre_like:
            where.append("b.genre ILIKE %s")
            params.append(genre_like)

        if date_from and date_to:
            where.append("i.created_at BETWEEN %s AND %s")
            params += [date_from, date_to]
        elif date_from:
            where.append("i.created_at >= %s")
            params.append(date_from)
        elif date_to:
            where.append("i.created_at <= %s")
            params.append(date_to)

        sql = f"""
        SELECT DISTINCT u.user_id,
                        u.username,
                        u.full_name
        FROM public.activity a
        JOIN public."user" u ON u.user_id = a.user_id
        JOIN public.books  b ON b.book_id = a.book_id
        JOIN public.book_impressions i
        ON i.user_id = a.user_id AND i.book_id = a.book_id
        WHERE {" AND ".join(where)}
        ORDER BY u.username;
        """
        with self._conn() as c, c.cursor() as cur:
            cur.execute(sql, tuple(params))
            return cur.fetchall()

    # ---------- Helper ----------

    def _single_count(self, table: str, where: str, params: tuple) -> int:
        sql = f"SELECT COUNT(*) AS cnt FROM {table} WHERE {where};"
        with self._conn() as c, c.cursor() as cur:
            cur.execute(sql, params)
            return cur.fetchone()["cnt"]
