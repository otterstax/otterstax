# CLAUDE.md — examples/integration

Framework code for integrating an application with OtterStax, and the runnable examples behind it.
The general approach — wires, names, the place, schema ownership, transactions, limitations — is
`docs/integration.md`; this file is the code and what each framework does with it. Section numbers
below (§) are that guide's.

Verified on 2026-11-27 against a Linux image of `main` at `961fa44`, with Django 6.1, SQLAlchemy 2.1,
psycopg2 2.9, psycopg 3.3 — by `examples/integration/python_orm/` below.

## SQLAlchemy 2

```python
from sqlalchemy import create_engine, select, Integer, String, Numeric, ForeignKey, text
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column, relationship, Session

engine = create_engine(
    "postgresql+psycopg2://app:secret@otterstax:8817/local",   # psycopg2: psycopg 3 fails at connect
    pool_pre_ping=True,
)
autocommit = engine.execution_options(isolation_level="AUTOCOMMIT")   # DDL, first use of a new table

class Base(DeclarativeBase):
    pass

class Customer(Base):                                   # a PostgreSQL table
    __tablename__ = "customers"
    __table_args__ = {"schema": "pg.public"}            # alias.namespace in the schema slot (§6)
    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    name: Mapped[str] = mapped_column(String(100))

class Invoice(Base):                                    # a MariaDB table
    __tablename__ = "invoices"
    __table_args__ = {"schema": "maria.billing"}
    id: Mapped[int] = mapped_column(Integer, primary_key=True)   # the backend's AUTO_INCREMENT
    customer_id: Mapped[int] = mapped_column(ForeignKey(Customer.id))
    amount: Mapped[float] = mapped_column(Numeric(10, 2))
    customer: Mapped[Customer] = relationship()

with Session(engine) as s:                              # one statement, two databases
    rows = s.execute(select(Customer.name, Invoice.amount)
                     .join(Invoice.customer).where(Invoice.amount > 100)).all()

with autocommit.connect() as c:                         # DDL of a backend table: autocommit only
    c.execute(text("CREATE TABLE pg.public.scratch (id integer, v text)"))   # no keys / defaults
```

| Works | Does not |
|-------|----------|
| `s.get()`, `select()` with WHERE / ORDER BY / LIMIT, GROUP BY, JOINs over 3 and 4 backends, relationships lazy-loaded across backends | `postgresql+psycopg` (psycopg 3): `no cast from oid to regtype` at connect |
| `add()` + `commit()` with the backend's id (`RETURNING`), `flush()`, update + delete in one unit of work, `rollback()` | `inspect().has_table()`: **`False` for every table**, no error — `checkfirst` decides wrongly |
| a transaction that reads two backends and writes one | `inspect().get_table_names()` (`pg_class.relpersistence`), `get_columns()` / `autoload_with` (`pg_attribute.atthasdef`) |
| plain `CREATE` / `DROP TABLE` of a backend table through `autocommit` | `metadata.create_all()` of a backend table: `PRIMARY KEY`, `SERIAL` refused; through `engine` also refused for running in a transaction |

- `ForeignKey` / `relationship()` across backends: the ORM's JOIN paths work; nothing enforces it.
- Alembic: not run. Its `alembic_version` table has a key, its migrations add keys and defaults, and
  it runs PostgreSQL migrations in a transaction — each refused on a backend (§7). Point Alembic at
  the backend itself.

## Django

Django's own tables (`django_migrations`, auth, contenttypes, sessions) and its managed models go in
an ordinary database; OtterStax is a second database, reached through a router, for the models over
backend tables (§8). `migrate` on OtterStax fails at Django's first table (`django_migrations`:
`syntax error at or near "GENERATED"`).

```python
# settings.py
DATABASES = {
    "default": {                                        # Django's own tables; any database Django supports
        "ENGINE": "django.db.backends.postgresql",
        "NAME": "app", "USER": "app", "PASSWORD": "secret", "HOST": "app-db", "PORT": "5432",
    },
    "otterstax": {
        "ENGINE": "django.db.backends.postgresql",     # psycopg 3 works here
        "NAME": "local",                                # §5
        "USER": "app", "PASSWORD": "secret", "HOST": "otterstax", "PORT": "8817",
    },
}
DATABASE_ROUTERS = ["myapp.routers.OtterStaxRouter"]
```

```python
# myapp/routers.py
class OtterStaxRouter:
    def _db(self, model):
        return "otterstax" if getattr(model, "otterstax", False) else None
    def db_for_read(self, model, **hints):
        return self._db(model)
    def db_for_write(self, model, **hints):
        return self._db(model)
    def allow_relation(self, a, b, **hints):
        return getattr(a, "otterstax", False) == getattr(b, "otterstax", False)
    def allow_migrate(self, db, app_label, model_name=None, **hints):
        return db != "otterstax"                        # the backends own their schemas
```

```python
# myapp/models.py
from django.db import models

class Customer(models.Model):
    otterstax = True                                    # routed to "otterstax"
    id = models.IntegerField(primary_key=True)
    name = models.CharField(max_length=100)
    class Meta:
        managed = False                                 # never created / migrated by Django
        db_table = '"pg"."public"."customers"'          # no schema slot: each part quoted (§6)

class Invoice(models.Model):
    otterstax = True
    id = models.AutoField(primary_key=True)             # the backend's AUTO_INCREMENT, via RETURNING
    customer = models.ForeignKey(Customer, on_delete=models.DO_NOTHING,
                                 db_constraint=False, db_column="customer_id")
    amount = models.DecimalField(max_digits=10, decimal_places=2)
    class Meta:
        managed = False
        db_table = '"maria"."billing"."invoices"'

# Invoice.objects.filter(amount__gt=100).select_related("customer")  — one federated JOIN
```

| Works | Does not |
|-------|----------|
| `get`, `filter` / slicing, `values().annotate()`, `select_related` across 3 backends, GROUP BY through relations | `migrate --database otterstax`: Django creates `django_migrations` on the target whatever the router says |
| `create()` with the backend's `AutoField` id, `update()`, `delete()` | `inspectdb --database otterstax`: every table fails (`pg_constraint.conkey[1]`) |
| `transaction.atomic(using="otterstax")`, rollback, nested savepoints, read MariaDB + write PostgreSQL in one `atomic()` | relations between a `default` model and an OtterStax model (two databases): query one side, then `filter(x__in=ids)` |
| `introspection.table_names()`: the place's tables — none with `NAME = "local"`, the connection's with `NAME = "pg"` | writing two backends in one `atomic()` (§9) |

- `ATOMIC_REQUESTS`: set it on the `otterstax` entry, and keep a request to one written backend (§9).
- Outside `atomic()` Django runs in autocommit: a new backend table's first use (§7) and DDL go there.

## `python_orm` — the runnable example

In `examples/integration/python_orm/`.

A SQLAlchemy script and a Django app over the quick start's backends: `pgshop` and `pgcat`
(PostgreSQL), `sales` (MariaDB), `web` (ClickHouse). Each step prints `OK`, `FAIL`, or `REFUSED` (a
documented limitation, refused with the error the guide names).

| File | What it is |
|------|------------|
| `examples/integration/python_orm/config.host.yaml` | the server's config: OtterStax on the host, the backends on their published ports (3310–3313) |
| `examples/integration/python_orm/config.docker.yaml` | the same for OtterStax in Docker on the quick start's network (service names, container ports) |
| `examples/integration/python_orm/setup_backend.sql` | the app's own table `shop.app_notes` (serial key), created on the `pgshop` backend itself |
| `examples/integration/python_orm/requirements.txt` | SQLAlchemy, psycopg2, Django, psycopg 3 |
| `examples/integration/python_orm/sqlalchemy_app.py` | 19 steps: the SQLAlchemy section above |
| `examples/integration/python_orm/django_app/` | project `shopreport` (SQLite `default`, OtterStax `otterstax`), app `report` (models, `examples/integration/python_orm/django_app/report/routers.py`), `python manage.py otx_check`: 15 steps |

Run it, from the repository root:

```bash
# 1. the backends, and the app's table on pgshop
(cd examples/quick_start && docker compose up -d --wait --no-deps \
    qs-postgres-shop qs-postgres-catalog qs-mariadb qs-clickhouse)
docker exec -i qs-postgres-shop psql -U demo shop < examples/integration/python_orm/setup_backend.sql

# 2. OtterStax in Docker, on the quick start's network ...
docker run -d --name otx-orm --network quick_start_quickstart_net -p 8817:8817 \
    -v $PWD/examples/integration/python_orm/config.docker.yaml:/otterstax/config.yaml:ro otterstax:local
#    ... or on the host, a build of this checkout
build/Release/server --config examples/integration/python_orm/config.host.yaml

# 3. the apps
cd examples/integration/python_orm
python3 -m venv .venv && .venv/bin/pip install -r requirements.txt
.venv/bin/python sqlalchemy_app.py                        # -v prints each failure's traceback
cd django_app
../.venv/bin/python manage.py migrate                     # "default" (SQLite) only
../.venv/bin/python manage.py otx_check                   # --trace prints tracebacks
```

The 2026-11-27 run: SQLAlchemy 13 OK, 3 REFUSED, 3 FAIL (the three reflection calls); Django 14 OK,
1 REFUSED. The same on a freshly started server. A server image built before #58 (2026-11) fails
every step: no three-part names, no `version()`.

## Invariants — keep them when editing

- **The quick start's seed tables stay read-only.** An example writes only its own tables
  (`pgshop.shop.app_notes`, emptied at the start of each run; `pgshop.shop.app_scratch`, created and
  dropped) and its own files (the Django app's git-ignored SQLite database, django_state.sqlite3). A step that must name a seed table in
  a write (the two-backend refusals) writes a no-op (`SET status = status`).
- **A step's status says what the guide says.** `OK` for what the guide calls working, `REFUSED`
  (with the expected error text) for a documented refusal, `FAIL` for anything else — never weaken a
  check to turn a `FAIL` into `OK`.
- **A table an app writes through an ORM is created on the backend** (`examples/integration/python_orm/setup_backend.sql`), not
  through OtterStax, and is first used in autocommit (§7).
- **A new example** — another framework or language — goes in its own directory here, runs against
  the same quick start backends, follows the same three statuses, and gets a section in this file.

## When the server changes

A change to what a client driver sees — the wire frontends, pg_catalog's shape and the catalog
functions, name resolution, transactions, generated ids, types — can move a step. Re-run both apps
against a build of the change, update the tables above and the run line, and the markers and
§12 / §13 of `docs/integration.md`. raft-board covers the wider driver × ORM × wire matrix.
