"""A small SQLAlchemy 2 app over OtterStax (examples/integration/CLAUDE.md, "SQLAlchemy 2").

Reads the quick start's four backends as ORM models, joins them in one statement, and writes to its
own table on a backend (`pgshop.shop.app_notes`, created by setup_backend.sql). The seed tables stay
read-only.

    python sqlalchemy_app.py [-v] [postgresql+psycopg2://demo:demo@127.0.0.1:8817/local]

Each step prints OK, FAIL, or REFUSED (a documented limitation, refused as the guide says).
"""

from __future__ import annotations

import sys
import traceback
from datetime import datetime

from sqlalchemy import DateTime, Float, ForeignKey, Integer, String, create_engine, func, inspect, select, text
from sqlalchemy.orm import DeclarativeBase, Mapped, Session, mapped_column, relationship

args = [a for a in sys.argv[1:] if a != "-v"]
URL = args[0] if args else "postgresql+psycopg2://demo:demo@127.0.0.1:8817/local"
results: list[tuple[str, str, str]] = []


def step(name, refused=None):
    """Run fn now. refused="<text>": the guide says the server refuses it with an error holding <text>."""
    def wrap(fn):
        try:
            note = fn()
            if refused:
                results.append((name, "FAIL", f"accepted; the guide says it is refused ({note!r})"))
            else:
                results.append((name, "OK", "" if note is None else str(note)))
        except Exception as e:  # report and go on: the point is to find every gap
            msg = f"{type(e).__name__}: {e}".splitlines()[0][:300]
            results.append((name, "REFUSED" if refused and refused in msg else "FAIL", msg))
            if "-v" in sys.argv:
                traceback.print_exc()
        return fn
    return wrap


class Base(DeclarativeBase):
    pass


class Customer(Base):  # PostgreSQL #1
    __tablename__ = "customers"
    __table_args__ = {"schema": "pgshop.shop"}
    customer_id: Mapped[str] = mapped_column(String, primary_key=True)
    name: Mapped[str] = mapped_column(String)
    country: Mapped[str] = mapped_column(String(2))
    tier: Mapped[str] = mapped_column(String)


class Product(Base):  # PostgreSQL #2
    __tablename__ = "products"
    __table_args__ = {"schema": "pgcat.catalog"}
    product_id: Mapped[str] = mapped_column(String, primary_key=True)
    name: Mapped[str] = mapped_column(String)
    category: Mapped[str] = mapped_column(String)
    price: Mapped[float] = mapped_column(Float)


class Order(Base):  # MariaDB
    __tablename__ = "orders"
    __table_args__ = {"schema": "sales.ops"}
    order_id: Mapped[str] = mapped_column(String(8), primary_key=True)
    customer_id: Mapped[str] = mapped_column(ForeignKey(Customer.customer_id))
    product_id: Mapped[str] = mapped_column(ForeignKey(Product.product_id))
    amount: Mapped[float] = mapped_column(Float)
    status: Mapped[str] = mapped_column(String(16))
    customer: Mapped[Customer] = relationship()
    product: Mapped[Product] = relationship()


class PageView(Base):  # ClickHouse (no primary key there; the ORM needs one, any unique-enough set)
    __tablename__ = "pageviews"
    __table_args__ = {"schema": "web.analytics"}
    customer_id: Mapped[str] = mapped_column(String, primary_key=True)
    product_id: Mapped[str] = mapped_column(String, primary_key=True)
    event_ts: Mapped[datetime] = mapped_column(DateTime, primary_key=True)
    channel: Mapped[str] = mapped_column(String)


class Note(Base):  # the app's own table on PostgreSQL #1 (setup_backend.sql)
    __tablename__ = "app_notes"
    __table_args__ = {"schema": "pgshop.shop"}
    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    customer_id: Mapped[str] = mapped_column(String)
    body: Mapped[str] = mapped_column(String)


engine = create_engine(URL, pool_pre_ping=True)
# Remote DDL runs only outside a transaction; psycopg2 opens one for every statement by default.
autocommit = engine.execution_options(isolation_level="AUTOCOMMIT")


@step("connect")
def _():
    with engine.connect() as c:
        return c.execute(text("SELECT version()")).scalar()


@step("setup: the app's table is reachable (first use outside a transaction)")
def _():
    # A table created on a backend after OtterStax started is registered on its first use; that use
    # fails inside a transaction, so touch it once in autocommit (docs/integration.md, section 7).
    with autocommit.connect() as c:
        c.execute(text("DELETE FROM pgshop.shop.app_notes"))  # a clean slate for this run


@step("orm: get by key (PostgreSQL)")
def _():
    with Session(engine) as s:
        return s.get(Customer, "C0001").name


@step("orm: filter + order + limit (PostgreSQL #2)")
def _():
    with Session(engine) as s:
        rows = s.scalars(select(Product).where(Product.price > 485).order_by(Product.price.desc()).limit(3)).all()
        return [p.product_id for p in rows]


@step("orm: aggregate (MariaDB)")
def _():
    with Session(engine) as s:
        return s.execute(select(Order.status, func.count()).group_by(Order.status).order_by(Order.status)).all()


@step("orm: federated join MariaDB x PostgreSQL x PostgreSQL")
def _():
    with Session(engine) as s:
        q = (select(Product.category, Customer.country, func.sum(Order.amount).label("revenue"))
             .join(Order.customer).join(Order.product)
             .where(Order.status == "paid")
             .group_by(Product.category, Customer.country)
             .order_by(func.sum(Order.amount).desc()).limit(3))
        return s.execute(q).all()


@step("orm: relationship lazy load across backends")
def _():
    with Session(engine) as s:
        o = s.get(Order, "O00001")
        return (o.customer.name, o.product.category)


@step("orm: 4-backend join with ClickHouse")
def _():
    with Session(engine) as s:
        q = (select(PageView.channel, func.count(func.distinct(Order.order_id)))
             .join(Order, (Order.customer_id == PageView.customer_id) & (Order.product_id == PageView.product_id))
             .join(Order.customer).join(Order.product)
             .group_by(PageView.channel).order_by(PageView.channel))
        return s.execute(q).all()


@step("orm: insert with generated id (RETURNING)")
def _():
    with Session(engine) as s:
        n = Note(customer_id="C0001", body="first")
        s.add(n)
        s.commit()
        if not n.id:
            raise AssertionError(f"no generated id: {n.id!r}")
        return n.id


@step("orm: update + delete in one unit of work")
def _():
    with Session(engine) as s:
        n = Note(customer_id="C0002", body="draft")
        s.add(n)
        s.flush()
        n.body = "final"
        s.flush()
        s.delete(n)
        s.commit()
        left = s.scalar(select(func.count()).select_from(Note).where(Note.customer_id == "C0002"))
        if left:
            raise AssertionError(f"{left} row(s) left after delete")


@step("tx: rollback")
def _():
    with Session(engine) as s:
        s.add(Note(customer_id="C0003", body="gone"))
        s.flush()
        s.rollback()
        left = s.scalar(select(func.count()).select_from(Note).where(Note.body == "gone"))
        if left:
            raise AssertionError(f"{left} row(s) survived the rollback")


@step("tx: read two backends + write one, then commit")
def _():
    with Session(engine) as s:
        c = s.get(Customer, "C0004")
        total = s.scalar(select(func.sum(Order.amount)).where(Order.customer_id == c.customer_id))
        s.add(Note(customer_id=c.customer_id, body=f"total {total}"))
        s.commit()
        return total


@step("tx: write two backends in one transaction", refused="writing to connection")
def _():
    with Session(engine) as s:
        s.add(Note(customer_id="C0005", body="pg side"))
        s.flush()
        s.execute(text("UPDATE sales.ops.orders SET status = status WHERE order_id = 'O00001'"))
        s.commit()


@step("ddl: create_all() of a table with a primary key on a backend", refused="keys, checks and defaults")
def _():
    class Base2(DeclarativeBase):
        pass

    class Scratch(Base2):
        __tablename__ = "app_scratch"
        __table_args__ = {"schema": "pgshop.shop"}
        id: Mapped[int] = mapped_column(Integer, primary_key=True)

    Base2.metadata.create_all(autocommit)


@step("ddl: plain CREATE / DROP TABLE on a backend in autocommit")
def _():
    with autocommit.connect() as c:
        c.execute(text("CREATE TABLE pgshop.shop.app_scratch (id integer, v text)"))
        c.execute(text("DROP TABLE pgshop.shop.app_scratch"))


@step("ddl: DROP TABLE inside a transaction", refused="only supports DDL in autocommit")
def _():
    with autocommit.connect() as c:
        c.execute(text("CREATE TABLE pgshop.shop.app_scratch (id integer)"))
    try:
        with engine.begin() as c:
            c.execute(text("DROP TABLE pgshop.shop.app_scratch"))
    finally:
        with autocommit.connect() as c:
            c.execute(text("DROP TABLE IF EXISTS pgshop.shop.app_scratch"))


@step("reflect: inspect().has_table() of an existing table")
def _():
    found = inspect(engine).has_table("customers", schema="pgshop.shop")
    if not found:
        raise AssertionError("has_table returned False for pgshop.shop.customers")


@step("reflect: inspect().get_table_names()")
def _():
    return inspect(engine).get_table_names(schema="pgshop.shop")


@step("reflect: inspect().get_columns()")
def _():
    return [c["name"] for c in inspect(engine).get_columns("customers", schema="pgshop.shop")]


if __name__ == "__main__":
    print(f"URL: {URL}")
    w = max(len(n) for n, _, _ in results)
    for name, status, note in results:
        print(f"{status:<8} {name:<{w}}  {note}")
    sys.exit(0 if all(s != "FAIL" for _, s, _ in results) else 1)
