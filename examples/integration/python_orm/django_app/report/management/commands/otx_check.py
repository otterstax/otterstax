"""python manage.py otx_check — the ORM paths a Django app uses, each reported OK / FAIL / REFUSED.

REFUSED: a documented limitation, refused as docs/integration.md says.
"""

import traceback

from django.core.management.base import BaseCommand
from django.db import connections, transaction
from django.db.models import Count, F, Sum

from report.models import BackendNote, Bookmark, Customer, Order, Product

OTX = "otterstax"


class Command(BaseCommand):
    def add_arguments(self, parser):
        parser.add_argument("--trace", action="store_true")

    def handle(self, *args, trace=False, **kw):
        results = []
        otx = connections[OTX]

        def step(name, fn, refused=None):
            try:
                note = fn()
                if refused:
                    results.append((name, "FAIL", f"accepted; the guide says it is refused ({note!r})"))
                else:
                    results.append((name, "OK", "" if note is None else str(note)))
            except Exception as e:
                msg = f"{type(e).__name__}: {e}".splitlines()[0][:300]
                results.append((name, "REFUSED" if refused and refused in msg else "FAIL", msg))
                if trace:
                    traceback.print_exc()

        def version():
            with otx.cursor() as c:
                c.execute("SELECT version()")
                return c.fetchone()[0]

        def warm_up():
            # first use of a table created on a backend after OtterStax started: outside a transaction
            with otx.cursor() as c:  # Django runs in autocommit outside atomic()
                c.execute('DELETE FROM "pgshop"."shop"."app_notes"')

        step("connect + version()", version)
        step("setup: the app's backend table is reachable", warm_up)
        step("get by key (PostgreSQL)", lambda: Customer.objects.get(pk="C0001").name)
        step("filter + order + slice (PostgreSQL #2)", lambda: list(
            Product.objects.filter(price__gt=485).order_by("-price").values_list("product_id", flat=True)[:3]))
        step("aggregate (MariaDB)", lambda: list(
            Order.objects.values("status").annotate(n=Count("order_id")).order_by("status")))
        step("select_related across 3 backends", lambda: [
            (o.customer.name, o.product.category)
            for o in Order.objects.select_related("customer", "product").filter(status="paid").order_by("order_id")[:2]])
        step("federated group by through relations", lambda: list(
            Order.objects.filter(status="paid").values("product__category", "customer__country")
            .annotate(revenue=Sum("amount")).order_by("-revenue")[:3]))

        def create_note():
            n = BackendNote.objects.create(customer_id="C0001", body="first")
            if not n.pk:
                raise AssertionError(f"no generated id: {n.pk!r}")
            return n.pk

        step("create on a backend (AutoField id)", create_note)

        def update_delete():
            n = BackendNote.objects.create(customer_id="C0002", body="draft")
            updated = BackendNote.objects.filter(pk=n.pk).update(body="final")
            n.delete()
            left = BackendNote.objects.filter(pk=n.pk).count()
            if updated != 1 or left:
                raise AssertionError(f"updated {updated}, left {left}")

        step("update + delete on a backend", update_delete)

        def atomic_rollback():
            class Rollback(Exception):
                pass
            try:
                with transaction.atomic(using=OTX):
                    BackendNote.objects.create(customer_id="C0003", body="gone")
                    raise Rollback
            except Rollback:
                pass
            left = BackendNote.objects.filter(body="gone").count()
            if left:
                raise AssertionError(f"{left} row(s) survived the rollback")

        step("atomic() rollback", atomic_rollback)

        def nested_atomic():
            with transaction.atomic(using=OTX):
                BackendNote.objects.create(customer_id="C0004", body="outer")
                try:
                    with transaction.atomic(using=OTX):  # a savepoint
                        BackendNote.objects.create(customer_id="C0004", body="inner")
                        raise ValueError
                except ValueError:
                    pass
            got = sorted(BackendNote.objects.filter(customer_id="C0004").values_list("body", flat=True))
            if got != ["outer"]:
                raise AssertionError(f"rows after the savepoint rollback: {got}")

        step("nested atomic() (savepoint)", nested_atomic)

        def read_mariadb_write_pg():
            with transaction.atomic(using=OTX):
                total = Order.objects.filter(customer_id="C0005").aggregate(t=Sum("amount"))["t"]
                BackendNote.objects.create(customer_id="C0005", body=f"total {total}")
            return total

        step("atomic(): read MariaDB, write PostgreSQL", read_mariadb_write_pg)

        def write_two_backends():
            with transaction.atomic(using=OTX):
                BackendNote.objects.create(customer_id="C0006", body="pg side")
                Order.objects.filter(order_id="O00001").update(status=F("status"))  # a no-op

        step("atomic(): write two backends", write_two_backends, refused="writing to connection")

        def django_side_drives_backend():
            Bookmark.objects.all().delete()
            Bookmark.objects.bulk_create([Bookmark(customer_id=c) for c in ("C0001", "C0002", "C0003")])
            ids = list(Bookmark.objects.values_list("customer_id", flat=True))  # "default"
            return list(Customer.objects.filter(customer_id__in=ids).order_by("customer_id")
                        .values_list("name", flat=True))  # OtterStax

        step("default-db rows drive a backend query", django_side_drives_backend)

        def table_names():
            with otx.cursor() as c:
                return len(otx.introspection.table_names(c))

        step("introspection.table_names() (place: local)", table_names)

        w = max(len(n) for n, _, _ in results)
        for name, status, note in results:
            self.stdout.write(f"{status:<8} {name:<{w}}  {note}")
