from django.db import models


class Customer(models.Model):  # PostgreSQL #1
    otterstax = True
    customer_id = models.TextField(primary_key=True)
    name = models.TextField()
    country = models.CharField(max_length=2)
    tier = models.TextField()

    class Meta:
        managed = False
        db_table = '"pgshop"."shop"."customers"'


class Product(models.Model):  # PostgreSQL #2
    otterstax = True
    product_id = models.TextField(primary_key=True)
    name = models.TextField()
    category = models.TextField()
    price = models.FloatField()

    class Meta:
        managed = False
        db_table = '"pgcat"."catalog"."products"'


class Order(models.Model):  # MariaDB
    otterstax = True
    order_id = models.CharField(max_length=8, primary_key=True)
    customer = models.ForeignKey(Customer, on_delete=models.DO_NOTHING, db_constraint=False,
                                 db_column="customer_id")
    product = models.ForeignKey(Product, on_delete=models.DO_NOTHING, db_constraint=False,
                                db_column="product_id")
    amount = models.FloatField()
    status = models.CharField(max_length=16)

    class Meta:
        managed = False
        db_table = '"sales"."ops"."orders"'


class BackendNote(models.Model):  # the app's table on PostgreSQL #1 (../setup_backend.sql)
    otterstax = True
    customer_id = models.TextField()
    body = models.TextField()

    class Meta:
        managed = False
        db_table = '"pgshop"."shop"."app_notes"'


class Bookmark(models.Model):  # managed by Django, in "default": Django's side of the app
    customer_id = models.TextField()
    created_at = models.DateTimeField(auto_now_add=True)
