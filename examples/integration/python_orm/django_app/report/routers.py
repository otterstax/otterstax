"""Models with `otterstax = True` read and write through OtterStax; everything else is "default"."""


class OtterStaxRouter:
    def _db(self, model):
        return "otterstax" if getattr(model, "otterstax", False) else None

    def db_for_read(self, model, **hints):
        return self._db(model)

    def db_for_write(self, model, **hints):
        return self._db(model)

    def allow_relation(self, a, b, **hints):
        # relations only within one side: OtterStax models with each other (one federated JOIN)
        return getattr(a, "otterstax", False) == getattr(b, "otterstax", False)

    def allow_migrate(self, db, app_label, model_name=None, **hints):
        # nothing is ever migrated on OtterStax: the backends own their schemas
        return db != "otterstax"
