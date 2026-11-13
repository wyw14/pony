import threading
import unittest

from pony.orm.core import (
    Database,
    DirtySessionReadError,
    PrimaryKey,
    ReadBudgetError,
    ReadBudgetExceeded,
    Required,
    SessionReadBudget,
    Set,
    Optional,
    UnsafeRawStatementError,
    _build_read_path,
    db_session,
    local,
    read_path,
    select,
)
from pony.orm.tests import setup_database, teardown_database


class SQLTracer:
    """Records every SQL statement actually sent to the DBAPI driver."""

    def __init__(self, db):
        self.db = db
        self.statements = []
        self._orig = db.provider.execute

    def __enter__(self):
        tracer = self
        orig = self._orig

        def traced(cursor, sql, arguments=None, returning_id=False):
            tracer.statements.append(sql)
            return orig(cursor, sql, arguments, returning_id)

        self.db.provider.execute = traced
        return self

    def __exit__(self, *exc):
        self.db.provider.execute = self._orig

    def selects(self):
        return [
            sql
            for sql in self.statements
            if sql.lstrip().lower().startswith("select")
        ]


def make_database():
    db = Database()

    class Department(db.Entity):
        name = Required(str)
        employees = Set("Employee", nplus1_threshold=None)

    class Employee(db.Entity):
        name = Required(str)
        department = Required(Department)

    class Widget(db.Entity):
        name = Required(str)
        bio = Required(str, lazy=True)

    class Supplier(db.Entity):
        name = Required(str)
        parts = Set("Part")

    class Part(db.Entity):
        warehouse = Required(str)
        sku = Required(str)
        name = Required(str)
        supplier = Optional(Supplier)
        lines = Set("OrderLine")
        PrimaryKey(warehouse, sku)

    class OrderLine(db.Entity):
        qty = Required(int)
        part = Required(Part)

    return db, Department, Employee, Widget, Supplier, Part, OrderLine


class TestSessionReadBudget(unittest.TestCase):
    def setUp(self):
        (
            self.db,
            self.Department,
            self.Employee,
            self.Widget,
            self.Supplier,
            self.Part,
            self.OrderLine,
        ) = make_database()
        setup_database(self.db)
        with db_session:
            for i in range(5):
                self.Widget(id=i + 1, name="w%d" % i, bio="bio-%d" % i)
            d1 = self.Department(id=1, name="d1")
            d2 = self.Department(id=2, name="d2")
            d3 = self.Department(id=3, name="d3")
            self.Employee(id=1, name="e1", department=d1)
            self.Employee(id=2, name="e2", department=d1)
            self.Employee(id=3, name="e3", department=d1)
            self.Employee(id=4, name="e4", department=d2)
            self.Employee(id=5, name="e5", department=d3)
            s1 = self.Supplier(id=1, name="s1")
            self.Part(warehouse="A", sku="1", name="p1", supplier=s1)
            self.Part(warehouse="A", sku="2", name="p2", supplier=s1)
            self.Part(warehouse="B", sku="1", name="p3")
            self.OrderLine(id=1, qty=10, part=self.Part["A", "1"])
            self.OrderLine(id=2, qty=20, part=self.Part["A", "2"])

    def tearDown(self):
        teardown_database(self.db)

    # ------------------------------------------------------------------
    # Basic SQL count accounting
    # ------------------------------------------------------------------
    def test_explicit_query_counts_one_sql(self):
        db, Widget = self.db, self.Widget
        with db_session:
            with SQLTracer(db) as tracer:
                with SessionReadBudget(max_sql_count=10) as budget:
                    rows = select(w for w in Widget)[:]
            self.assertEqual(len(rows), 5)
            self.assertEqual(len(tracer.selects()), 1)
            usage = budget.usage(db)
            self.assertEqual(usage["sql_count"], 1)
            self.assertEqual(usage["rows"], 5)
            self.assertGreater(usage["result_bytes"], 0)

    def test_zero_sql_limit_blocks_before_statement_is_sent(self):
        db, Widget = self.db, self.Widget
        with db_session:
            with SQLTracer(db) as tracer:
                with SessionReadBudget(max_sql_count=0) as budget:
                    with self.assertRaises(ReadBudgetExceeded) as cm:
                        select(w for w in Widget)[:]
            self.assertEqual(tracer.statements, [])
            self.assertEqual(cm.exception.limit_type, "sql_count")
            self.assertEqual(cm.exception.limit, 0)
            self.assertEqual(budget.usage(db)["sql_count"], 0)
            self.assertTrue(
                any("query" in part for part in cm.exception.path)
            )

    def test_without_budget_behavior_is_unchanged(self):
        db, Widget = self.db, self.Widget
        self.assertFalse(SessionReadBudget.is_active())
        with db_session:
            with SQLTracer(db) as tracer:
                rows = select(w for w in Widget)[:]
            self.assertEqual(len(rows), 5)
            self.assertEqual(len(tracer.selects()), 1)
        self.assertFalse(SessionReadBudget.is_active())

    # ------------------------------------------------------------------
    # Cache hits must cost zero SQL
    # ------------------------------------------------------------------
    def test_query_result_cache_hit_costs_nothing(self):
        db, Widget = self.db, self.Widget
        with db_session:
            query = select(w for w in Widget)
            with SQLTracer(db) as tracer:
                with SessionReadBudget(max_sql_count=10) as budget:
                    first = query[:]
                    usage_after_first = budget.usage(db)
                    second = query[:]
                    usage_after_second = budget.usage(db)
            self.assertEqual(len(first), 5)
            self.assertIs(first._get_items(), second._get_items())
            self.assertEqual(len(tracer.selects()), 1)
            self.assertEqual(usage_after_first, usage_after_second)

    def test_identity_map_hit_costs_nothing(self):
        db, Employee = self.db, self.Employee
        with db_session:
            with SQLTracer(db) as tracer:
                with SessionReadBudget(max_sql_count=10) as budget:
                    e1 = Employee[1]
                    used_after_first = budget.usage(db)["sql_count"]
                    e1_again = Employee[1]
                    used_after_second = budget.usage(db)["sql_count"]
            self.assertIs(e1, e1_again)
            self.assertEqual(used_after_first, 1)
            self.assertEqual(used_after_second, 1)
            self.assertEqual(len(tracer.selects()), 1)

    def test_aggregate_counted_then_cached(self):
        db, Widget = self.db, self.Widget
        with db_session:
            query = select(w for w in Widget)
            with SQLTracer(db) as tracer:
                with SessionReadBudget(max_sql_count=10) as budget:
                    n = query.count()
                    first_usage = budget.usage(db)
                    n_again = query.count()
                    second_usage = budget.usage(db)
            self.assertEqual((n, n_again), (5, 5))
            self.assertEqual(len(tracer.selects()), 1)
            self.assertEqual(first_usage, second_usage)
            kinds = [event[0] for event in budget.events(db)]
            self.assertIn("sql", kinds)
            self.assertIn("fetch", kinds)

    # ------------------------------------------------------------------
    # N+1 lazy relationship loading
    # ------------------------------------------------------------------
    def test_n_plus_one_lazy_collection_is_accounted(self):
        db, Department = self.db, self.Department
        with db_session:
            departments = select(d for d in Department)[:]
            with SQLTracer(db) as tracer:
                with SessionReadBudget(max_sql_count=2) as budget:
                    with self.assertRaises(ReadBudgetExceeded) as cm:
                        for dept in departments:
                            list(dept.employees)
            # main query ran outside the budget; inside it: one lazy load
            # succeeds, the next one exceeds the two-statement quota.
            self.assertEqual(len(tracer.selects()), 2)
            self.assertEqual(budget.usage(db)["sql_count"], 2)
            self.assertEqual(cm.exception.limit_type, "sql_count")
            self.assertIn("lazy-collection", cm.exception.path[-1])

    def test_lazy_scalar_attribute_path(self):
        db, Widget = self.db, self.Widget
        with db_session:
            w = Widget[1]
            with SQLTracer(db) as tracer:
                with SessionReadBudget(max_sql_count=0) as budget:
                    with self.assertRaises(ReadBudgetExceeded) as cm:
                        w.bio
            self.assertEqual(tracer.selects(), [])
            self.assertIn("lazy-attr", cm.exception.path[-1])
            self.assertEqual(budget.usage(db)["sql_count"], 0)

    # ------------------------------------------------------------------
    # Row / byte quotas and partial state safety
    # ------------------------------------------------------------------
    def test_row_limit_aborts_before_query_cache_pollution(self):
        db, Widget = self.db, self.Widget
        with db_session:
            with SessionReadBudget(max_rows=2) as budget:
                with self.assertRaises(ReadBudgetExceeded) as cm:
                    select(w for w in Widget)[:]
            self.assertEqual(cm.exception.limit_type, "rows")
            cache = db._get_cache()
            self.assertEqual(cache.query_results, {})
            # The same query can be executed normally afterwards.
            rows = select(w for w in Widget)[:]
            self.assertEqual(len(rows), 5)

    def test_result_byte_limit_aborts_fetch(self):
        db, Widget = self.db, self.Widget
        with db_session:
            with SessionReadBudget(max_result_bytes=1) as budget:
                with self.assertRaises(ReadBudgetExceeded) as cm:
                    select(w for w in Widget)[:]
            self.assertEqual(cm.exception.limit_type, "result_bytes")
            self.assertGreaterEqual(
                budget.usage(db)["result_bytes"], 1
            )

    def test_partial_collection_is_not_marked_fully_loaded(self):
        db, Department, Employee = self.db, self.Department, self.Employee
        with db_session:
            dept = Department[1]
            with SessionReadBudget(max_rows=1):
                with self.assertRaises(ReadBudgetExceeded):
                    list(dept.employees)
            setdata = dept._vals_[Department.employees]
            self.assertFalse(setdata.is_fully_loaded)
            # After the budget is gone the collection loads normally.
            employees = list(dept.employees)
            self.assertEqual(len(employees), 3)
            self.assertTrue(setdata.is_fully_loaded)
            self.assertEqual(
                sorted(e.name for e in employees), ["e1", "e2", "e3"]
            )

    def test_over_limit_mid_iteration_then_continue(self):
        db, Department = self.db, self.Department
        with db_session:
            departments = select(d for d in Department).order_by(1)[:]
            loaded = []
            with SessionReadBudget(max_sql_count=2):
                try:
                    for dept in departments:
                        list(dept.employees)
                        loaded.append(dept.id)
                except ReadBudgetExceeded as exc:
                    self.assertEqual(exc.limit_type, "sql_count")
            self.assertEqual(loaded, [1, 2])
            # A fresh, larger budget allows the remaining work to finish.
            with SessionReadBudget(max_sql_count=5) as budget:
                for dept in departments:
                    list(dept.employees)
            self.assertEqual(budget.usage(db)["sql_count"], 1)
            for dept in departments:
                self.assertTrue(
                    dept._vals_[Department.employees].is_fully_loaded
                )

    # ------------------------------------------------------------------
    # Prefetch batches (incl. composite primary keys) cannot bypass
    # ------------------------------------------------------------------
    def test_composite_key_prefetch_is_accounted(self):
        db, Part = self.db, self.Part
        with db_session:
            with SQLTracer(db) as tracer:
                with SessionReadBudget(max_sql_count=10) as budget:
                    parts = select(p for p in Part).prefetch(
                        Part.supplier, Part.lines
                    )[:]
            self.assertEqual(len(parts), 3)
            select_count = len(tracer.selects())
            self.assertEqual(select_count, budget.usage(db)["sql_count"])
            # main query + supplier prefetch + order line prefetch
            self.assertEqual(select_count, 3)
            self.assertTrue(all(p._vals_ for p in parts))

    def test_tight_sql_budget_blocks_prefetch_batch(self):
        db, Part = self.db, self.Part
        with db_session:
            with SQLTracer(db) as tracer:
                with SessionReadBudget(max_sql_count=1):
                    with self.assertRaises(ReadBudgetExceeded) as cm:
                        select(p for p in Part).prefetch(Part.supplier)[:]
            self.assertEqual(len(tracer.selects()), 1)
            self.assertIn("prefetch", cm.exception.path[-1])

    # ------------------------------------------------------------------
    # Nested contexts: intersection of remaining quotas
    # ------------------------------------------------------------------
    def test_nested_outer_limit_is_the_effective_one(self):
        db, Widget = self.db, self.Widget
        with db_session:
            entity_query = select(w for w in Widget)
            count_query = select(w for w in Widget)
            with SessionReadBudget(max_sql_count=2) as outer:
                entity_query[:]
                with SessionReadBudget(max_sql_count=10) as inner:
                    count_query.count()
                    # cache hit for the entity query costs nothing
                    entity_query[:]
                    self.assertEqual(inner.remaining(db)["sql_count"], 0)
                    with self.assertRaises(ReadBudgetExceeded):
                        select(w for w in Widget).limit(2)[:]
                # inner exit must not reset the outer account
                self.assertEqual(outer.usage(db)["sql_count"], 2)

    def test_nested_inner_limit_is_stricter(self):
        db, Widget = self.db, self.Widget
        with db_session:
            entity_query = select(w for w in Widget)
            count_query = select(w for w in Widget)
            with SessionReadBudget(max_sql_count=10) as outer:
                with SessionReadBudget(max_sql_count=1) as inner:
                    entity_query[:]
                    with self.assertRaises(ReadBudgetExceeded):
                        count_query.count()
                self.assertEqual(outer.usage(db)["sql_count"], 1)
                # the outer budget still has room after the inner context
                count_query.count()
                self.assertEqual(outer.usage(db)["sql_count"], 2)

    def test_exception_inside_inner_keeps_outer_counters(self):
        db, Widget = self.db, self.Widget
        with db_session:
            with SessionReadBudget(max_sql_count=10) as outer:
                select(w for w in Widget)[:]
                used_before = outer.usage(db)["sql_count"]
                try:
                    with SessionReadBudget(max_sql_count=1):
                        raise RuntimeError("boom")
                except RuntimeError:
                    pass
                self.assertEqual(
                    outer.usage(db)["sql_count"], used_before
                )

    def test_remaining_reports_intersection(self):
        db, Widget = self.db, self.Widget
        with db_session:
            with SessionReadBudget(
                max_sql_count=5, max_rows=10, max_result_bytes=10000
            ) as outer:
                select(w for w in Widget)[:]
                with SessionReadBudget(max_sql_count=3) as inner:
                    remaining = inner.remaining(db)
                    # inner account starts fresh: min(5-1, 3-0) == 3
                    self.assertEqual(remaining["sql_count"], 3)
                    self.assertEqual(remaining["rows"], 5)

    # ------------------------------------------------------------------
    # Raw SQL: explicit reads counted, unclassified statements rejected
    # ------------------------------------------------------------------
    def test_raw_select_is_counted(self):
        db = self.db
        with db_session:
            with SessionReadBudget(max_sql_count=5) as budget:
                db.select("1")
            self.assertEqual(budget.usage(db)["sql_count"], 1)
            self.assertEqual(budget.usage(db)["rows"], 1)

    def test_strict_budget_rejects_unclassified_raw_statement(self):
        db = self.db
        with db_session:
            with SQLTracer(db) as tracer:
                with SessionReadBudget(max_sql_count=5):
                    with self.assertRaises(UnsafeRawStatementError):
                        db.execute("SELECT 1")
            self.assertEqual(tracer.statements, [])

    def test_permissive_budget_lets_unclassified_statement_bypass(self):
        db, Widget = self.db, self.Widget
        with db_session:
            with SessionReadBudget(max_sql_count=5, strict=False) as budget:
                db.execute("SELECT 1")
                self.assertEqual(budget.usage(db)["sql_count"], 0)
                # explicit writes are not reads and stay uncounted
                db.insert(Widget._table_, name="inserted", bio="raw-bio")
                self.assertEqual(budget.usage(db)["sql_count"], 0)
            self.assertEqual(Widget.get(name="inserted").name, "inserted")

    # ------------------------------------------------------------------
    # Dirty sessions and the no-flush guarantee
    # ------------------------------------------------------------------
    def test_dirty_session_is_rejected_by_default(self):
        db, Widget = self.db, self.Widget
        with db_session:
            Widget(name="dirty", bio="dirty-bio")
            self.assertTrue(db._get_cache().modified)
            with SQLTracer(db) as tracer:
                with SessionReadBudget(max_sql_count=5):
                    with self.assertRaises(DirtySessionReadError):
                        select(w for w in Widget)[:]
            self.assertNotIn(
                "select",
                " ".join(tracer.statements).lower(),
            )

    def test_dirty_session_allow_mode_keeps_normal_flush(self):
        db, Widget = self.db, self.Widget
        with db_session:
            Widget(name="dirty", bio="dirty-bio")
            with SQLTracer(db) as tracer:
                with SessionReadBudget(
                    max_sql_count=5, on_dirty="allow"
                ) as budget:
                    rows = select(w for w in Widget)[:]
            self.assertEqual(len(rows), 6)
            # The flush write is not counted as a read.
            self.assertEqual(budget.usage(db)["sql_count"], 1)
            self.assertEqual(len(tracer.selects()), 1)

    def test_flush_disabled_read_is_not_rejected(self):
        db, Department, Widget = self.db, self.Department, self.Widget
        with db_session:
            dept = Department[1]
            Widget(name="dirty-unrelated", bio="dirty-bio")
            # count() runs with autoflush disabled internally
            with SessionReadBudget(max_sql_count=5) as budget:
                n = dept.employees.count()
            self.assertEqual(n, 3)
            self.assertEqual(budget.usage(db)["sql_count"], 1)

    # ------------------------------------------------------------------
    # Thread isolation and bounded paths
    # ------------------------------------------------------------------
    def test_budget_stack_is_thread_local(self):
        with SessionReadBudget(max_sql_count=5):
            self.assertTrue(SessionReadBudget.is_active())
            seen = {}

            def worker():
                seen["active"] = SessionReadBudget.is_active()
                seen["stack"] = list(local.read_budget_stack)

            thread = threading.Thread(target=worker)
            thread.start()
            thread.join()
            self.assertFalse(seen["active"])
            self.assertEqual(seen["stack"], [])

    def test_budget_counters_are_independent_between_threads(self):
        errors = []

        def worker():
            try:
                db = Database()

                class Item(db.Entity):
                    value = Required(int)

                setup_database(db)
                with db_session:
                    for i in range(4):
                        Item(value=i)
                with db_session:
                    with SessionReadBudget(max_sql_count=1) as budget:
                        select(item for item in Item)[:]
                        assert budget.usage(db)["sql_count"] == 1
                        try:
                            select(item for item in Item).count()
                        except ReadBudgetExceeded:
                            pass
                        else:
                            raise AssertionError("expected ReadBudgetExceeded")
                teardown_database(db)
            except BaseException as exc:  # pragma: no cover
                errors.append(exc)

        thread = threading.Thread(target=worker)
        thread.start()
        thread.join()
        self.assertEqual(errors, [])

    def test_read_path_is_bounded(self):
        for _i in range(20):
            local.read_path_stack.append(("frame",))
        try:
            path = _build_read_path(("leaf",))
        finally:
            del local.read_path_stack[:]
        self.assertEqual(len(path), 8)
        self.assertEqual(path[-1], ("leaf",))

    def test_budget_can_be_used_as_decorator(self):
        db, Widget = self.db, self.Widget

        @SessionReadBudget(max_sql_count=1)
        def load_twice():
            select(w for w in Widget)[:]
            select(w for w in Widget).count()

        with db_session:
            with self.assertRaises(ReadBudgetExceeded):
                load_twice()
        self.assertFalse(SessionReadBudget.is_active())

    def test_rows_counted_at_fetch_boundary_not_object_count(self):
        db, Widget = self.db, self.Widget
        with db_session:
            with SessionReadBudget(max_sql_count=10) as budget:
                rows = select((w.id, w.name) for w in Widget)[:]
            self.assertEqual(len(rows), 5)
            # Every driver row is settled, not only materialized objects.
            self.assertEqual(budget.usage(db)["rows"], 5)
            self.assertGreater(budget.usage(db)["result_bytes"], 0)

    def test_join_rows_are_counted_before_identity_dedup(self):
        db, Department = self.db, self.Department
        with db_session:
            with SessionReadBudget(max_sql_count=10) as budget:
                pairs = select(
                    (d, e) for d in Department for e in d.employees
                )[:]
            self.assertEqual(len(pairs), 5)
            self.assertEqual(budget.usage(db)["rows"], 5)

    def test_invalid_constructor_arguments(self):
        with self.assertRaises(TypeError):
            SessionReadBudget(max_sql_count=-1)
        with self.assertRaises(TypeError):
            SessionReadBudget(max_rows="10")
        with self.assertRaises(ValueError):
            SessionReadBudget(on_dirty="unknown")

    def test_exception_is_independent_orror_type(self):
        self.assertTrue(issubclass(ReadBudgetExceeded, ReadBudgetError))
        self.assertTrue(issubclass(UnsafeRawStatementError, ReadBudgetError))
        self.assertTrue(issubclass(DirtySessionReadError, ReadBudgetError))


if __name__ == "__main__":
    unittest.main()
