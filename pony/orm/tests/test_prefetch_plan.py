import unittest

from pony.orm import *
from pony.orm.tests import setup_database, teardown_database

db = Database()
db2 = Database()


# ---------------------------------------------------------------------------
# Diamond / dual-path model:
#
# Company -> departments -> employees -> Task
#                            \-> projects -> Task      (Task via two paths)
# Employee.projects is a many-to-many relation
# ---------------------------------------------------------------------------
class Company(db.Entity):
    id = PrimaryKey(int)
    name = Required(str)
    departments = Set("Department")


class Department(db.Entity):
    id = PrimaryKey(int)
    name = Required(str)
    company = Required(Company)
    employees = Set("Employee")
    projects = Set("Project")


class Employee(db.Entity):
    id = PrimaryKey(int)
    name = Required(str)
    department = Required(Department)
    projects = Set("Project")
    tasks = Set("Task")


class Project(db.Entity):
    id = PrimaryKey(int)
    name = Required(str)
    department = Required(Department)
    employees = Set(Employee)
    tasks = Set("Task")


class Task(db.Entity):
    id = PrimaryKey(int)
    title = Required(str)
    employee = Required(Employee)
    project = Optional(Project)


# ---------------------------------------------------------------------------
# Self-loop model: one-to-many tree plus symmetric many-to-many friendship
# ---------------------------------------------------------------------------
class Node(db.Entity):
    id = PrimaryKey(int)
    label = Required(str)
    parent = Optional("Node", reverse="children")
    children = Set("Node", reverse="parent")
    friends = Set("Node", reverse="friends")


# ---------------------------------------------------------------------------
# Composite primary key model
# ---------------------------------------------------------------------------
class Supplier(db.Entity):
    id = PrimaryKey(int)
    name = Required(str)
    parts = Set("Part")


class Part(db.Entity):
    a = Required(int)
    b = Required(int)
    name = Required(str)
    supplier = Required(Supplier)
    tags = Set("Tag")
    PrimaryKey(a, b)


class Tag(db.Entity):
    id = PrimaryKey(int)
    label = Required(str)
    parts = Set(Part)


# ---------------------------------------------------------------------------
# Entities mapped to a second database (cross-database guard tests)
# ---------------------------------------------------------------------------
class ForeignParent(db2.Entity):
    id = PrimaryKey(int)
    children = Set("ForeignChild")


class ForeignChild(db2.Entity):
    id = PrimaryKey(int)
    parent = Required(ForeignParent)


class TestPrefetchPlan(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        setup_database(db)
        db2.bind(provider="sqlite", filename=":memory:")
        db2.generate_mapping(create_tables=True)
        with db_session:
            c1 = Company(id=1, name="C1")
            d1 = Department(id=1, name="D1", company=c1)
            d2 = Department(id=2, name="D2", company=c1)

            e1 = Employee(id=1, name="E1", department=d1)
            e2 = Employee(id=2, name="E2", department=d1)
            e3 = Employee(id=3, name="E3", department=d1)
            e4 = Employee(id=4, name="E4", department=d1)
            e5 = Employee(id=5, name="E5", department=d2)

            p1 = Project(id=1, name="P1", department=d1)
            p2 = Project(id=2, name="P2", department=d1)
            p3 = Project(id=3, name="P3", department=d1)
            p4 = Project(id=4, name="P4", department=d2)
            e1.projects = [p1, p2]
            e2.projects = [p1, p3]
            e3.projects = [p2]
            e4.projects = [p3]
            e5.projects = [p4]

            # E1 has 3 tasks, every other employee has one; some tasks tie to
            # projects so Task is reachable through both diamond branches
            Task(id=1, title="T1", employee=e1, project=p1)
            Task(id=2, title="T2", employee=e1, project=p1)
            Task(id=3, title="T3", employee=e1, project=p2)
            Task(id=4, title="T4", employee=e2, project=p1)
            Task(id=5, title="T5", employee=e3, project=p2)
            Task(id=6, title="T6", employee=e4, project=p3)
            Task(id=7, title="T7", employee=e5, project=p4)

            s1 = Supplier(id=1, name="S1")
            tags = [Tag(id=i, label="tag%d" % i) for i in range(1, 4)]
            for i in range(1, 6):
                part = Part(a=i, b=i * 10, name="part%d" % i, supplier=s1)
                part.tags = [tags[0]] if i % 2 else [tags[0], tags[1]]
            # an extra supplier without parts
            Supplier(id=2, name="S2")

            n1 = Node(id=1, label="N1")
            n2 = Node(id=2, label="N2", parent=n1)
            n3 = Node(id=3, label="N3", parent=n1)
            Node(id=4, label="N4", parent=n2)
            n5 = Node(id=5, label="N5", parent=n2)
            n1.friends.add(n3)
            n3.friends.add(n5)

    @classmethod
    def tearDownClass(cls):
        teardown_database(db)
        db2.disconnect()

    def _db_count(self):
        return db.local_stats[None].db_count

    # -- compilation / validation -------------------------------------------

    def test_requires_relationship_attributes_no_string_eval(self):
        plan = PrefetchPlan(Department)
        with self.assertRaises(TypeError):
            plan.add_path("employees")  # strings are never evaluated
        with self.assertRaises(TypeError):
            plan.add_path(123)
        with self.assertRaises(TypeError):
            PrefetchPlan(Department.name)  # basic attribute is not an entity

    def test_unmapped_entity_rejected(self):
        raw_db = Database()

        class Unmapped(raw_db.Entity):
            id = PrimaryKey(int)

        with self.assertRaises(ERDiagramError):
            PrefetchPlan(Unmapped)

    def test_path_connectivity_validated(self):
        plan = PrefetchPlan(Company)
        with self.assertRaises(PrefetchPlanError):
            plan.add_path(Company.departments, Employee.tasks)

    def test_cross_database_rejected_on_query(self):
        plan2 = PrefetchPlan(ForeignParent)
        with db_session:
            q = select(d for d in Department)
            with self.assertRaises(TypeError):
                q.prefetch_plan(plan2)

    def test_max_objects_only_for_collections(self):
        plan = PrefetchPlan(Employee)
        with self.assertRaises(PrefetchPlanError):
            plan.add_path(Employee.department, max_objects=2)

    def test_depth_repetition_only_for_self_reference(self):
        plan = PrefetchPlan(Department)
        with self.assertRaises(PrefetchPlanError):
            plan.add_path(Department.employees, max_depth=2)

    def test_invalid_limits(self):
        with self.assertRaises(ValueError):
            PrefetchPlan(Department, max_sql=0)
        with self.assertRaises(ValueError):
            PrefetchPlan(Department, max_nodes=False)
        plan = PrefetchPlan(Department)
        with self.assertRaises(ValueError):
            plan.add_path(Department.employees, max_objects=0)
        with self.assertRaises(ValueError):
            plan.add_path(
                Department.employees,
                edge_limits=[{"max_objects": 1}, None],
            )

    def test_conflicting_budgets_same_state(self):
        plan = PrefetchPlan(Department)
        plan.add_path(Department.employees, max_objects=2)
        with self.assertRaises(PrefetchPlanError):
            plan.add_path(Department.employees, max_objects=3)

    # -- finite state plan structure -----------------------------------------

    def test_distinct_states_per_path_position(self):
        plan = PrefetchPlan(Company)
        plan.add_path(Company.departments, Department.employees, Employee.tasks)
        plan.add_path(Company.departments, Department.projects, Project.tasks)
        # root + department + employee + employee-task state
        #                + project + project-task state = 6 states; Task gets
        # two different states because the budgets/paths differ
        state_entities = [state.entity for state in plan.states]
        self.assertEqual(state_entities.count(Task), 2)
        self.assertEqual(len(plan.states), 6)

    # -- diamond + real SQL counting -----------------------------------------

    def test_diamond_dual_path_batch_loading(self):
        db.merge_local_stats()
        plan = PrefetchPlan(Company)
        plan.add_path(
            Company.departments,
            Department.employees,
            Employee.tasks,
        )
        plan.add_path(
            Company.departments,
            Department.projects,
            Project.tasks,
            edge_limits=(None, None, {"max_objects": 1}),
        )
        observer = RecordingPrefetchObserver()
        with db_session:
            company = Company[1]
            before = self._db_count()
            result = plan.load([company], observer)
            plan_sql = self._db_count() - before

            # 5 collection edges, each served by one batched SQL statement
            self.assertEqual(plan_sql, 5)
            self.assertEqual(result.sql_count, 5)
            self.assertEqual(result.layers, 4)

            # all relations are actually loaded and correct
            d1 = Department[1]
            self.assertEqual(set(d1.employees.name), {"E1", "E2", "E3", "E4"})
            self.assertEqual(len(Employee[1].tasks), 3)
            # every loaded collection is marked complete except the truncated
            for dept in Department.select():
                setdata = dept._vals_[Department.employees]
                self.assertTrue(setdata.is_fully_loaded)
            # P1 has 3 tasks, P2 has 2 tasks: both exceed the budget of 1,
            # so both stay partial without a fully-loaded flag
            p1_setdata = Project[1]._vals_[Project.tasks]
            p2_setdata = Project[2]._vals_[Project.tasks]
            self.assertFalse(p1_setdata.is_fully_loaded)
            self.assertEqual(len(p1_setdata), 1)
            self.assertFalse(p2_setdata.is_fully_loaded)
            self.assertEqual(len(p2_setdata), 1)
            self.assertTrue(Project[3]._vals_[Project.tasks].is_fully_loaded)

            # observer saw both Task edges as distinct events
            paths = {event.path for event in observer.edge_events}
            self.assertIn("Employee.tasks", paths)
            self.assertIn("Project.tasks", paths)
            task_events = [
                event for event in observer.edge_events if event.path == "Project.tasks"
            ]
            self.assertEqual(sum(e.truncated_count for e in task_events), 2)
            self.assertEqual(len(observer.truncated), 2)
            by_project = {record.obj.id: record for record in observer.truncated}
            self.assertIsInstance(observer.truncated[0], TruncatedPrefetch)
            self.assertEqual(by_project[1].available_count, 3)
            self.assertEqual(by_project[1].loaded_count, 1)
            self.assertEqual(by_project[2].available_count, 2)
            self.assertEqual(by_project[2].loaded_count, 1)
            for record in observer.truncated:
                self.assertEqual(record.max_objects, 1)

    def test_dual_path_different_depth_and_budget(self):
        db.merge_local_stats()
        plan = PrefetchPlan(Company)
        # shallow path: departments -> employees (depth 2 from root)
        plan.add_path(
            Company.departments,
            Department.employees,
            edge_limits=[None, {"max_objects": 2}],
        )
        # deep path: departments -> projects -> tasks (depth 3)
        plan.add_path(Company.departments, Department.projects, Project.tasks)
        with db_session:
            result = plan.load([Company[1]])
            self.assertEqual(result.layers, 4)
            # D1 keeps only 2 of its 4 employees; D2 keeps its single employee
            d1_setdata = Department[1]._vals_[Department.employees]
            d2_setdata = Department[2]._vals_[Department.employees]
            self.assertFalse(d1_setdata.is_fully_loaded)
            self.assertEqual(len(d1_setdata), 2)
            self.assertTrue(d2_setdata.is_fully_loaded)
            self.assertEqual(len(d2_setdata), 1)
            # projects were fully loaded on both departments
            self.assertTrue(Department[1]._vals_[Department.projects].is_fully_loaded)

    # -- self-loop termination -----------------------------------------------

    def test_self_loop_tree_depth(self):
        db.merge_local_stats()
        plan = PrefetchPlan(Node)
        plan.add_path(Node.children, max_depth=2)
        with db_session:
            result = plan.load([Node[1]])
            self.assertEqual(result.layers, 3)
            labels = {node.label for node in Node.select()}
            # grandchildren N4/N5 must be reachable
            n4 = Node[4]
            n5 = Node[5]
            self.assertEqual(set(Node[2].children.label), {"N4", "N5"})
            self.assertIn("N4", labels)
            self.assertIn("N5", labels)
            # the same node is tracked per (identity, state): 5 distinct
            # (obj, state) visits for the 5-node tree
            self.assertEqual(result.nodes_visited, 5)

    def test_self_loop_symmetric_m2m_terminates(self):
        db.merge_local_stats()
        plan = PrefetchPlan(Node)
        plan.add_path(Node.friends, max_depth=2)
        with db_session:
            # must terminate instead of expanding the reverse edge forever
            result = plan.load([Node[1]])
            self.assertEqual(result.layers, 3)
            friend_labels = set()
            for node in Node.select():
                setdata = node._vals_.get(Node.friends)
                if setdata is not None and setdata.is_fully_loaded:
                    friend_labels.update(n.label for n in setdata)
            self.assertIn("N5", friend_labels)
            # N1 is visited again at the second friends state, but the
            # (obj, state) key prevents infinite recursion
            self.assertGreaterEqual(result.nodes_visited, 4)

    # -- composite key batching ----------------------------------------------

    def test_composite_key_respects_max_params_count(self):
        db.merge_local_stats()
        original_limit = db.provider.max_params_count
        plan = PrefetchPlan(Part)
        plan.add_path(Part.tags)
        try:
            # Part has a 2-column PK: batch size becomes 4 // 2 = 2,
            # so 5 parts require 3 batched SQL statements
            db.provider.max_params_count = 4
            with db_session:
                parts = list(Part.select().order_by(Part.a))
                self.assertEqual(len(parts), 5)
                before = self._db_count()
                result = plan.load(parts)
                used = self._db_count() - before
                self.assertEqual(used, 3)
                self.assertEqual(result.sql_count, 3)
                for part in parts:
                    setdata = part._vals_[Part.tags]
                    self.assertTrue(setdata.is_fully_loaded)
                self.assertEqual(set(Part[1, 10].tags.label), {"tag1"})
                self.assertEqual(set(Part[2, 20].tags.label), {"tag1", "tag2"})
        finally:
            db.provider.max_params_count = original_limit

    # -- truncation semantics -------------------------------------------------

    def test_truncated_collection_stays_lazy(self):
        db.merge_local_stats()
        plan = PrefetchPlan(Department)
        plan.add_path(Department.employees, max_objects=2)
        with db_session:
            d1 = Department[1]
            result = plan.load([d1])
            self.assertEqual(len(result.truncated), 1)
            record = result.truncated[0]
            self.assertEqual(record.available_count, 4)
            self.assertEqual(record.loaded_count, 2)
            setdata = d1._vals_[Department.employees]
            self.assertFalse(setdata.is_fully_loaded)
            self.assertIsNone(setdata.count)

            # count() must not trust the partial cache: it issues COUNT SQL
            before = self._db_count()
            self.assertEqual(d1.employees.count(), 4)
            self.assertEqual(self._db_count() - before, 1)
            self.assertFalse(setdata.is_fully_loaded)

            # full iteration lazy-loads the rest and then marks complete
            before = self._db_count()
            self.assertEqual(len(d1.employees), 4)
            self.assertEqual(self._db_count() - before, 1)
            self.assertTrue(setdata.is_fully_loaded)

    def test_membership_after_truncation(self):
        plan = PrefetchPlan(Employee)
        plan.add_path(Employee.projects, max_objects=1)

        # positive membership: a truncated (partial) collection must consult
        # the database instead of trusting its cached subset
        db.merge_local_stats()
        with db_session:
            e1 = Employee[1]  # linked with P1 and P2
            plan.load([e1])
            setdata = e1._vals_[Employee.projects]
            self.assertEqual(len(setdata), 1)
            self.assertFalse(setdata.is_fully_loaded)
            kept = set(setdata)
            missing = next(p for p in Project.select() if p not in kept and p.id == 2)
            before = self._db_count()
            self.assertIn(missing, e1.projects)
            self.assertGreaterEqual(self._db_count() - before, 1)

        # negative membership on a fresh partial cache hits the database too
        db.merge_local_stats()
        with db_session:
            e1 = Employee[1]
            plan.load([e1])
            self.assertFalse(e1._vals_[Employee.projects].is_fully_loaded)
            before = self._db_count()
            self.assertNotIn(Project[4], e1.projects)
            self.assertGreaterEqual(self._db_count() - before, 1)

    def test_truncation_stops_next_layer_expansion(self):
        db.merge_local_stats()
        plan = PrefetchPlan(Department)
        plan.add_path(
            Department.employees,
            Employee.tasks,
            edge_limits=[{"max_objects": 2}, None],
        )
        with db_session:
            d1 = Department[1]
            plan.load([d1])
            kept = set(d1._vals_[Department.employees])
            self.assertEqual(len(kept), 2)
            for employee in kept:
                self.assertTrue(employee._vals_[Employee.tasks].is_fully_loaded)
            skipped = next(
                e for e in Employee.select() if e.department == d1 and e not in kept
            )
            # the skipped employee's tasks were never loaded
            self.assertNotIn(Employee.tasks, skipped._vals_)

    # -- identity map reuse ---------------------------------------------------

    def test_m2m_shared_objects_reuse_identity_map(self):
        db.merge_local_stats()
        plan = PrefetchPlan(Employee)
        plan.add_path(Employee.projects)
        with db_session:
            employees = list(Employee.select().order_by(Employee.id))
            plan.load(employees)
            e1_projects = set(employees[0].projects)
            e2_projects = set(employees[1].projects)
            shared = e1_projects & e2_projects
            self.assertEqual({p.name for p in shared}, {"P1"})
            # the shared project is literally the same cached object
            p1_via_e1 = next(p for p in e1_projects if p.name == "P1")
            p1_via_e2 = next(p for p in e2_projects if p.name == "P1")
            self.assertIs(p1_via_e1, p1_via_e2)
            self.assertIs(p1_via_e1, Project[1])

    # -- to-one edges issue no extra SQL when FK is already loaded -----------

    def test_to_one_edge_uses_cached_fk(self):
        db.merge_local_stats()
        plan = PrefetchPlan(Employee)
        plan.add_path(Employee.department, Department.employees)
        with db_session:
            employees = list(Employee.select())
            before = self._db_count()
            result = plan.load(employees)
            used = self._db_count() - before
            # to-one walk uses FK values already present on employee rows;
            # only the reverse collection costs one batched statement
            self.assertEqual(used, 1)
            self.assertEqual(result.sql_count, 1)
            self.assertEqual(Employee[1].department, Department[1])

    # -- Query integration & immutability ------------------------------------

    def test_query_prefetch_plan_hook(self):
        db.merge_local_stats()
        plan = PrefetchPlan(Department)
        plan.add_path(Department.employees, Employee.projects)
        observer = RecordingPrefetchObserver()
        with db_session:
            q = select(d for d in Department).order_by(Department.id)
            q2 = q.prefetch_plan(plan, observer)
            # original query is untouched
            self.assertIsNone(q._prefetch_plan)
            departments = list(q2)
            self.assertEqual(len(departments), 2)
            self.assertEqual(observer.result.sql_count, 2)
            self.assertEqual(len(Department[1].employees), 4)
            # plan data is correct through the M2M edge as well
            self.assertEqual({p.name for p in Employee[1].projects}, {"P1", "P2"})

    def test_query_prefetch_plan_with_tuple_result(self):
        db.merge_local_stats()
        plan = PrefetchPlan(Department)
        plan.add_path(Department.employees)
        with db_session:
            q = select((d.id, d) for d in Department)
            rows = list(q.prefetch_plan(plan))
            self.assertEqual(len(rows), 2)
            for _, department in rows:
                self.assertTrue(department._vals_[Department.employees].is_fully_loaded)

    def test_prefetch_context_and_query_remain_immutable(self):
        plan = PrefetchPlan(Department)
        plan.add_path(Department.employees)
        with db_session:
            q = select(d for d in Department)
            context = q._prefetch_context
            before_entities = set(context.entities_to_prefetch)
            before_attrs = {
                entity: set(attrs)
                for entity, attrs in context.attrs_to_prefetch_dict.items()
            }
            q.prefetch_plan(plan)
            self.assertEqual(context.entities_to_prefetch, before_entities)
            self.assertEqual(
                {
                    entity: set(attrs)
                    for entity, attrs in context.attrs_to_prefetch_dict.items()
                },
                before_attrs,
            )
            self.assertIsNone(q._prefetch_plan)
            self.assertFalse(q._prefetch)

    def test_plan_is_reusable_across_queries(self):
        plan = PrefetchPlan(Department)
        plan.add_path(Department.employees)
        with db_session:
            list(select(d for d in Department).prefetch_plan(plan))
        with db_session:
            departments = list(select(d for d in Department).prefetch_plan(plan))
            self.assertTrue(departments[0]._vals_[Department.employees].is_fully_loaded)
        with self.assertRaises(PrefetchPlanError):
            plan.add_path(Department.projects)  # locked after execution

    # -- global SQL / node limits --------------------------------------------

    def test_sql_limit_enforced(self):
        plan = PrefetchPlan(Department, max_sql=1)
        plan.add_path(Department.employees, Employee.projects)
        with db_session:
            with self.assertRaises(PrefetchPlanLimitError):
                plan.load(list(Department.select()))

    def test_node_limit_enforced(self):
        plan = PrefetchPlan(Department, max_nodes=1)
        plan.add_path(Department.employees)
        with db_session:
            with self.assertRaises(PrefetchPlanLimitError):
                plan.load(list(Department.select()))

    # -- error safety ---------------------------------------------------------

    def test_loading_error_leaves_no_false_loaded_flags(self):
        plan = PrefetchPlan(Department)
        plan.add_path(Department.employees)
        with db_session:
            d1 = Department[1]
            original_exec_sql = db._exec_sql

            def failing_exec_sql(sql, arguments=None, returning_id=False):
                raise RuntimeError("simulated database failure")

            db._exec_sql = failing_exec_sql
            try:
                with self.assertRaises(RuntimeError):
                    plan.load([d1])
            finally:
                db._exec_sql = original_exec_sql

            setdata = d1._vals_.get(Department.employees)
            self.assertTrue(setdata is None or not setdata.is_fully_loaded)
            # the collection still lazy-loads correctly afterwards
            self.assertEqual(len(d1.employees), 4)
            self.assertTrue(d1._vals_[Department.employees].is_fully_loaded)

    # -- observer restrictions ------------------------------------------------

    def test_observer_receives_layer_and_edge_events(self):
        plan = PrefetchPlan(Department)
        plan.add_path(Department.employees)
        observer = RecordingPrefetchObserver()
        with db_session:
            plan.load(list(Department.select()), observer)
        self.assertEqual(observer.layer_events, [(0, 2), (1, 5)])
        self.assertEqual(len(observer.edge_events), 1)
        event = observer.edge_events[0]
        self.assertIsInstance(event, PrefetchEdgeEvent)
        self.assertEqual(event.path, "Department.employees")
        self.assertEqual(event.source_count, 2)
        self.assertEqual(event.loaded_count, 5)
        self.assertEqual(event.sql_count, 1)
        self.assertIs(observer.result.observer, observer)

    def test_invalid_observer_rejected(self):
        plan = PrefetchPlan(Department)
        plan.add_path(Department.employees)
        with db_session:
            with self.assertRaises(TypeError):
                plan.load([Department[1]], observer=object())


if __name__ == "__main__":
    unittest.main()
