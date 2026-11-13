import unittest

from pony.orm import *
from pony.orm.tests import setup_database, teardown_database

db = Database()

# Observed ORM after_delete hook calls; explain_delete() must never populate it.
delete_hook_log = []


class TreeNode(db.Entity):
    id = PrimaryKey(int)
    title = Required(str)
    parent = Optional("TreeNode", reverse="children")
    children = Set("TreeNode", reverse="parent", cascade_delete=True)

    def after_delete(self):
        delete_hook_log.append(self.id)


class BlockParent(db.Entity):
    id = PrimaryKey(int)
    # Required reverse + cascade_delete disabled -> deletion is blocked
    blocked_children = Set("BlockChild", cascade_delete=False)


class BlockChild(db.Entity):
    id = PrimaryKey(int)
    parent = Required(BlockParent)


class NullParent(db.Entity):
    id = PrimaryKey(int)
    # Nullable reverse + cascade_delete disabled -> FKs are set to NULL
    null_children = Set("NullChild", cascade_delete=False)


class NullChild(db.Entity):
    id = PrimaryKey(int)
    parent = Optional(NullParent)


class Person(db.Entity):
    id = PrimaryKey(int)
    # Symmetric many-to-many, used for m2m cleanup and traversal cycles
    friends = Set("Person", reverse="friends")


class A(db.Entity):
    id = PrimaryKey(int)
    b1s = Set("B1", cascade_delete=True)
    b2s = Set("B2", cascade_delete=True)


class B1(db.Entity):
    id = PrimaryKey(int)
    a = Required(A)
    shareds = Set("Shared", cascade_delete=True)


class B2(db.Entity):
    id = PrimaryKey(int)
    a = Required(A)
    shareds = Set("Shared", cascade_delete=True)


class Shared(db.Entity):
    id = PrimaryKey(int)
    b1 = Required(B1)
    b2 = Required(B2)


class LazyHolder(db.Entity):
    id = PrimaryKey(int)
    # Lazy scalar relation: absent from _vals_ until explicitly loaded
    buddy = Optional("LazyBuddy", lazy=True, cascade_delete=True)


class LazyBuddy(db.Entity):
    id = PrimaryKey(int)
    holder = Optional(LazyHolder, reverse="buddy")


ALL_ENTITIES = (
    TreeNode,
    BlockParent,
    BlockChild,
    NullParent,
    NullChild,
    Person,
    A,
    B1,
    B2,
    Shared,
    LazyHolder,
    LazyBuddy,
)


def table_row_count(entity):
    table = entity._table_
    quoted = db.provider.quote_name(table)
    return db.select("select count(*) from " + quoted)[0]


def table_pks(entity):
    # Raw SQL keeps the check provider-neutral and independent of the
    # ORM identity map; all test entities use a single-column primary key.
    assert len(entity._pk_columns_) == 1
    sql = "select %s from %s" % (
        db.provider.quote_name(entity._pk_columns_[0]),
        db.provider.quote_name(entity._table_),
    )
    return set(db.select(sql))


def link_table_count():
    table = Person.friends.table
    quoted = db.provider.quote_name(table)
    return db.select("select count(*) from " + quoted)[0]


def graph_signature(graph):
    return (
        tuple(sorted(node.label + "|" + node.action for node in graph.nodes.values())),
        tuple(
            sorted(
                (edge.attr, edge.kind, edge.label, edge.reason) for edge in graph.edges
            )
        ),
        graph.status,
    )


def _obj_key(obj):
    return (
        type(obj).__name__,
        obj.get_pk() if obj._pkval_ is not None else ("new", obj._newid_),
    )


def session_snapshot():
    """Mutation-relevant session state.

    Plain read loading is allowed to grow the identity map, so the snapshot
    tracks only observable mutations: object statuses, pending saves,
    modified-collection bookkeeping and the added/removed sets of collections
    (never the mere fact that a collection got cached).
    """
    cache = db._get_cache()
    statuses = frozenset(_obj_key(obj) + (obj._status_,) for obj in cache.objects)
    collection_mutations = set()
    for obj in cache.objects:
        for attr in obj._attrs_:
            if not attr.is_collection:
                continue
            setdata = obj._vals_.get(attr)
            if setdata is None:
                continue
            collection_mutations.add(
                (
                    _obj_key(obj),
                    attr.entity.__name__ + "." + attr.name,
                    frozenset(_obj_key(x) for x in setdata.added)
                    if setdata.added
                    else frozenset(),
                    frozenset(_obj_key(x) for x in setdata.removed)
                    if setdata.removed
                    else frozenset(),
                )
            )
    return (
        cache.modified,
        statuses,
        frozenset(collection_mutations),
        frozenset(_obj_key(obj) for obj in cache.objects_to_save if obj is not None),
        frozenset(
            (attr.entity.__name__ + "." + attr.name, len(objs))
            for attr, objs in cache.modified_collections.items()
        ),
    )


class TestExplainDelete(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        setup_database(db)
        with db_session:
            t1 = TreeNode(id=1, title="secret-tree-1")
            t2 = TreeNode(id=2, title="secret-tree-2", parent=t1)
            TreeNode(id=3, title="secret-tree-3", parent=t1)
            TreeNode(id=4, title="secret-tree-4", parent=t2)
            # Persisted nullable cycle t5 <-> t6 (flush first, then close
            # the cycle with a single UPDATE)
            t5 = TreeNode(id=5, title="secret-tree-5")
            t6 = TreeNode(id=6, title="secret-tree-6", parent=t5)
            flush()
            t5.parent = t6

            bp = BlockParent(id=1)
            BlockChild(id=1, parent=bp)

            np = NullParent(id=1)
            NullChild(id=1, parent=np)
            NullChild(id=2, parent=np)

            p1 = Person(id=1)
            p2 = Person(id=2)
            p3 = Person(id=3)
            p1.friends = [p2, p3]
            p2.friends.add(p3)

            a1 = A(id=1)
            a2 = A(id=2)
            b11 = B1(id=1, a=a1)
            b12 = B1(id=2, a=a2)
            b21 = B2(id=1, a=a1)
            b22 = B2(id=2, a=a2)
            # Shared(1) is reachable from both roots through different paths
            Shared(id=1, b1=b11, b2=b22)
            Shared(id=2, b1=b12, b2=b21)

            LazyBuddy(id=1, holder=LazyHolder(id=1))

    @classmethod
    def tearDownClass(cls):
        teardown_database(db)

    def setUp(self):
        delete_hook_log[:] = []
        rollback()
        db_session.__enter__()

    def tearDown(self):
        rollback()
        db_session.__exit__()

    # ------------------------------------------------------------------
    # Required relationship blocker
    # ------------------------------------------------------------------
    def test_required_blocker(self):
        bp = BlockParent[1]
        graph = bp.explain_delete()
        self.assertEqual(graph.status, "complete")
        self.assertTrue(graph.has_blockers)
        self.assertFalse(graph.is_executable)
        blockers = graph.edges_of_kind("required_blocker")
        self.assertEqual(1, len(blockers))
        edge = blockers[0]
        self.assertEqual(edge.target, (BlockChild, (1,)))
        self.assertEqual(edge.orm_action, "raise_constraint_error")
        # Required relation without cascade has no database ON DELETE action
        self.assertIsNone(edge.db_on_delete)
        self.assertEqual(
            [node.action for node in graph.nodes.values() if node.entity is BlockChild],
            ["blocked"],
        )
        # Preview does not fire any ORM hook
        self.assertEqual([], delete_hook_log)
        # The real delete() raises ConstraintError for the same reason
        with self.assertRaises(ConstraintError):
            bp.delete()
        self.assertEqual("loaded", bp._status_)
        # Sensitive ordinary attribute values never leak into the report
        rendered = (
            repr(graph)
            + "".join(map(repr, graph.nodes.values()))
            + "".join(map(repr, graph.edges))
        )
        self.assertNotIn("secret-", rendered)
        self.assertIn("BlockParent[1]", rendered)

    # ------------------------------------------------------------------
    # Nullable reverse edges
    # ------------------------------------------------------------------
    def test_nullable_reverse_edges(self):
        graph = NullParent[1].explain_delete()
        self.assertTrue(graph.is_complete)
        self.assertTrue(graph.is_executable)
        unlinks = graph.edges_of_kind("nullable_unlink")
        self.assertEqual(2, len(unlinks))
        targets = sorted(edge.target[1][0] for edge in unlinks)
        self.assertEqual([1, 2], targets)
        for edge in unlinks:
            self.assertEqual("SET NULL", edge.db_on_delete)
            self.assertEqual(edge.orm_action, "clear_collection_set_null")
        self.assertEqual({(NullParent, (1,))}, set(graph.deleted_identities()))
        detached = [node for node in graph.nodes.values() if node.action == "detached"]
        self.assertEqual(2, len(detached))

    # ------------------------------------------------------------------
    # Cascade cycles (self-referential cascade tree + persisted cycle)
    # ------------------------------------------------------------------
    def test_cascade_tree(self):
        graph = TreeNode[1].explain_delete()
        self.assertTrue(graph.is_complete)
        cascades = graph.edges_of_kind("cascade_delete")
        self.assertTrue(cascades)
        deleted = graph.deleted_pks_by_entity()[TreeNode]
        self.assertEqual({(1,), (2,), (3,), (4,)}, deleted)
        # No hooks fired during preview
        self.assertEqual([], delete_hook_log)

    def test_cascade_cycle_terminates(self):
        graph = TreeNode[5].explain_delete()
        self.assertTrue(graph.is_complete)
        deleted = graph.deleted_pks_by_entity()[TreeNode]
        self.assertEqual({(5,), (6,)}, deleted)
        # Edge back to the already visited root proves cycle handling
        kinds = {
            (edge.source[1][0], edge.target[1][0], edge.kind)
            for edge in graph.edges
            if edge.kind == "cascade_delete"
        }
        self.assertIn((5, 6, "cascade_delete"), kinds)
        self.assertIn((6, 5, "cascade_delete"), kinds)

    def test_m2m_cycle_cleanup(self):
        graph = Person[1].explain_delete()
        self.assertTrue(graph.is_complete)
        cleanups = graph.edges_of_kind("m2m_cleanup")
        self.assertEqual(2, len(cleanups))
        for edge in cleanups:
            self.assertEqual("delete_m2m_link_rows", edge.orm_action)
            self.assertEqual("CASCADE", edge.db_on_delete)
            self.assertEqual(Person.friends.table, edge.m2m_table)
        # m2m endpoints survive
        survivor_ids = {
            node.pk[0] for node in graph.nodes.values() if node.action == "detached"
        }
        self.assertEqual({2, 3}, survivor_ids)
        self.assertEqual({(Person, (1,))}, set(graph.deleted_identities()))

    # ------------------------------------------------------------------
    # Multiple roots sharing descendants, deduplicated by full identity
    # ------------------------------------------------------------------
    def test_multiple_roots_shared_descendants(self):
        a1, a2 = A[1], A[2]
        # Both instance-style and class-style calls are supported
        graph = A.explain_delete(a1, a2)
        self.assertTrue(graph.is_complete)
        root_nodes = [node for node in graph.nodes.values() if node.is_root]
        self.assertEqual(2, len(root_nodes))
        shared_nodes = [node for node in graph.nodes.values() if node.entity is Shared]
        self.assertEqual(2, len(shared_nodes))  # each Shared appears once
        deleted = graph.deleted_pks_by_entity()
        self.assertEqual({(1,), (2,)}, deleted[A])
        self.assertEqual({(1,), (2,)}, deleted[B1])
        self.assertEqual({(1,), (2,)}, deleted[B2])
        self.assertEqual({(1,), (2,)}, deleted[Shared])
        # Every identity occurs at most once
        identities = [node.identity for node in graph.nodes.values()]
        self.assertEqual(len(identities), len(set(identities)))

    # ------------------------------------------------------------------
    # Budgets and restricted loading
    # ------------------------------------------------------------------
    def test_sql_budget_interruption_keeps_unknown_edges(self):
        graph = TreeNode[1].explain_delete(max_sql=0)
        self.assertEqual("incomplete", graph.status)
        self.assertTrue(graph.unknown_edges)
        self.assertEqual(0, graph.used["sql"])
        reasons = {edge.reason for edge in graph.unknown_edges}
        self.assertEqual({"sql_budget"}, reasons)
        # The root itself is retained even with a zero SQL budget
        self.assertEqual([(TreeNode, (1,))], graph.deleted_identities())

    def test_node_budget_interruption(self):
        graph = TreeNode[1].explain_delete(max_nodes=1)
        self.assertEqual("incomplete", graph.status)
        self.assertEqual(1, graph.used["nodes"])
        self.assertTrue(
            any(edge.reason == "node_budget" for edge in graph.unknown_edges)
        )
        # A zero node budget still retains the explicit root itself
        zero = TreeNode[1].explain_delete(max_nodes=0)
        self.assertEqual("incomplete", zero.status)
        self.assertEqual([(TreeNode, (1,))], zero.deleted_identities())
        self.assertTrue(
            any(edge.reason == "node_budget" for edge in zero.unknown_edges)
        )

    def test_edge_budget_interruption(self):
        graph = TreeNode[1].explain_delete(max_edges=1)
        self.assertEqual("incomplete", graph.status)
        known = [e for e in graph.edges if e.kind != "unknown"]
        self.assertLessEqual(len(known), 1)
        self.assertTrue(
            any(edge.reason == "edge_budget" for edge in graph.unknown_edges)
        )

    def test_load_disabled_never_treats_unloaded_as_empty(self):
        graph = BlockParent[1].explain_delete(load=False)
        self.assertEqual("incomplete", graph.status)
        self.assertTrue(
            any(edge.reason == "load_disallowed" for edge in graph.unknown_edges)
        )
        # With loading enabled the very same collection reveals the blocker
        full = BlockParent[1].explain_delete()
        self.assertTrue(full.has_blockers)

    def test_load_allow_list(self):
        allowed = TreeNode.children
        graph = TreeNode[1].explain_delete(load=[allowed])
        self.assertTrue(graph.is_complete)
        self.assertIn("cascade_delete", {edge.kind for edge in graph.edges})

    def test_unloaded_lazy_scalar_keeps_unknown_edge(self):
        holder = LazyHolder[1]
        self.assertNotIn(LazyHolder.buddy, holder._vals_)
        graph = holder.explain_delete(load=False)
        self.assertTrue(graph.is_incomplete)
        edge = next(
            edge for edge in graph.unknown_edges if edge.attr == "LazyHolder.buddy"
        )
        self.assertEqual("load_disallowed", edge.reason)
        # Never conflated with an empty relation: the buddy node is absent
        self.assertNotIn(
            (LazyBuddy, (1,)),
            [node.identity for node in graph.nodes.values()],
        )

    def test_lazy_scalar_sql_budget(self):
        holder = LazyHolder[1]
        graph = holder.explain_delete(max_sql=0)
        self.assertTrue(graph.is_incomplete)
        self.assertEqual(0, graph.used["sql"])
        self.assertTrue(
            any(
                edge.attr == "LazyHolder.buddy" and edge.reason == "sql_budget"
                for edge in graph.unknown_edges
            )
        )

    def test_lazy_scalar_cascade_loaded_on_demand(self):
        graph = LazyHolder[1].explain_delete()
        self.assertTrue(graph.is_complete)
        self.assertEqual(1, graph.used["sql"])
        buddy_edges = [edge for edge in graph.edges if edge.attr == "LazyHolder.buddy"]
        self.assertEqual(1, len(buddy_edges))
        edge = buddy_edges[0]
        self.assertEqual("cascade_delete", edge.kind)
        self.assertEqual((LazyBuddy, (1,)), edge.target)
        self.assertEqual(
            {(LazyHolder, (1,)), (LazyBuddy, (1,))},
            set(graph.deleted_identities()),
        )

    def test_full_budget_is_complete(self):
        graph = TreeNode[1].explain_delete(max_nodes=100, max_edges=100, max_sql=100)
        self.assertTrue(graph.is_complete)
        self.assertEqual(4, graph.used["nodes"])

    def test_invalid_arguments(self):
        with self.assertRaises(TypeError):
            TreeNode[1].explain_delete(max_nodes=-1)
        with self.assertRaises(TypeError):
            TreeNode[1].explain_delete(max_edges=True)
        with self.assertRaises(TypeError):
            TreeNode[1].explain_delete(load=123)
        with self.assertRaises(TypeError):
            TreeNode[1].explain_delete(load=[TreeNode.id])
        with self.assertRaises(TypeError):
            TreeNode[1].explain_delete(42)
        with self.assertRaises(DeleteExplainError):
            obj = TreeNode[1]
            obj.delete()
            obj.explain_delete()

    # ------------------------------------------------------------------
    # Read-only behavior and observation versioning
    # ------------------------------------------------------------------
    def test_preview_is_read_only_and_select_only(self):
        t1 = TreeNode[1]
        # A dirty, unflushed object must survive the preview untouched
        t1.title = "dirty-unflushed-title"
        before = session_snapshot()
        db.merge_local_stats()
        graph = t1.explain_delete()
        stats_sql = [sql for sql in db.local_stats if sql is not None]
        after = session_snapshot()
        # Identity-map growth through read caching is allowed, but no object
        # may change status and no mutation marker may appear/disappear.
        self.assertEqual(before[0], after[0])
        self.assertTrue(before[1] <= after[1])
        self.assertTrue(before[2] <= after[2])
        self.assertEqual(before[3], after[3])
        self.assertEqual(before[4], after[4])
        self.assertTrue(stats_sql)
        for sql in stats_sql:
            normalized = sql.lstrip().upper()
            self.assertTrue(normalized.startswith("SELECT"), sql)
            self.assertNotIn("FOR UPDATE", normalized)
        # The dirty object was not flushed and not marked for deletion
        self.assertEqual("modified", t1._status_)
        self.assertTrue(db._get_cache().modified)
        self.assertFalse(graph.is_observation_stale())

    def test_mapping_and_session_binding(self):
        cache = db._get_cache()
        graph = TreeNode[1].explain_delete(max_sql=0)
        self.assertEqual(db.provider.dialect, graph.mapping_info["provider"])
        self.assertEqual(cache.num, graph.session_info["session_num"])
        self.assertEqual(cache.num, graph._cache_num)
        # After rollback the session is gone, so the observation is stale
        rollback()
        db_session.__exit__()
        db_session.__enter__()
        try:
            self.assertTrue(graph.is_observation_stale())
        finally:
            rollback()
            db_session.__exit__()
            db_session.__enter__()

    # ------------------------------------------------------------------
    # Rollback-isolated execution: real delete() vs predicted impact graph
    # ------------------------------------------------------------------
    def _assert_graph_matches_real_delete(self, roots, graph):
        self.assertTrue(graph.is_executable)
        entity_tables = {entity: table_row_count(entity) for entity in ALL_ENTITIES}
        pks_before = {entity: table_pks(entity) for entity in ALL_ENTITIES}
        links_before = link_table_count()
        signature_before = graph_signature(graph)
        root_keys = [(type(root), root.get_pk()) for root in roots]

        for root in roots:
            root.delete()
        flush()

        # Row-level changes actually performed by the ORM
        for entity in ALL_ENTITIES:
            pks_after = table_pks(entity)
            missing = pks_before[entity] - pks_after
            predicted = {
                pk[0] if len(pk) == 1 else pk
                for pk in graph.deleted_pks_by_entity().get(entity, set())
            }
            self.assertEqual(
                predicted,
                missing,
                "row changes for %s do not match impact graph" % entity.__name__,
            )
            survivors = pks_before[entity] & pks_after
            self.assertEqual(
                len(survivors),
                entity_tables[entity] - len(missing),
            )

        # Nullable edges materialize as SET NULL, but only when the target
        # row survives; a target that is itself deleted leaves nothing to
        # unlink. Verify every surviving target generically per entity table.
        nullable = graph.edges_of_kind("nullable_unlink")
        deleted = set(graph.deleted_identities())
        expected_nulls = {}
        for edge in nullable:
            if edge.target in deleted:
                continue
            target_entity, target_pk = edge.target
            reverse_name = edge.reverse_attr.split(".", 1)[1]
            key = (target_entity, reverse_name)
            expected_nulls.setdefault(key, set()).add(target_pk[0])
        for (target_entity, reverse_name), pks in expected_nulls.items():
            fk_attr = target_entity._adict_[reverse_name]
            nulled_sql = "select %s from %s where %s is null" % (
                db.provider.quote_name(target_entity._pk_columns_[0]),
                db.provider.quote_name(target_entity._table_),
                db.provider.quote_name(fk_attr.columns[0]),
            )
            self.assertEqual(pks, set(db.select(nulled_sql)))

        # m2m cleanup edges materialize as removed link-table rows. A
        # symmetric m2m relationship stores one mirrored row per direction,
        # and the mirror rows are removed by the database ON DELETE CASCADE
        # rather than by an ORM hook -- both mechanisms are observed here but
        # stay distinct in the impact graph (one m2m_cleanup edge per pair).
        m2m = graph.edges_of_kind("m2m_cleanup")
        expected_removed_rows = 0
        for edge in m2m:
            source_entity, unused_pk = edge.source
            attr_name = edge.attr.split(".", 1)[1]
            attr = source_entity._adict_[attr_name]
            expected_removed_rows += 2 if attr.symmetric else 1
        self.assertEqual(links_before - expected_removed_rows, link_table_count())

        # Undo everything inside the rollback-isolated transaction and
        # verify the persisted world is restored to its pre-execution state.
        rollback()
        db_session.__exit__()
        db_session.__enter__()
        for entity, count in entity_tables.items():
            self.assertEqual(
                count,
                table_row_count(entity),
                "rollback did not restore %s" % entity.__name__,
            )
        self.assertEqual(links_before, link_table_count())

        # The roots belong to the rolled-back session, so re-fetch them and
        # rebuild the graph in the fresh session for an identical prediction.
        fresh_roots = [entity[pk] for entity, pk in root_keys]
        rebuilt = fresh_roots[0].explain_delete(
            *fresh_roots[1:],
            max_nodes=graph.budgets["max_nodes"],
            max_edges=graph.budgets["max_edges"],
            max_sql=graph.budgets["max_sql"],
        )
        self.assertEqual(signature_before, graph_signature(rebuilt))
        self.assertFalse(rebuilt.is_observation_stale())

    def test_execution_matches_tree_cascade(self):
        graph = TreeNode[1].explain_delete()
        self._assert_graph_matches_real_delete([TreeNode[1]], graph)
        # Hooks only fire for the real delete, never for the preview
        self.assertEqual(
            sorted(graph.deleted_pks_by_entity()[TreeNode]),
            sorted((pk,) for pk in delete_hook_log),
        )

    def test_execution_matches_nullable_unlink(self):
        graph = NullParent[1].explain_delete()
        self._assert_graph_matches_real_delete([NullParent[1]], graph)

    def test_execution_matches_m2m_cleanup(self):
        # Symmetric m2m stores both directions: 3 pairs -> 6 link rows
        self.assertEqual(6, link_table_count())
        graph = Person[1].explain_delete()
        self._assert_graph_matches_real_delete([Person[1]], graph)
        # After rollback every person and all six link rows are back
        self.assertEqual(3, table_row_count(Person))
        self.assertEqual(6, link_table_count())

    def test_preview_handles_cycle_that_real_delete_cannot(self):
        t5 = TreeNode[5]
        graph = t5.explain_delete()
        self.assertTrue(graph.is_complete)
        # The read-only preview terminates and predicts both cyclic nodes
        self.assertEqual({(5,), (6,)}, graph.deleted_pks_by_entity()[TreeNode])
        # Pony's current Entity._delete_() cannot unwind a cascade cycle
        # itself; the preview must never trigger that recursive path.
        with self.assertRaises(RecursionError):
            t5.delete()
        rollback()
        db_session.__exit__()
        db_session.__enter__()
        self.assertEqual(6, table_row_count(TreeNode))

    def test_execution_matches_multi_root(self):
        a1, a2 = A[1], A[2]
        graph = a1.explain_delete(a2)
        self._assert_graph_matches_real_delete([a1, a2], graph)

    def test_execution_matches_lazy_scalar_cascade(self):
        graph = LazyHolder[1].explain_delete()
        self._assert_graph_matches_real_delete([LazyHolder[1]], graph)


if __name__ == "__main__":
    unittest.main()
