import hashlib
import inspect
import io
import unittest
from datetime import date, datetime, timedelta
from decimal import Decimal
from uuid import UUID

from pony.orm import (
    Database,
    Json,
    LongUnicode,
    Optional,
    PrimaryKey,
    Required,
    Set,
    db_session,
    flush,
    rollback,
)
from pony.orm.graph_envelope import (
    ABSENT_LAZY,
    ABSENT_UNLOADED,
    DEFAULT_MAX_DEPTH,
    EnvelopeClosureError,
    EnvelopeCorruptError,
    EnvelopeIncompleteError,
    EnvelopeLimitError,
    ExternalRef,
    FRAME_CHUNK,
    FRAME_FOOTER,
    FRAME_MANIFEST,
    FRAME_REF_INDEX,
    GraphEnvelopeExporter,
    GraphEnvelopeReader,
    MAGIC_PREFIX,
    MappingFingerprintError,
    UnknownEnvelopeTagError,
    make_footer,
    write_frame,
)
from pony.orm.serialization import Bag
from pony.orm.tests import setup_database, teardown_database


FIXED_UID = UUID("12345678-1234-5678-1234-567812345678")


def _plain(value):
    """Recursively turn frozen graph values back into plain dicts/lists."""
    from types import MappingProxyType

    if isinstance(value, MappingProxyType):
        return {key: _plain(val) for key, val in value.items()}
    if isinstance(value, tuple):
        return [_plain(item) for item in value]
    return value


class TestGraphEnvelope(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        db = cls.db = Database()

        class Group(db.Entity):
            number = PrimaryKey(int)
            students = Set("Student")

        class Course(db.Entity):
            code = PrimaryKey(str)
            title = Required(str)
            students = Set("Student")

        class Counter(db.Entity):
            id = PrimaryKey(int)
            label = Required(str)

        class Student(db.Entity):
            id = PrimaryKey(int, auto=True)
            name = Required(str)
            scholarship = Optional(int)
            gpa = Optional(Decimal)
            dob = Optional(date)
            group = Optional("Group")
            courses = Set("Course")

        class Composite(db.Entity):
            a = Required(int)
            b = Required(str)
            note = Optional(Decimal)
            PrimaryKey(a, b)
            tags = Set("Tag")

        class Tag(db.Entity):
            name = PrimaryKey(str)
            items = Set(Composite)

        class Rich(db.Entity):
            id = PrimaryKey(int)
            amount = Optional(Decimal)
            uid = Optional(UUID)
            blob = Optional(bytes)
            day = Optional(date)
            moment = Optional(datetime)
            span = Optional(timedelta)
            meta = Optional(Json)
            bio = Optional(LongUnicode)
            note = Optional(str, nullable=True)

        class Node(db.Entity):
            id = PrimaryKey(int, auto=True)
            label = Required(str)
            parent = Optional("Node", reverse="children")
            children = Set("Node", reverse="parent")

        setup_database(db)

        with db_session:
            g1 = Group(number=1)
            Group(number=2)
            c1 = Course(code="1", title="Math")
            c2 = Course(code="2", title="Physics")
            Counter(id=1, label="counter-one")
            Student(
                id=1,
                name="S1",
                gpa=Decimal("3.1"),
                group=g1,
                courses=[c1],
            )
            Student(
                id=2,
                name="S2",
                scholarship=100,
                group=g1,
                courses=[c1, c2],
            )
            Student(id=3, name="S3")
            cp = Composite(a=1, b="x", note=Decimal("9.99"))
            tag = Tag(name="red")
            cp.tags.add(tag)
            Rich(
                id=1,
                amount=Decimal("3.50"),
                uid=FIXED_UID,
                blob=b"\x00\x01\xff",
                day=date(2020, 1, 1),
                moment=datetime(2020, 1, 2, 3, 4, 5),
                span=timedelta(days=1, seconds=2, microseconds=3),
                meta={"k": [1, 2], "n": {"x": "y"}},
                bio="long biography",
                note=None,
            )
            Rich(id=2, amount=Decimal("0.5"))
            n1 = Node(label="a")
            n2 = Node(label="b")
            flush()
            n1.parent = n2
            n2.parent = n1

    @classmethod
    def tearDownClass(cls):
        teardown_database(cls.db)

    def setUp(self):
        rollback()
        db_session.__enter__()

    def tearDown(self):
        rollback()
        db_session.__exit__()

    # -- helpers -------------------------------------------------------------

    def _export(self, *objects, **kwargs):
        bag = Bag(self.db)
        configs = kwargs.pop("configs", None)
        if configs:
            for entity, cfg in configs.items():
                bag.config(entity, **cfg)
        bag.put(list(objects))
        exporter = GraphEnvelopeExporter(bag, **kwargs)
        stream = io.BytesIO()
        summary = exporter.export(stream)
        return stream.getvalue(), summary

    def _graph(self, blob, **reader_kwargs):
        return GraphEnvelopeReader(blob, **reader_kwargs).read_value_graph()

    def _snapshot(self, objects):
        snapshot = {}
        for obj in objects:
            vals = obj._vals_
            collections = {}
            for attr in obj.__class__._attrs_:
                if attr.is_collection:
                    setdata = vals.get(attr)
                    collections[attr.id] = (
                        setdata is not None,
                        None if setdata is None else bool(setdata.is_fully_loaded),
                        None if setdata is None else id(setdata),
                    )
            snapshot[id(obj)] = (
                obj._rbits_,
                tuple(sorted(attr.id for attr in vals)),
                collections,
            )
        return snapshot

    # -- tests ---------------------------------------------------------------

    def test_typed_scalars_and_null_roundtrip(self):
        rich = self.db.Rich[1]
        blob, _ = self._export(
            rich, configs={self.db.Rich: dict(with_lazy=True)}
        )
        # Unambiguous, type-preserving wire tags (no str() coercion).
        self.assertIn(b'["d","3.50"]', blob)
        self.assertIn(b'["u","12345678-1234-5678-1234-567812345678"]', blob)
        self.assertIn(b'["x","AAH/"]', blob)
        self.assertIn(b'["date","2020-01-01"]', blob)
        self.assertIn(b'["dt","2020-01-02T03:04:05"]', blob)
        self.assertIn(b'["td",[1,2,3]]', blob)

        graph = self._graph(blob)
        node = graph.get("Rich", 1)
        self.assertIsInstance(node.fields["amount"], Decimal)
        self.assertEqual(node.fields["amount"], Decimal("3.50"))
        self.assertIsInstance(node.fields["uid"], UUID)
        self.assertEqual(node.fields["uid"], FIXED_UID)
        self.assertIsInstance(node.fields["blob"], bytes)
        self.assertEqual(node.fields["blob"], b"\x00\x01\xff")
        self.assertIsInstance(node.fields["day"], date)
        self.assertIsInstance(node.fields["moment"], datetime)
        self.assertIsInstance(node.fields["span"], timedelta)
        self.assertEqual(
            node.fields["span"], timedelta(days=1, seconds=2, microseconds=3)
        )
        # Real NULL is decoded as None, not as an absent marker.
        self.assertIsNone(node.fields["note"])
        # JSON content is reconstructed as immutable plain structures.
        self.assertEqual(
            _plain(node.fields["meta"]),
            {"k": [1, 2], "n": {"x": "y"}},
        )
        with self.assertRaises(TypeError):
            node.fields["meta"]["new"] = 1

    def test_composite_primary_key_typed_identity(self):
        cp = self.db.Composite[1, "x"]
        blob, _ = self._export(cp)
        graph = self._graph(blob)
        node = graph.get("Composite", (1, "x"))
        self.assertIsNotNone(node)
        # Identity carries typed PK components, never a joined string.
        self.assertEqual(
            node.key,
            ("Composite", ("pk", (("i", 1), ("s", "x")))),
        )
        self.assertNotIn(b"1,x", blob)
        self.assertEqual(node.fields["note"], Decimal("9.99"))

    def test_shared_reference_and_cycle_topology(self):
        s1 = self.db.Student[1]
        s2 = self.db.Student[2]
        list(s1.courses)
        list(s2.courses)
        n1 = self.db.Node.get(label="a")
        n2 = self.db.Node.get(label="b")
        list(n1.children)
        list(n2.children)

        blob, summary = self._export(s1, s2, n1, n2, chunk_size=1)
        graph = self._graph(blob)

        student1 = graph.get("Student", 1)
        student2 = graph.get("Student", 2)
        course1 = graph.get("Course", "1")
        # Shared reference: the same reconstructed node, not two copies.
        c1_from_s1 = next(
            item for item in student1.collections["courses"].items
            if item.key == course1.key
        )
        c1_from_s2 = next(
            item for item in student2.collections["courses"].items
            if item.key == course1.key
        )
        self.assertIs(c1_from_s1, course1)
        self.assertIs(c1_from_s2, course1)
        self.assertEqual(graph.external_refs, frozenset())

        # Cycle: parent links form a closed loop inside the value graph.
        node_a = next(
            node for node in graph.all("Node") if node.fields["label"] == "a"
        )
        node_b = next(
            node for node in graph.all("Node") if node.fields["label"] == "b"
        )
        self.assertIs(node_a.fields["parent"], node_b)
        self.assertIs(node_b.fields["parent"], node_a)
        # Nodes are plain value objects, never live Entity instances.
        self.assertFalse(hasattr(node_a, "_vals_"))
        self.assertFalse(hasattr(node_b, "_session_cache_"))

    def test_same_text_different_type_pk_never_conflated(self):
        course = self.db.Course["1"]   # str PK "1"
        counter = self.db.Counter[1]   # int PK 1
        blob, _ = self._export(course, counter)
        graph = self._graph(blob)
        course_node = graph.get("Course", "1")
        counter_node = graph.get("Counter", 1)
        self.assertIsNotNone(course_node)
        self.assertIsNotNone(counter_node)
        self.assertIsNot(course_node, counter_node)
        self.assertNotEqual(course_node.key, counter_node.key)
        # The same textual PK appears under two distinct, type-tagged identities.
        self.assertIn(b'["s","1"]', blob)
        self.assertIn(b'["i",1]', blob)

    def test_unflushed_autopk_uses_artifact_local_temp_id(self):
        new_student = self.db.Student(name="newcomer")
        self.assertIsNone(new_student._pkval_)
        blob, summary = self._export(new_student)
        # Exporting must not force a flush.
        self.assertIsNone(new_student._pkval_)
        self.assertEqual(summary["temp_objects"], 1)
        graph = self._graph(blob)
        temp_nodes = graph.all("Student")
        self.assertEqual(len(temp_nodes), 1)
        node = temp_nodes[0]
        self.assertTrue(node.is_temp)
        self.assertEqual(node.key[1][0], "tmp")
        self.assertTrue(node.key[1][1].startswith("t"))
        self.assertEqual(
            graph.temp_node("Student", node.key[1][1]).fields["name"], "newcomer"
        )
        # A temp identity can never be confused with a typed PK identity.
        self.assertIsNone(graph.get("Student", node.key[1][1]))

    def test_absent_unloaded_vs_excluded_vs_real_null(self):
        rich = self.db.Rich[2]  # bio never loaded; note is NULL in DB
        blob, _ = self._export(
            rich,
            configs={
                self.db.Rich: dict(with_lazy=True, exclude=("amount",)),
            },
        )
        graph = self._graph(blob)
        node = graph.get("Rich", 2)
        # Unloaded lazy field -> explicit Absent marker.
        self.assertEqual(node.fields["bio"], ABSENT_LAZY)
        # Real NULL -> None.
        self.assertIsNone(node.fields["note"])
        # Excluded field -> not part of the field boundary at all.
        self.assertNotIn("amount", node.fields)
        manifest = graph.manifest
        self.assertNotIn(
            "amount", manifest["field_config"]["Rich"]["attrs"]
        )
        self.assertIn("bio", manifest["field_config"]["Rich"]["attrs"])

        # A relation missing from the in-memory values is reported as plain
        # Absent (simulating a not-yet-loaded column without touching the DB).
        student = self.db.Student[3]
        group_attr = self.db.Student.group
        self.assertIn(group_attr, student._vals_)
        del student._vals_[group_attr]
        blob2, _ = self._export(student)
        node2 = self._graph(blob2).get("Student", 3)
        self.assertEqual(node2.fields["group"], ABSENT_UNLOADED)

    def test_no_implicit_lazy_load_or_session_mutation(self):
        rich = self.db.Rich[1]
        student = self.db.Student[1]
        group = self.db.Group[1]
        empty_group = self.db.Group[2]
        objects = [rich, student, group, empty_group]
        before = self._snapshot(objects)

        sql_calls = []
        original_exec = self.db._exec_sql

        def spy(*args, **kwargs):
            sql_calls.append(args[0] if args else b"?")
            return original_exec(*args, **kwargs)

        self.db._exec_sql = spy
        try:
            blob, _ = self._export(
                rich,
                student,
                group,
                empty_group,
                configs={self.db.Rich: dict(with_lazy=True)},
                allow_lazy=False,
            )
        finally:
            self.db._exec_sql = original_exec

        # No SQL at all: no lazy loads, no flushes, no implicit table scans.
        self.assertEqual(sql_calls, [])
        # Session/lazy state is byte-for-byte unchanged.
        self.assertEqual(self._snapshot(objects), before)
        graph = self._graph(blob)
        # Loading the student seeded only the reverse SetData (partial), and
        # the exporter must not complete the collection behind our back.
        group_node = graph.get("Group", 1)
        self.assertEqual(group_node.collections["students"].state, "partial")
        self.assertFalse(group_node.collections["students"].complete)
        # A never-touched collection is an explicit Absent, not an empty set.
        empty_node = graph.get("Group", 2)
        self.assertEqual(
            empty_node.collections["students"].state, "absent"
        )
        self.assertEqual(
            empty_node.collections["students"].reason, "unloaded"
        )

    def test_collection_completeness_recorded_separately(self):
        # Partial collection: reverse-add creates SetData without a full load.
        group = self.db.Group[1]
        self.assertIsNone(group._vals_.get(type(group).students))
        extra = self.db.Student(name="latecomer", group=group)
        setdata = group._vals_.get(type(group).students)
        self.assertIsNotNone(setdata)
        self.assertFalse(setdata.is_fully_loaded)

        blob, _ = self._export(group, extra)
        graph = self._graph(blob)
        group_node = graph.get("Group", 1)
        snapshot = group_node.collections["students"]
        self.assertEqual(snapshot.state, "partial")
        self.assertFalse(snapshot.complete)
        self.assertTrue(
            any(node.is_temp for node in snapshot.items)
        )

        # Fully loaded collection reports completeness independently of items.
        full_group = self.db.Group[2]
        self.assertEqual(list(full_group.students), [])
        blob2, _ = self._export(full_group)
        full_node = self._graph(blob2).get("Group", 2)
        full_snapshot = full_node.collections["students"]
        self.assertEqual(full_snapshot.state, "complete")
        self.assertTrue(full_snapshot.complete)
        self.assertEqual(len(full_snapshot.items), 0)

    def test_chunked_streaming_forward_references_and_footer(self):
        student = self.db.Student[1]
        blob, summary = self._export(student, chunk_size=1)
        reader = GraphEnvelopeReader(blob)
        manifest = reader.read_manifest()
        self.assertEqual(manifest["format"], "pony-graph-envelope")
        # Records can be consumed incrementally, before reaching the footer.
        kinds = [kind for kind, _ in reader.iter_frames()]
        self.assertEqual(kinds[0], FRAME_CHUNK)
        self.assertEqual(kinds[-1], FRAME_FOOTER)
        self.assertIn(FRAME_REF_INDEX, kinds)
        # Chunk and reference-index sequence numbers are contiguous.
        self.assertEqual(summary["chunks"], kinds.count(FRAME_CHUNK))
        self.assertEqual(summary["ref_chunks"], kinds.count(FRAME_REF_INDEX))
        graph = self._graph(blob)
        self.assertEqual(graph.footer["complete"], True)
        self.assertEqual(graph.footer["unresolved"], [])
        # The Group may be defined after the Student chunk: forward ref works.
        self.assertIsNotNone(graph.get("Group", 1))

    def test_truncation_and_corruption_are_incomplete_graphs(self):
        blob, _ = self._export(self.db.Student[1], self.db.Student[2])
        for cut in (len(blob) - 1, len(blob) // 2, 40, 0):
            with self.assertRaises(EnvelopeIncompleteError):
                GraphEnvelopeReader(blob[:cut]).read_value_graph()

        corrupted = bytearray(blob)
        middle = len(blob) // 2
        corrupted[middle] ^= 0xFF
        with self.assertRaises(
            (EnvelopeCorruptError, EnvelopeIncompleteError, UnknownEnvelopeTagError)
        ):
            GraphEnvelopeReader(bytes(corrupted)).read_value_graph()

    def test_unknown_tag_is_rejected(self):
        stream = io.BytesIO()
        stream.write(MAGIC_PREFIX)
        hasher = hashlib.sha256()
        manifest = {
            "format": "pony-graph-envelope",
            "version": 1,
            "created_utc": "2020-01-01T00:00:00+00:00",
            "mapping_fingerprint": "0" * 64,
            "mapping": {"format": "x", "version": 1, "entities": []},
            "field_config": {
                "Thing": {
                    "attrs": ["id", "weird"],
                    "related_objects": True,
                    "collections": [],
                    "lazy_attrs": [],
                }
            },
            "pk_types": {"Thing": ["i"]},
            "limits": {
                "max_frame_bytes": 16 * 1024 * 1024,
                "max_depth": DEFAULT_MAX_DEPTH,
                "max_objects": 1000,
                "max_refs": 1000,
            },
            "allow_lazy": False,
        }
        header, payload = write_frame(stream, FRAME_MANIFEST, manifest)
        hasher.update(header)
        hasher.update(payload)
        chunk = {
            "seq": 1,
            "objects": [
                {
                    "id": {"e": "Thing", "pk": [["i", 1]]},
                    "fields": {"id": ["i", 1], "weird": ["zzz", 1]},
                    "collections": {},
                }
            ],
        }
        header, payload = write_frame(stream, FRAME_CHUNK, chunk, 1)
        hasher.update(header)
        hasher.update(payload)
        footer = make_footer(hasher.hexdigest(), 1, 0, 1, 0, 0, 0)
        write_frame(stream, FRAME_FOOTER, footer)
        with self.assertRaises(UnknownEnvelopeTagError):
            GraphEnvelopeReader(stream.getvalue()).read_value_graph()

    def test_unclosed_internal_reference_is_rejected(self):
        stream = io.BytesIO()
        stream.write(MAGIC_PREFIX)
        hasher = hashlib.sha256()
        manifest = {
            "format": "pony-graph-envelope",
            "version": 1,
            "created_utc": "2020-01-01T00:00:00+00:00",
            "mapping_fingerprint": "0" * 64,
            "mapping": {"format": "x", "version": 1, "entities": []},
            "field_config": {
                "Thing": {
                    "attrs": ["id", "peer"],
                    "related_objects": True,
                    "collections": [],
                    "lazy_attrs": [],
                }
            },
            "pk_types": {"Thing": ["i"], "Other": ["i"]},
            "limits": {
                "max_frame_bytes": 16 * 1024 * 1024,
                "max_depth": DEFAULT_MAX_DEPTH,
                "max_objects": 1000,
                "max_refs": 1000,
            },
            "allow_lazy": False,
        }
        header, payload = write_frame(stream, FRAME_MANIFEST, manifest)
        hasher.update(header)
        hasher.update(payload)
        missing = {"e": "Other", "pk": [["i", 9]]}
        thing_id = {"e": "Thing", "pk": [["i", 1]]}
        chunk = {
            "seq": 1,
            "objects": [
                {
                    "id": thing_id,
                    "fields": {
                        "id": ["i", 1],
                        "peer": ["ref", missing, False],
                    },
                    "collections": {},
                }
            ],
        }
        header, payload = write_frame(stream, FRAME_CHUNK, chunk, 1)
        hasher.update(header)
        hasher.update(payload)
        edge = {
            "from": thing_id,
            "attr": "peer",
            "to": missing,
            "collection": False,
            "external": False,
        }
        header, payload = write_frame(
            stream, FRAME_REF_INDEX, {"seq": 1, "edges": [edge]}, 1
        )
        hasher.update(header)
        hasher.update(payload)
        # Digest and counts are valid; only the reference closure is broken.
        footer = make_footer(hasher.hexdigest(), 1, 0, 1, 1, 1, 0)
        write_frame(stream, FRAME_FOOTER, footer)
        with self.assertRaises(EnvelopeClosureError):
            GraphEnvelopeReader(stream.getvalue()).read_value_graph()

    def test_mapping_fingerprint_detects_schema_change(self):
        blob, summary = self._export(self.db.Rich[1])
        # The envelope is bound to the mapping it was produced from.
        GraphEnvelopeReader(blob).verify_mapping(self.db)
        self.assertEqual(
            summary["mapping_fingerprint"],
            GraphEnvelopeReader(blob).read_manifest()["mapping_fingerprint"],
        )
        # An identical mini-schema yields the same fingerprint.
        db_a, _ = self._build_mini_database(Decimal)
        db_b, _ = self._build_mini_database(Decimal)
        from pony.orm.graph_envelope import compute_mapping_fingerprint

        self.assertEqual(
            compute_mapping_fingerprint(db_a),
            compute_mapping_fingerprint(db_b),
        )
        # Changing a column type changes the fingerprint and fails binding.
        db_c, _ = self._build_mini_database(str)
        self.assertNotEqual(
            compute_mapping_fingerprint(db_a),
            compute_mapping_fingerprint(db_c),
        )
        mini_blob = self._mini_envelope(db_b)
        with self.assertRaises(MappingFingerprintError):
            GraphEnvelopeReader(mini_blob).verify_mapping(db_c)

    def _build_mini_database(self, amount_type):
        db = Database()

        class Mini(db.Entity):
            id = PrimaryKey(int)
            amount = Optional(amount_type)

        db.bind(provider="sqlite", filename=":memory:")
        # Mapping only: the fingerprint needs no DDL or live tables.
        db.generate_mapping(check_tables=False)
        return db, Mini

    def _mini_envelope(self, database):
        from pony.orm.graph_envelope import (
            FRAME_MANIFEST,
            build_mapping_structure,
            compute_mapping_fingerprint,
        )

        stream = io.BytesIO()
        stream.write(MAGIC_PREFIX)
        manifest = {
            "format": "pony-graph-envelope",
            "version": 1,
            "created_utc": "2020-01-01T00:00:00+00:00",
            "mapping_fingerprint": compute_mapping_fingerprint(database),
            "mapping": build_mapping_structure(database),
            "field_config": {},
            "pk_types": {},
            "limits": {
                "max_frame_bytes": 16 * 1024 * 1024,
                "max_depth": DEFAULT_MAX_DEPTH,
                "max_objects": 1000,
                "max_refs": 1000,
            },
            "allow_lazy": False,
        }
        write_frame(stream, FRAME_MANIFEST, manifest)
        return stream.getvalue()

    def test_parser_enforces_limits_before_allocation(self):
        rich = self.db.Rich[1]
        blob, _ = self._export(rich)
        with self.assertRaises(EnvelopeLimitError):
            GraphEnvelopeReader(blob, max_frame_bytes=128).read_value_graph()

        nested = {"lvl": {}}
        cursor = nested["lvl"]
        for _ in range(40):
            cursor["lvl"] = {}
            cursor = cursor["lvl"]
        deep = self.db.Rich(id=100, meta=nested)
        deep_blob, _ = self._export(
            deep,
            configs={self.db.Rich: {}},
            max_depth=DEFAULT_MAX_DEPTH + 20,
        )
        with self.assertRaises(EnvelopeLimitError):
            GraphEnvelopeReader(deep_blob, max_depth=10).read_value_graph()

        stream = io.BytesIO()
        bag = Bag(self.db)
        bag.put([self.db.Student[1], self.db.Student[2]])
        with self.assertRaises(EnvelopeLimitError):
            GraphEnvelopeExporter(bag, max_objects=1).export(stream)
        # The limit-aborted stream carries no footer -> incomplete, not a graph.
        with self.assertRaises(EnvelopeIncompleteError):
            GraphEnvelopeReader(stream.getvalue()).read_value_graph()

    def test_read_only_value_graph_rejects_mutation(self):
        blob, _ = self._export(self.db.Student[1])
        graph = self._graph(blob)
        node = graph.get("Student", 1)
        with self.assertRaises(TypeError):
            node.fields = {}
        with self.assertRaises(TypeError):
            node.fields["name"] = "hacked"
        with self.assertRaises(TypeError):
            node.entity = "Other"
        snapshot = node.collections["courses"]
        self.assertIsInstance(snapshot.items, frozenset)
        with self.assertRaises(AttributeError):
            snapshot.items.add(1)

    def test_manifest_contains_no_runtime_or_executable_artifacts(self):
        blob, _ = self._export(self.db.Student[1])
        for forbidden in (
            b"_session_cache",
            b":memory:",
            b"pony.",
            b".py",
            b"permission",
            b"connection",
        ):
            self.assertNotIn(forbidden, blob)
        source = inspect.getsource(inspect.getmodule(GraphEnvelopeExporter))
        # The pickle machinery is never imported or invoked.
        self.assertNotIn("import pickle", source)
        self.assertNotIn("pickle.", source)

    def test_external_boundary_reference_keeps_closure_valid(self):
        student = self.db.Student[1]
        blob, summary = self._export(
            student,
            configs={self.db.Student: dict(related_objects=False)},
        )
        graph = self._graph(blob)
        self.assertEqual(summary["external_refs"], 1)
        self.assertEqual(len(graph.external_refs), 1)
        external = next(iter(graph.external_refs))
        self.assertIsInstance(external, ExternalRef)
        self.assertEqual(external.entity, "Group")
        # The boundary target has no definition, but the graph still closes.
        self.assertIsNone(graph.get("Group", 1))
        self.assertEqual(graph.footer["unresolved"], [])
        node = graph.get("Student", 1)
        self.assertIsInstance(node.fields["group"], ExternalRef)

    def test_allow_lazy_permission_gate(self):
        rich = self.db.Rich[2]
        denied_blob, _ = self._export(
            rich, configs={self.db.Rich: dict(with_lazy=True)}, allow_lazy=False
        )
        denied_node = self._graph(denied_blob).get("Rich", 2)
        self.assertEqual(denied_node.fields["bio"], ABSENT_LAZY)
        rbits_before = rich._rbits_
        allowed_blob, _ = self._export(
            rich, configs={self.db.Rich: dict(with_lazy=True)}, allow_lazy=True
        )
        allowed_node = self._graph(allowed_blob).get("Rich", 2)
        # With permission the lazy field is actually loaded: an explicit
        # string value here, instead of the Absent marker.
        self.assertEqual(allowed_node.fields["bio"], "")
        self.assertNotEqual(rich._rbits_, rbits_before)


if __name__ == "__main__":
    unittest.main()
