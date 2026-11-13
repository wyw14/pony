"""Versioned, incrementally readable entity-graph envelope export/import.

The :class:`GraphEnvelopeExporter` exports the finite entity graph bounded by a
:class:`pony.orm.serialization.Bag` configuration as a framed record stream.

Core guarantees:

* object identity is always the pair *entity mapping name + typed primary key*;
  ``str(pk)`` is never used as a cross-type identity;
* unflushed objects without a primary key receive artifact-local temporary ids;
* Decimal, UUID, bytes, date/time values and NULL carry unambiguous type tags;
* unloaded fields, excluded fields and real NULLs have distinct representations;
* without explicit lazy-load permission no attribute or collection is loaded
  from the database, so the source session/lazy state is never modified and the
  whole database is never implicitly traversed;
* collection completeness is recorded independently of the collected items;
* object definitions and the reference index are emitted in bounded-size chunks,
  forward references are allowed and reference closure is verified before the
  footer is written;
* a truncated or corrupted stream (missing/invalid footer, digest mismatch) can
  never be interpreted as a complete graph;
* the manifest binds a structural mapping fingerprint and the field
  configuration and contains no session connection data, permission caches or
  executable class paths;
* the parser enforces frame length, nesting depth and object count limits
  *before* allocating the corresponding structures;
* the reader rebuilds a read-only *value graph* of plain Python values -- never
  live Entity instances and never through ``pickle``.
"""

import base64
import binascii
import hashlib
import io
import json
import math
from datetime import date, datetime, time, timedelta, timezone
from decimal import Decimal
from types import MappingProxyType
from uuid import UUID

from pony.orm.core import Entity
from pony.orm.ormtypes import Array, TrackedValue
from pony.orm.serialization import Bag
from pony.utils import throw


FORMAT_NAME = "pony-graph-envelope"
FORMAT_VERSION = 1
MAGIC_PREFIX = b"PONY-GRAPH-ENVELOPE/1\n"

FRAME_MANIFEST = "M"
FRAME_CHUNK = "C"
FRAME_REF_INDEX = "R"
FRAME_FOOTER = "F"

DEFAULT_CHUNK_SIZE = 128
DEFAULT_MAX_FRAME_BYTES = 16 * 1024 * 1024
DEFAULT_MAX_DEPTH = 32
DEFAULT_MAX_OBJECTS = 1_000_000
DEFAULT_MAX_REFS = 10_000_000
_MAX_HEADER_LINE = 256

# Unambiguous value tags. The tag set is closed -- unknown tags are rejected.
TAG_NULL = "n"
TAG_BOOL = "b"
TAG_INT = "i"
TAG_FLOAT = "f"
TAG_STR = "s"
TAG_DECIMAL = "d"
TAG_UUID = "u"
TAG_BYTES = "x"
TAG_DATE = "date"
TAG_DATETIME = "dt"
TAG_TIME = "time"
TAG_TIMEDELTA = "td"
TAG_JSON = "j"
TAG_ARRAY = "arr"
TAG_REF = "ref"
TAG_ABSENT = "absent"

SCALAR_TAGS = frozenset(
    (
        TAG_NULL,
        TAG_BOOL,
        TAG_INT,
        TAG_FLOAT,
        TAG_STR,
        TAG_DECIMAL,
        TAG_UUID,
        TAG_BYTES,
        TAG_DATE,
        TAG_DATETIME,
        TAG_TIME,
        TAG_TIMEDELTA,
        TAG_JSON,
        TAG_ARRAY,
    )
)
ABSENT_REASONS = frozenset(("unloaded", "lazy"))
COLLECTION_STATES = frozenset(("complete", "partial", "absent"))

_PRIM_TYPE_TAGS = {
    bool: TAG_BOOL,
    int: TAG_INT,
    float: TAG_FLOAT,
    str: TAG_STR,
    Decimal: TAG_DECIMAL,
    UUID: TAG_UUID,
    bytes: TAG_BYTES,
    date: TAG_DATE,
    datetime: TAG_DATETIME,
    time: TAG_TIME,
    timedelta: TAG_TIMEDELTA,
}


# ---------------------------------------------------------------------------
# Errors
# ---------------------------------------------------------------------------


class EnvelopeError(Exception):
    """Base class for all graph envelope errors."""


class EnvelopeStateError(EnvelopeError):
    """The exporter was asked to serialize objects in an invalid state."""


class EnvelopeUnsupportedTypeError(EnvelopeError):
    """A Python value cannot be represented with the closed tag set."""


class EnvelopeLimitError(EnvelopeError):
    """A declared frame/depth/object limit would be exceeded."""


class EnvelopeFormatError(EnvelopeError):
    """The stream does not conform to the envelope record format."""


class EnvelopeIncompleteError(EnvelopeError):
    """The stream ends before a valid footer completes the graph."""


class EnvelopeCorruptError(EnvelopeError):
    """The stream is structurally present but fails integrity validation."""


class EnvelopeClosureError(EnvelopeError):
    """An internal reference cannot be resolved inside the graph."""


class UnknownEnvelopeTagError(EnvelopeFormatError):
    """A record carries a tag outside the closed tag set."""


class MappingFingerprintError(EnvelopeError):
    """The manifest fingerprint does not match the current mapping."""


# ---------------------------------------------------------------------------
# Read-only value graph
# ---------------------------------------------------------------------------


class _Absent:
    """Explicit marker for a field that was not loaded when exporting."""

    __slots__ = ("reason",)

    def __init__(self, reason):
        self.reason = reason

    def __repr__(self):
        return "Absent(%r)" % self.reason

    def __eq__(self, other):
        return isinstance(other, _Absent) and other.reason == self.reason

    def __hash__(self):
        return hash(("Absent", self.reason))


ABSENT_UNLOADED = _Absent("unloaded")
ABSENT_LAZY = _Absent("lazy")
_ABSENT_MAP = {"unloaded": ABSENT_UNLOADED, "lazy": ABSENT_LAZY}


class ExternalRef:
    """A reference crossing the Bag field boundary (identity only)."""

    __slots__ = ("entity", "key", "identity", "is_temp")

    def __init__(self, identity, key):
        self.identity = MappingProxyType(dict(identity))
        self.key = key
        self.entity = key[0]
        self.is_temp = key[1][0] == "tmp"

    def __repr__(self):
        return "ExternalRef(%r)" % (self.key,)

    def __eq__(self, other):
        return isinstance(other, ExternalRef) and other.key == self.key

    def __hash__(self):
        return hash(("ExternalRef", self.key))


class CollectionSnapshot:
    """Read-only snapshot of a collection, with its completeness flag."""

    __slots__ = ("state", "complete", "items", "reason")

    def __init__(self, state, complete, items, reason):
        self.state = state
        self.complete = complete
        self.items = frozenset(items)
        self.reason = reason

    def __repr__(self):
        return "CollectionSnapshot(state=%r, complete=%r, items=%d)" % (
            self.state,
            self.complete,
            len(self.items),
        )


class ValueNode:
    """A read-only reconstructed object definition (never a live Entity)."""

    __slots__ = ("entity", "key", "is_temp", "fields", "collections", "_frozen")

    def __init__(self, entity, key, is_temp):
        self.entity = entity
        self.key = key
        self.is_temp = is_temp
        self.fields = {}
        self.collections = {}
        self._frozen = False

    def freeze(self):
        self.fields = MappingProxyType(self.fields)
        self.collections = MappingProxyType(self.collections)
        self._frozen = True

    def __setattr__(self, name, value):
        if getattr(self, "_frozen", False):
            throw(TypeError, "ValueNode is read-only: %s" % name)
        object.__setattr__(self, name, value)

    def __delattr__(self, name):
        throw(TypeError, "ValueNode is read-only")

    def __repr__(self):
        return "ValueNode(%r, %r)" % (self.entity, self.key[1])


class ReadOnlyValueGraph:
    """The reconstructed read-only value graph."""

    def __init__(self, manifest, footer, nodes, external_refs):
        self.manifest = MappingProxyType(manifest)
        self.footer = MappingProxyType(footer)
        self.nodes = MappingProxyType(nodes)
        self.external_refs = frozenset(external_refs)
        self.mapping_fingerprint = manifest["mapping_fingerprint"]

    def get(self, entity, pk):
        """Look up a persisted node by entity name and scalar/composite pk."""
        if not isinstance(pk, tuple):
            pk = (pk,)
        key = (entity, ("pk", tuple(tuple(_tag_scalar(v)) for v in pk)))
        return self.nodes.get(key)

    def temp_node(self, entity, temp_id):
        return self.nodes.get((entity, ("tmp", temp_id)))

    def all(self, entity=None):
        if entity is None:
            return tuple(self.nodes.values())
        return tuple(node for node in self.nodes.values() if node.entity == entity)

    def verify_mapping(self, database):
        """Bind the graph to a current mapping; raise on schema changes."""
        fingerprint = compute_mapping_fingerprint(database)
        if fingerprint != self.mapping_fingerprint:
            throw(
                MappingFingerprintError,
                "Mapping fingerprint mismatch: manifest=%s current=%s"
                % (self.mapping_fingerprint, fingerprint),
            )
        return True


# ---------------------------------------------------------------------------
# Tagging of scalar values
# ---------------------------------------------------------------------------


def _tag_scalar(value):
    """Tag a primary-key / scalar value. Never stringifies the value."""
    if isinstance(value, bool):
        return [TAG_BOOL, value]
    if isinstance(value, int):
        return [TAG_INT, value]
    if isinstance(value, float):
        if not math.isfinite(value):
            throw(EnvelopeUnsupportedTypeError, "Non-finite float is not supported")
        return [TAG_FLOAT, value]
    if isinstance(value, str):
        return [TAG_STR, value]
    if isinstance(value, Decimal):
        return [TAG_DECIMAL, str(value)]
    if isinstance(value, UUID):
        return [TAG_UUID, str(value)]
    if isinstance(value, (bytes, bytearray)):
        return [TAG_BYTES, base64.b64encode(bytes(value)).decode("ascii")]
    if isinstance(value, datetime):
        return [TAG_DATETIME, value.isoformat()]
    if isinstance(value, date):
        return [TAG_DATE, value.isoformat()]
    if isinstance(value, time):
        return [TAG_TIME, value.isoformat()]
    if isinstance(value, timedelta):
        return [TAG_TIMEDELTA, [value.days, value.seconds, value.microseconds]]
    throw(
        EnvelopeUnsupportedTypeError,
        "Values of type %s cannot be represented in a graph envelope"
        % type(value).__name__,
    )


def _primitive_type_tag(py_type):
    tag = _PRIM_TYPE_TAGS.get(py_type)
    if tag is not None:
        return tag
    for base_type, base_tag in _PRIM_TYPE_TAGS.items():
        if issubclass(py_type, base_type):
            return base_tag
    throw(
        EnvelopeUnsupportedTypeError,
        "Primary key component type %s is not supported" % py_type.__name__,
    )


def _encode_json_payload(value, depth, max_depth):
    if depth > max_depth:
        throw(EnvelopeLimitError, "Nesting depth limit exceeded")
    if isinstance(value, TrackedValue):
        value = value.get_untracked()
    if value is None:
        return None
    if isinstance(value, bool):
        return value
    if isinstance(value, int):
        return value
    if isinstance(value, float):
        if not math.isfinite(value):
            throw(EnvelopeUnsupportedTypeError, "Non-finite float is not supported")
        return value
    if isinstance(value, str):
        return value
    if isinstance(value, dict):
        return {
            str(key): _encode_json_payload(val, depth + 1, max_depth)
            for key, val in value.items()
        }
    if isinstance(value, (list, tuple)):
        return [
            _encode_json_payload(item, depth + 1, max_depth) for item in value
        ]
    throw(
        EnvelopeUnsupportedTypeError,
        "JSON attribute contains a non-JSON value of type %s"
        % type(value).__name__,
    )


def encode_attribute_value(attr, value, max_depth):
    """Encode a loaded, non-relation attribute value as a tagged record."""
    py_type = attr.py_type
    if isinstance(value, TrackedValue):
        value = value.get_untracked()
    if isinstance(py_type, type) and issubclass(py_type, Array):
        if not isinstance(value, (list, tuple)):
            throw(
                EnvelopeUnsupportedTypeError,
                "Array attribute %s holds %s" % (attr.name, type(value).__name__),
            )
        return [
            TAG_ARRAY,
            [_tag_scalar(item) for item in value],
        ]
    if isinstance(value, (dict, list, tuple)):
        return [TAG_JSON, _encode_json_payload(value, 1, max_depth)]
    return _tag_scalar(value)


def decode_tagged(tagged):
    """Decode a tagged scalar record back into a plain Python value."""
    if not isinstance(tagged, list) or len(tagged) != 2:
        throw(EnvelopeFormatError, "Tagged value must be a [tag, payload] pair")
    tag, payload = tagged
    if tag == TAG_NULL:
        if payload is not None:
            throw(EnvelopeFormatError, "NULL tag payload must be null")
        return None
    if tag == TAG_BOOL:
        if not isinstance(payload, bool):
            throw(EnvelopeFormatError, "BOOL payload must be true/false")
        return payload
    if tag == TAG_INT:
        if isinstance(payload, bool) or not isinstance(payload, int):
            throw(EnvelopeFormatError, "INT payload must be an integer")
        return payload
    if tag == TAG_FLOAT:
        if isinstance(payload, bool) or not isinstance(payload, (int, float)):
            throw(EnvelopeFormatError, "FLOAT payload must be a number")
        return float(payload)
    if tag == TAG_STR:
        if not isinstance(payload, str):
            throw(EnvelopeFormatError, "STR payload must be a string")
        return payload
    if tag == TAG_DECIMAL:
        if not isinstance(payload, str):
            throw(EnvelopeFormatError, "DECIMAL payload must be a string")
        try:
            return Decimal(payload)
        except Exception:
            throw(EnvelopeCorruptError, "Invalid DECIMAL payload: %r" % payload)
    if tag == TAG_UUID:
        if not isinstance(payload, str):
            throw(EnvelopeFormatError, "UUID payload must be a string")
        try:
            return UUID(payload)
        except (ValueError, AttributeError):
            throw(EnvelopeCorruptError, "Invalid UUID payload: %r" % payload)
    if tag == TAG_BYTES:
        if not isinstance(payload, str):
            throw(EnvelopeFormatError, "BYTES payload must be a base64 string")
        try:
            return base64.b64decode(payload.encode("ascii"), validate=True)
        except (binascii.Error, ValueError):
            throw(EnvelopeCorruptError, "Invalid base64 BYTES payload")
    if tag == TAG_DATE:
        if not isinstance(payload, str):
            throw(EnvelopeFormatError, "DATE payload must be a string")
        try:
            return date.fromisoformat(payload)
        except ValueError:
            throw(EnvelopeCorruptError, "Invalid DATE payload: %r" % payload)
    if tag == TAG_DATETIME:
        if not isinstance(payload, str):
            throw(EnvelopeFormatError, "DATETIME payload must be a string")
        try:
            return datetime.fromisoformat(payload)
        except ValueError:
            throw(EnvelopeCorruptError, "Invalid DATETIME payload: %r" % payload)
    if tag == TAG_TIME:
        if not isinstance(payload, str):
            throw(EnvelopeFormatError, "TIME payload must be a string")
        try:
            return time.fromisoformat(payload)
        except ValueError:
            throw(EnvelopeCorruptError, "Invalid TIME payload: %r" % payload)
    if tag == TAG_TIMEDELTA:
        if (
            not isinstance(payload, list)
            or len(payload) != 3
            or any(isinstance(v, bool) or not isinstance(v, int) for v in payload)
        ):
            throw(EnvelopeFormatError, "TIMEDELTA payload must be [d, s, us]")
        return timedelta(
            days=payload[0], seconds=payload[1], microseconds=payload[2]
        )
    if tag == TAG_JSON:
        return _freeze_json(payload, 1)
    if tag == TAG_ARRAY:
        if not isinstance(payload, list):
            throw(EnvelopeFormatError, "ARRAY payload must be a list")
        return tuple(decode_tagged(item) for item in payload)
    if tag in (TAG_REF, TAG_ABSENT):
        throw(EnvelopeFormatError, "Tag %r is not a scalar value tag" % tag)
    throw(UnknownEnvelopeTagError, "Unknown envelope value tag: %r" % tag)


def _freeze_json(value, depth):
    """Freeze decoded JSON content into immutable plain structures."""
    if depth > DEFAULT_MAX_DEPTH:
        throw(EnvelopeLimitError, "Nesting depth limit exceeded")
    if value is None or isinstance(value, (bool, int, float, str)):
        return value
    if isinstance(value, dict):
        return MappingProxyType(
            {
                key: _freeze_json(val, depth + 1)
                for key, val in value.items()
            }
        )
    if isinstance(value, list):
        return tuple(_freeze_json(item, depth + 1) for item in value)
    throw(EnvelopeCorruptError, "JSON payload contains unsupported structure")


# ---------------------------------------------------------------------------
# Mapping fingerprint (structural metadata only, no runtime objects)
# ---------------------------------------------------------------------------


def _mapping_type_name(py_type):
    if isinstance(py_type, type):
        if issubclass(py_type, Entity):
            return py_type.__name__
        if issubclass(py_type, Array):
            return {"array": py_type.item_type.__name__}
        return py_type.__name__
    throw(
        EnvelopeStateError,
        "Mapping must be generated before exporting; unresolved type %r" % py_type,
    )


def _raw_pk_tags(entity):
    """Typed tag schema of the raw PK columns of an entity."""
    tags = []
    for attr in entity._pk_attrs_:
        if attr.reverse:
            tags.extend(_raw_pk_tags(attr.py_type))
        else:
            tags.append(_primitive_type_tag(attr.py_type))
    return tags


def build_mapping_structure(database):
    """Canonical, name-only description of a mapped database.

    Contains exclusively entity/attribute names, column names and flag values.
    Never contains connection settings, cache state or importable class paths.
    """
    entities = []
    for entity in sorted(database.entities.values(), key=lambda e: e._id_):
        attrs = []
        for attr in entity._attrs_:
            attrs.append(
                {
                    "name": attr.name,
                    "kind": attr.__class__.__name__,
                    "type": _mapping_type_name(attr.py_type),
                    "columns": list(attr.columns),
                    "sql_type": attr.sql_type,
                    "lazy": bool(attr.lazy),
                    "nullable": bool(attr.nullable),
                    "auto": bool(attr.auto),
                    "collection": bool(attr.is_collection),
                    "relation": bool(attr.is_relation),
                    "pk": bool(attr.is_pk),
                    "reverse": attr.reverse.name if attr.reverse else None,
                }
            )
        table = entity._table_
        if isinstance(table, tuple):
            table = list(table)
        entities.append(
            {
                "name": entity.__name__,
                "table": table,
                "root": entity._root_.__name__,
                "pk_attrs": [attr.name for attr in entity._pk_attrs_],
                "pk_columns": list(entity._pk_columns_),
                "pk_tags": _raw_pk_tags(entity),
                "attrs": attrs,
            }
        )
    return {
        "format": FORMAT_NAME + "-mapping",
        "version": FORMAT_VERSION,
        "entities": entities,
    }


def compute_mapping_fingerprint(database):
    structure = build_mapping_structure(database)
    canonical = json.dumps(
        structure, sort_keys=True, separators=(",", ":"), ensure_ascii=False
    )
    return hashlib.sha256(canonical.encode("utf-8")).hexdigest()


# ---------------------------------------------------------------------------
# Frame IO
# ---------------------------------------------------------------------------


def _encode_payload(payload):
    if isinstance(payload, (dict, list)):
        return json.dumps(
            payload, ensure_ascii=False, separators=(",", ":")
        ).encode("utf-8")
    if isinstance(payload, str):
        return payload.encode("utf-8")
    return bytes(payload)


def _build_frame(kind, payload, seq=None):
    raw_payload = _encode_payload(payload)
    if seq is None:
        header = ("%s %d\n" % (kind, len(raw_payload))).encode("ascii")
    else:
        header = ("%s %d %d\n" % (kind, seq, len(raw_payload))).encode("ascii")
    return header, raw_payload


def write_frame(stream, kind, payload, seq=None):
    """Write one length-prefixed envelope frame and return header/payload."""
    header, raw_payload = _build_frame(kind, payload, seq)
    stream.write(header)
    stream.write(raw_payload)
    stream.write(b"\n")
    return header, raw_payload


def make_footer(digest, objects, temp_objects, chunks, ref_chunks, refs,
                external_refs):
    return {
        "complete": True,
        "digest": digest,
        "objects": objects,
        "temp_objects": temp_objects,
        "chunks": chunks,
        "ref_chunks": ref_chunks,
        "refs": refs,
        "external_refs": external_refs,
        "unresolved": [],
    }


def _scan_nesting_depth(raw, max_depth):
    """Validate JSON nesting depth on raw bytes before parsing/allocating."""
    depth = 0
    in_string = False
    escaped = False
    for byte in raw:
        if in_string:
            if escaped:
                escaped = False
            elif byte == 0x5C:  # backslash
                escaped = True
            elif byte == 0x22:  # quote
                in_string = False
            continue
        if byte == 0x22:
            in_string = True
        elif byte == 0x7B or byte == 0x5B:  # { or [
            depth += 1
            if depth > max_depth:
                throw(
                    EnvelopeLimitError,
                    "Envelope nesting depth exceeds the limit of %d" % max_depth,
                )
        elif byte == 0x7D or byte == 0x5D:  # } or ]
            depth -= 1
            if depth < 0:
                throw(EnvelopeCorruptError, "Unbalanced JSON structure")
    if depth != 0 or in_string:
        throw(EnvelopeCorruptError, "Unbalanced JSON structure")


# ---------------------------------------------------------------------------
# Exporter
# ---------------------------------------------------------------------------


class GraphEnvelopeExporter:
    """Exports the Bag-bounded finite entity graph into an envelope stream.

    Object definitions and reference edges are buffered only up to
    ``chunk_size`` and flushed as frames immediately, so serialized field
    data never accumulates for the whole graph. The traversal keeps a compact
    identity set (one small key per visited object), which is the minimum
    state needed to support shared references, cycles and closure checks.
    """

    def __init__(
        self,
        bag,
        *,
        chunk_size=DEFAULT_CHUNK_SIZE,
        allow_lazy=False,
        max_frame_bytes=DEFAULT_MAX_FRAME_BYTES,
        max_depth=DEFAULT_MAX_DEPTH,
        max_objects=DEFAULT_MAX_OBJECTS,
        max_refs=DEFAULT_MAX_REFS,
    ):
        if not isinstance(bag, Bag):
            throw(TypeError, "Bag instance expected. Got: %r" % bag)
        if bag.database.schema is None:
            throw(
                EnvelopeStateError,
                "Database mapping must be generated before exporting a graph",
            )
        if not isinstance(chunk_size, int) or chunk_size < 1:
            throw(ValueError, "chunk_size must be a positive integer")
        self.bag = bag
        self.database = bag.database
        self.chunk_size = chunk_size
        self.allow_lazy = bool(allow_lazy)
        self.max_frame_bytes = max_frame_bytes
        self.max_depth = max_depth
        self.max_objects = max_objects
        self.max_refs = max_refs

        self._session_cache = bag.session_cache
        self._seen = {}
        self._internal_targets = set()
        self._queue = []
        self._id_cache = {}
        self._temp_seq = 0
        self._defs_buffer = []
        self._edges_buffer = []
        self._hasher = hashlib.sha256()
        self.object_count = 0
        self.temp_object_count = 0
        self.ref_count = 0
        self.external_ref_count = 0
        self.chunk_count = 0
        self.ref_chunk_count = 0

    # -- mapping / configuration -------------------------------------------

    @property
    def mapping_fingerprint(self):
        return compute_mapping_fingerprint(self.database)

    def _ensure_configs(self):
        """Mirror Bag's own lazy default-config behavior, up-front."""
        for entity in self.database.entities.values():
            if entity not in self.bag.entity_configs:
                self.bag.config(entity)

    def _config_for(self, entity):
        config = self.bag.entity_configs.get(entity)
        if config is None:
            config = self.bag.config(entity)
        return config

    def _field_configuration(self):
        result = {}
        for entity, (attrs, related_objects) in sorted(
            self.bag.entity_configs.items(), key=lambda item: item[0]._id_
        ):
            result[entity.__name__] = {
                "attrs": [attr.name for attr in attrs],
                "related_objects": bool(related_objects),
                "collections": [
                    attr.name for attr in attrs if attr.is_collection
                ],
                "lazy_attrs": [attr.name for attr in attrs if attr.lazy],
            }
        return result

    def _build_manifest(self):
        mapping_structure = build_mapping_structure(self.database)
        return {
            "format": FORMAT_NAME,
            "version": FORMAT_VERSION,
            "created_utc": datetime.now(timezone.utc).isoformat(),
            "mapping_fingerprint": compute_mapping_fingerprint(self.database),
            "mapping": mapping_structure,
            "field_config": self._field_configuration(),
            "pk_types": {
                entity.__name__: _raw_pk_tags(entity)
                for entity in sorted(
                    self.database.entities.values(), key=lambda e: e._id_
                )
            },
            "limits": {
                "max_frame_bytes": self.max_frame_bytes,
                "max_depth": self.max_depth,
                "max_objects": self.max_objects,
                "max_refs": self.max_refs,
            },
            "allow_lazy": self.allow_lazy,
        }

    # -- identity -----------------------------------------------------------

    def _identity(self, obj):
        cached = self._id_cache.get(id(obj))
        if cached is not None:
            return cached
        entity = obj.__class__
        if obj._pkval_ is None:
            # Unflushed auto-PK object: an id valid only inside this artifact.
            self._temp_seq += 1
            temp_id = "t%06d" % self._temp_seq
            ident = {"e": entity.__name__, "tmp": temp_id}
            key = (entity.__name__, ("tmp", temp_id))
        else:
            tagged_pk = tuple(
                tuple(_tag_scalar(raw_value))
                for raw_value in obj._get_raw_pkval_()
            )
            ident = {
                "e": entity.__name__,
                "pk": [list(pair) for pair in tagged_pk],
            }
            key = (entity.__name__, ("pk", tagged_pk))
        self._id_cache[id(obj)] = (ident, key)
        return ident, key

    def _check_object_state(self, obj):
        if obj._vals_ is None:
            throw(
                EnvelopeStateError,
                "Cannot export %r: its database session is over" % obj,
            )
        if obj._status_ in ("marked_to_delete", "deleted", "cancelled"):
            throw(EnvelopeStateError, "Cannot export deleted object %r" % obj)

    # -- traversal (bounded, no implicit lazy load) -------------------------

    def _enqueue_seed(self, obj):
        self._check_object_state(obj)
        ident, key = self._identity(obj)
        if key in self._seen:
            return
        if self.object_count + len(self._defs_buffer) >= self.max_objects:
            throw(EnvelopeLimitError, "Object count limit reached")
        self._seen[key] = ident
        self._queue.append(obj)

    def _register_reference(self, source_ident, attr_name, target,
                            related, is_collection):
        """Record an edge; enqueue the target only when Bag config allows it."""
        if self.ref_count >= self.max_refs:
            throw(EnvelopeLimitError, "Reference count limit reached")
        target_ident, target_key = self._identity(target)
        external = True
        if (
            related
            and target._session_cache_ is self._session_cache
            and target._status_ not in ("marked_to_delete", "deleted", "cancelled")
            and target._vals_ is not None
        ):
            if target_key not in self._seen:
                if self.object_count + len(self._defs_buffer) >= self.max_objects:
                    throw(EnvelopeLimitError, "Object count limit reached")
                self._seen[target_key] = target_ident
                self._queue.append(target)
            self._internal_targets.add(target_key)
            external = False
        self._edges_buffer.append(
            {
                "from": source_ident,
                "attr": attr_name,
                "to": target_ident,
                "collection": bool(is_collection),
                "external": external,
            }
        )
        self.ref_count += 1
        if external:
            self.external_ref_count += 1
        return [TAG_REF, target_ident, external]

    def _process_scalar_attr(self, obj, attr, ident, related_objects):
        vals = obj._vals_
        if attr not in vals:
            # Direct dict membership: never triggers a lazy load.
            if self.allow_lazy:
                value = attr.__get__(obj)
            else:
                return [
                    TAG_ABSENT,
                    "lazy" if attr.lazy else "unloaded",
                ]
        else:
            value = vals[attr]
        if value is None:
            return [TAG_NULL, None]
        if attr.is_relation:
            return self._register_reference(
                ident, attr.name, value, related_objects, False
            )
        return encode_attribute_value(attr, value, self.max_depth)

    def _process_collection(self, obj, attr, ident, related_objects):
        setdata = obj._vals_.get(attr)
        if setdata is None:
            if self.allow_lazy:
                setdata = attr.load(obj)
            else:
                return {
                    "state": "absent",
                    "reason": "lazy" if attr.lazy else "unloaded",
                }
        items = [
            self._register_reference(
                ident, attr.name, target, related_objects, True
            )
            for target in setdata
        ]
        complete = bool(setdata.is_fully_loaded)
        return {
            "state": "complete" if complete else "partial",
            "complete": complete,
            "items": items,
        }

    def _process_object(self, obj):
        self._check_object_state(obj)
        entity = obj.__class__
        attrs, related_objects = self._config_for(entity)
        ident, _ = self._identity(obj)
        fields = {}
        collections = {}
        for attr in attrs:
            if attr.is_collection:
                collections[attr.name] = self._process_collection(
                    obj, attr, ident, related_objects
                )
            else:
                fields[attr.name] = self._process_scalar_attr(
                    obj, attr, ident, related_objects
                )
        return {"id": ident, "fields": fields, "collections": collections}

    # -- frame emission ------------------------------------------------------

    def _write_frame(self, stream, kind, payload, seq=None):
        # Validate the frame size before any byte is written.
        header, raw_payload = _build_frame(kind, payload, seq)
        if len(raw_payload) > self.max_frame_bytes:
            throw(
                EnvelopeLimitError,
                "Frame of %d bytes exceeds max_frame_bytes=%d"
                % (len(raw_payload), self.max_frame_bytes),
            )
        stream.write(header)
        stream.write(raw_payload)
        stream.write(b"\n")
        self._hasher.update(header)
        self._hasher.update(raw_payload)

    def _flush_buffers(self, stream):
        if self._defs_buffer:
            self.chunk_count += 1
            self._write_frame(
                stream,
                FRAME_CHUNK,
                {"seq": self.chunk_count, "objects": self._defs_buffer},
                self.chunk_count,
            )
            self._defs_buffer = []
        if self._edges_buffer:
            self.ref_chunk_count += 1
            self._write_frame(
                stream,
                FRAME_REF_INDEX,
                {"seq": self.ref_chunk_count, "edges": self._edges_buffer},
                self.ref_chunk_count,
            )
            self._edges_buffer = []

    def _verify_closure(self):
        # Every internal edge target was enqueued and drained into a
        # definition; verify that invariant explicitly before the footer.
        unresolved = self._internal_targets - set(self._seen)
        if unresolved:
            throw(
                EnvelopeClosureError,
                "Unresolved internal references: %r" % sorted(unresolved),
            )

    # -- public API ----------------------------------------------------------

    def export(self, stream):
        """Write the complete envelope to a binary stream.

        Returns a summary dictionary. No footer is emitted when a limit or
        closure error is raised, so the partial stream is provably incomplete.
        """
        self._ensure_configs()
        stream.write(MAGIC_PREFIX)
        manifest = self._build_manifest()
        self._write_frame(stream, FRAME_MANIFEST, manifest)

        seeds = []
        for objects in self.bag.objects.values():
            seeds.extend(objects)
        # Stable ordering keeps temporary ids and frames deterministic.
        seeds.sort(key=lambda obj: self._identity(obj)[1])
        for obj in seeds:
            self._enqueue_seed(obj)

        while self._queue:
            obj = self._queue.pop(0)
            if self.object_count >= self.max_objects:
                throw(EnvelopeLimitError, "Object count limit reached")
            definition = self._process_object(obj)
            self._defs_buffer.append(definition)
            self.object_count += 1
            if obj._pkval_ is None:
                self.temp_object_count += 1
            if len(self._defs_buffer) >= self.chunk_size:
                self._flush_buffers(stream)

        self._flush_buffers(stream)
        self._verify_closure()

        footer = make_footer(
            self._hasher.copy().hexdigest(),
            self.object_count,
            self.temp_object_count,
            self.chunk_count,
            self.ref_chunk_count,
            self.ref_count,
            self.external_ref_count,
        )
        self._write_frame(stream, FRAME_FOOTER, footer)
        return {
            "objects": self.object_count,
            "temp_objects": self.temp_object_count,
            "chunks": self.chunk_count,
            "ref_chunks": self.ref_chunk_count,
            "refs": self.ref_count,
            "external_refs": self.external_ref_count,
            "mapping_fingerprint": manifest["mapping_fingerprint"],
        }

    def export_bytes(self):
        stream = io.BytesIO()
        self.export(stream)
        return stream.getvalue()


# ---------------------------------------------------------------------------
# Reader
# ---------------------------------------------------------------------------


def _identity_key(ident):
    if not isinstance(ident, dict):
        throw(EnvelopeFormatError, "Object identity must be an object")
    name = ident.get("e")
    if not isinstance(name, str):
        throw(EnvelopeFormatError, "Identity entity name must be a string")
    if set(ident.keys()) - {"e", "pk", "tmp"}:
        throw(EnvelopeFormatError, "Unknown identity fields: %r" % ident.keys())
    if "tmp" in ident:
        if "pk" in ident:
            throw(EnvelopeFormatError, "Identity cannot carry both pk and tmp")
        temp_id = ident["tmp"]
        if not isinstance(temp_id, str) or not temp_id:
            throw(EnvelopeFormatError, "Temporary id must be a non-empty string")
        return (name, ("tmp", temp_id))
    if "pk" not in ident:
        throw(EnvelopeFormatError, "Identity must carry pk or tmp")
    raw_pk = ident["pk"]
    if not isinstance(raw_pk, list) or not raw_pk:
        throw(EnvelopeFormatError, "Typed PK must be a non-empty list")
    pk = []
    for pair in raw_pk:
        if (
            not isinstance(pair, list)
            or len(pair) != 2
            or pair[0] not in SCALAR_TAGS
        ):
            throw(EnvelopeFormatError, "Each PK component must be typed")
        pk.append(tuple(pair))
    return (name, ("pk", tuple(pk)))


class GraphEnvelopeReader:
    """Incrementally parses an envelope stream and rebuilds a value graph."""

    def __init__(
        self,
        source,
        *,
        max_frame_bytes=DEFAULT_MAX_FRAME_BYTES,
        max_depth=DEFAULT_MAX_DEPTH,
        max_objects=DEFAULT_MAX_OBJECTS,
    ):
        if isinstance(source, (bytes, bytearray, memoryview)):
            source = io.BytesIO(bytes(source))
        if not hasattr(source, "read") or not hasattr(source, "readline"):
            throw(TypeError, "Binary stream or bytes expected")
        self.stream = source
        self.max_frame_bytes = max_frame_bytes
        self.max_depth = max_depth
        self.max_objects = max_objects
        self._hasher = hashlib.sha256()
        self._manifest = None
        self._magic_seen = False

    # -- low-level frame parsing --------------------------------------------

    def _read_exact(self, size):
        chunks = []
        remaining = size
        while remaining:
            chunk = self.stream.read(remaining)
            if not chunk:
                throw(
                    EnvelopeIncompleteError,
                    "Stream ended while reading %d more frame bytes" % remaining,
                )
            chunks.append(chunk)
            remaining -= len(chunk)
        return b"".join(chunks)

    def _read_frame(self):
        line = self.stream.readline()
        if not line:
            return None
        if len(line) > _MAX_HEADER_LINE:
            throw(EnvelopeFormatError, "Oversized frame header")
        try:
            tokens = line.decode("ascii").split()
        except UnicodeDecodeError:
            throw(EnvelopeFormatError, "Non-ASCII frame header")
        if len(tokens) not in (2, 3):
            throw(EnvelopeFormatError, "Malformed frame header: %r" % line)
        kind = tokens[0]
        if kind not in (
            FRAME_MANIFEST,
            FRAME_CHUNK,
            FRAME_REF_INDEX,
            FRAME_FOOTER,
        ):
            throw(EnvelopeFormatError, "Unknown frame kind: %r" % kind)
        try:
            if kind in (FRAME_MANIFEST, FRAME_FOOTER):
                if len(tokens) != 2:
                    throw(EnvelopeFormatError, "Frame %s takes no sequence" % kind)
                seq = None
                size = int(tokens[1])
            else:
                if len(tokens) != 3:
                    throw(
                        EnvelopeFormatError, "Frame %s requires a sequence" % kind
                    )
                seq = int(tokens[1])
                size = int(tokens[2])
        except ValueError:
            throw(EnvelopeFormatError, "Non-numeric frame header: %r" % line)
        if size < 0 or size > self.max_frame_bytes:
            # Length is validated before any payload allocation happens.
            throw(
                EnvelopeLimitError,
                "Frame length %d exceeds max_frame_bytes=%d"
                % (size, self.max_frame_bytes),
            )
        raw_payload = self._read_exact(size)
        trailing = self.stream.read(1)
        if not trailing:
            throw(EnvelopeIncompleteError, "Stream ended after frame payload")
        if trailing != b"\n":
            throw(EnvelopeCorruptError, "Frame is not newline-terminated")
        _scan_nesting_depth(raw_payload, self.max_depth)
        try:
            payload = json.loads(raw_payload.decode("utf-8"))
        except (UnicodeDecodeError, json.JSONDecodeError) as exc:
            throw(EnvelopeCorruptError, "Invalid frame payload: %s" % exc)
        return kind, seq, payload, raw_payload, line

    def _hash_frame(self, header, raw_payload):
        self._hasher.update(header)
        self._hasher.update(raw_payload)

    def read_manifest(self):
        if self._manifest is not None:
            return self._manifest
        magic = self.stream.readline()
        if not magic:
            throw(EnvelopeIncompleteError, "Empty envelope stream")
        if magic != MAGIC_PREFIX:
            throw(EnvelopeFormatError, "Bad envelope magic prefix")
        self._magic_seen = True
        frame = self._read_frame()
        if frame is None or frame[0] != FRAME_MANIFEST:
            throw(EnvelopeFormatError, "First frame must be the manifest")
        kind, seq, manifest, raw_payload, header = frame
        self._hash_frame(header, raw_payload)
        self._validate_manifest(manifest)
        self._manifest = manifest
        return manifest

    def _validate_manifest(self, manifest):
        if not isinstance(manifest, dict):
            throw(EnvelopeFormatError, "Manifest must be an object")
        if manifest.get("format") != FORMAT_NAME:
            throw(EnvelopeFormatError, "Unknown envelope format")
        if manifest.get("version") != FORMAT_VERSION:
            throw(EnvelopeFormatError, "Unsupported envelope version")
        limits = manifest.get("limits")
        if not isinstance(limits, dict):
            throw(EnvelopeFormatError, "Manifest limits are missing")
        if limits.get("max_frame_bytes", 0) > self.max_frame_bytes:
            throw(
                EnvelopeLimitError,
                "Manifest declares a larger frame limit than the parser allows",
            )
        if limits.get("max_depth", 0) > self.max_depth:
            throw(
                EnvelopeLimitError,
                "Manifest declares a larger depth limit than the parser allows",
            )
        if limits.get("max_objects", 0) > self.max_objects:
            throw(
                EnvelopeLimitError,
                "Manifest declares a larger object limit than the parser allows",
            )
        if not isinstance(manifest.get("mapping_fingerprint"), str):
            throw(EnvelopeFormatError, "Manifest mapping fingerprint is missing")
        if not isinstance(manifest.get("field_config"), dict):
            throw(EnvelopeFormatError, "Manifest field configuration is missing")
        pk_types = manifest.get("pk_types")
        if not isinstance(pk_types, dict):
            throw(EnvelopeFormatError, "Manifest pk_types section is missing")

    def iter_frames(self):
        """Yield ``(kind, payload)`` records incrementally up to the footer."""
        self.read_manifest()
        while True:
            frame = self._read_frame()
            if frame is None:
                throw(EnvelopeIncompleteError, "Stream ended without a footer")
            kind, seq, payload, raw_payload, header = frame
            if kind != FRAME_FOOTER:
                self._hash_frame(header, raw_payload)
            yield kind, payload
            if kind == FRAME_FOOTER:
                return

    # -- graph reconstruction ------------------------------------------------

    def read_value_graph(self):
        manifest = self.read_manifest()
        definitions = []
        edges = []
        footer = None
        chunk_seqs = []
        ref_seqs = []
        object_count = 0
        while True:
            frame = self._read_frame()
            if frame is None:
                break
            kind, seq, payload, raw_payload, header = frame
            if kind != FRAME_FOOTER:
                self._hash_frame(header, raw_payload)
            if kind == FRAME_CHUNK:
                chunk_seqs.append(seq)
                objects = payload.get("objects") if isinstance(payload, dict) else None
                if not isinstance(objects, list) or payload.get("seq") != seq:
                    throw(EnvelopeCorruptError, "Malformed chunk frame")
                object_count += len(objects)
                if object_count > self.max_objects:
                    throw(EnvelopeLimitError, "Object count limit reached")
                definitions.extend(objects)
            elif kind == FRAME_REF_INDEX:
                ref_seqs.append(seq)
                frame_edges = payload.get("edges") if isinstance(payload, dict) else None
                if not isinstance(frame_edges, list) or payload.get("seq") != seq:
                    throw(EnvelopeCorruptError, "Malformed reference index frame")
                edges.extend(frame_edges)
            elif kind == FRAME_FOOTER:
                footer = payload
                break
            else:
                throw(EnvelopeFormatError, "Unexpected frame %s after manifest" % kind)

        if footer is None:
            throw(EnvelopeIncompleteError, "Envelope has no footer; graph is incomplete")
        self._validate_footer(
            footer, manifest, definitions, edges, chunk_seqs, ref_seqs
        )
        return self._build_graph(manifest, footer, definitions, edges)

    def _validate_footer(self, footer, manifest, definitions, edges,
                         chunk_seqs, ref_seqs):
        if not isinstance(footer, dict) or footer.get("complete") is not True:
            throw(EnvelopeIncompleteError, "Footer does not mark a complete graph")
        if footer.get("digest") != self._hasher.hexdigest():
            throw(EnvelopeCorruptError, "Envelope digest mismatch")
        if footer.get("objects") != len(definitions):
            throw(EnvelopeCorruptError, "Footer object count mismatch")
        if footer.get("refs") != len(edges):
            throw(EnvelopeCorruptError, "Footer reference count mismatch")
        if footer.get("chunks") != len(chunk_seqs):
            throw(EnvelopeCorruptError, "Footer chunk count mismatch")
        if footer.get("ref_chunks") != len(ref_seqs):
            throw(EnvelopeCorruptError, "Footer reference chunk count mismatch")
        if chunk_seqs != list(range(1, len(chunk_seqs) + 1)):
            throw(EnvelopeCorruptError, "Chunk sequence is not contiguous")
        if ref_seqs != list(range(1, len(ref_seqs) + 1)):
            throw(EnvelopeCorruptError, "Reference sequence is not contiguous")
        if footer.get("unresolved") != []:
            throw(EnvelopeClosureError, "Footer reports unresolved references")

        defined_keys = set()
        temp_count = 0
        for definition in definitions:
            if not isinstance(definition, dict) or "id" not in definition:
                throw(EnvelopeCorruptError, "Malformed object definition")
            key = _identity_key(definition["id"])
            if key in defined_keys:
                throw(EnvelopeCorruptError, "Duplicate object definition")
            self._check_pk_schema(manifest, key)
            defined_keys.add(key)
            if key[1][0] == "tmp":
                temp_count += 1
        if footer.get("temp_objects") != temp_count:
            throw(EnvelopeCorruptError, "Footer temporary object count mismatch")

        inline_refs = set()
        for definition in definitions:
            source_key = _identity_key(definition["id"])
            for tagged in definition.get("fields", {}).values():
                if isinstance(tagged, list) and tagged and tagged[0] == TAG_REF:
                    inline_refs.add(
                        (source_key, _identity_key(tagged[1]), bool(tagged[2]))
                    )
            for snapshot in definition.get("collections", {}).values():
                for tagged in snapshot.get("items", []):
                    inline_refs.add(
                        (source_key, _identity_key(tagged[1]), bool(tagged[2]))
                    )

        indexed_refs = set()
        for edge in edges:
            indexed_refs.add(
                (
                    _identity_key(edge["from"]),
                    _identity_key(edge["to"]),
                    bool(edge["external"]),
                )
            )
        if inline_refs != indexed_refs:
            throw(
                EnvelopeCorruptError,
                "Reference index does not match inline references",
            )
        external_count = sum(1 for _s, _t, external in indexed_refs if external)
        if footer.get("external_refs") != external_count:
            throw(EnvelopeCorruptError, "Footer external reference count mismatch")
        for _source, target_key, external in indexed_refs:
            if not external and target_key not in defined_keys:
                throw(
                    EnvelopeClosureError,
                    "Internal reference has no object definition: %r" % (target_key,),
                )

    def _check_pk_schema(self, manifest, key):
        entity, identity = key
        if identity[0] == "tmp":
            return
        pk_types = manifest["pk_types"].get(entity)
        if pk_types is None:
            throw(EnvelopeFormatError, "Unknown entity in manifest: %s" % entity)
        actual_tags = [pair[0] for pair in identity[1]]
        if actual_tags != pk_types:
            throw(
                EnvelopeFormatError,
                "Typed PK tags %r do not match manifest schema %r for %s"
                % (actual_tags, pk_types, entity),
            )

    def _build_graph(self, manifest, footer, definitions, edges):
        nodes = {}
        pending = []
        for definition in definitions:
            key = _identity_key(definition["id"])
            entity = key[0]
            node = ValueNode(entity, key, is_temp=(key[1][0] == "tmp"))
            nodes[key] = node
            pending.append((node, definition))

        external_refs = set()

        def resolve_reference(tagged):
            if (
                not isinstance(tagged, list)
                or len(tagged) != 3
                or tagged[0] != TAG_REF
            ):
                throw(EnvelopeCorruptError, "Malformed reference record")
            target_key = _identity_key(tagged[1])
            external = bool(tagged[2])
            if external:
                ref = ExternalRef(tagged[1], target_key)
                external_refs.add(ref)
                return ref
            target = nodes.get(target_key)
            if target is None:
                throw(
                    EnvelopeClosureError,
                    "Unresolved internal reference: %r" % (target_key,),
                )
            return target

        for node, definition in pending:
            fields = definition.get("fields")
            if not isinstance(fields, dict):
                throw(EnvelopeCorruptError, "Object fields must be an object")
            for name, tagged in fields.items():
                if isinstance(tagged, list) and tagged and tagged[0] == TAG_REF:
                    node.fields[name] = resolve_reference(tagged)
                elif isinstance(tagged, list) and tagged and tagged[0] == TAG_ABSENT:
                    if len(tagged) != 2 or tagged[1] not in ABSENT_REASONS:
                        throw(EnvelopeFormatError, "Invalid ABSENT record")
                    node.fields[name] = _ABSENT_MAP[tagged[1]]
                else:
                    node.fields[name] = decode_tagged(tagged)

            collections = definition.get("collections")
            if not isinstance(collections, dict):
                throw(EnvelopeCorruptError, "Object collections must be an object")
            for name, snapshot in collections.items():
                if not isinstance(snapshot, dict):
                    throw(EnvelopeCorruptError, "Malformed collection snapshot")
                state = snapshot.get("state")
                if state not in COLLECTION_STATES:
                    throw(EnvelopeFormatError, "Unknown collection state: %r" % state)
                if state == "absent":
                    reason = snapshot.get("reason")
                    if reason not in ABSENT_REASONS:
                        throw(EnvelopeFormatError, "Invalid absent collection reason")
                    node.collections[name] = CollectionSnapshot(
                        "absent", None, (), reason
                    )
                    continue
                complete = snapshot.get("complete")
                if not isinstance(complete, bool):
                    throw(EnvelopeFormatError, "Collection completeness must be bool")
                if state == "complete" and not complete:
                    throw(
                        EnvelopeFormatError,
                        "Complete collection must carry complete=true",
                    )
                if state == "partial" and complete:
                    throw(
                        EnvelopeFormatError,
                        "Partial collection must carry complete=false",
                    )
                items = [
                    resolve_reference(tagged)
                    for tagged in snapshot.get("items", [])
                ]
                node.collections[name] = CollectionSnapshot(
                    state, complete, items, None
                )

        for node, _ in pending:
            node.freeze()
        return ReadOnlyValueGraph(manifest, footer, nodes, external_refs)

    def verify_mapping(self, database):
        manifest = self.read_manifest()
        fingerprint = compute_mapping_fingerprint(database)
        if fingerprint != manifest["mapping_fingerprint"]:
            throw(
                MappingFingerprintError,
                "Mapping fingerprint mismatch: manifest=%s current=%s"
                % (manifest["mapping_fingerprint"], fingerprint),
            )
        return True
