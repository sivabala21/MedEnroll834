# 12. 834 Parser

> Source: [`src/medenroll834/parsers/edi834_parser.py`](https://github.com/yourusername/medenroll834/blob/main/src/medenroll834/parsers/edi834_parser.py)

`EDI834Parser` is the PySpark-native entry point that turns raw 834 file
content into a Bronze-shaped DataFrame. It is a thin wrapper around the same
segment-splitting logic used by `BronzeLayer` (see
[Chapter 13](13-bronze-layer.md)) — the parser exists as a standalone,
directly-testable class so tests can parse files without going through the
full `BronzeLayer.ingest()` Delta-write path.

## Segment splitting

```python
def split_segments(content: str) -> list:
    """Split an X12 834 payload into individual segments.

    Handles both segment conventions found in real 834 files:
    * terminator-delimited ("ISA*...~GS*...~") where "~" is the only
      record delimiter and physical newlines are cosmetic;
    * line-delimited ("ISA*...\\nGS*...\\n") which some upstream systems
      emit, where the newline is the de-facto terminator and "~" may be
      absent.

    The two are frequently mixed within a single file, so neither
    delimiter can be assumed on its own. Normalise every newline to "~"
    then split on "~" to get exactly one record per segment whatever the
    producer used.
    """
    normalised = content.replace("\r\n", "~").replace("\r", "~").replace("\n", "~")
    return [seg.strip() for seg in normalised.split("~") if seg.strip()]
```

This is a deliberate real-world robustness decision: production 834 feeds
are not guaranteed to be consistently delimited, and this normalization
means the parser never silently drops or merges segments because of a
delimiter mismatch.

## Parsing a directory of files

```python
def parse_files(self, landing_path: str) -> DataFrame:
    path = Path(landing_path)
    files = sorted(path.glob("*.834"))
    rows = []
    for file in files:
        content = file.read_text(encoding="utf-8")
        file_name = file.name
        for seg_order, line in enumerate(split_segments(content)):
            elements = line.split("*")
            rows.append({
                "file_name": file_name,
                "segment_id": elements[0],
                "elements": elements,
                "raw_segment": line,
                "seg_order": seg_order,
            })
    return self.spark.createDataFrame(rows, schema=BRONZE_SCHEMA)
```

Each X12 segment is split on `*` into its data elements; `elements[0]` is
always the segment identifier (`ISA`, `GS`, `ST`, `HL`, `NM1`, `INS`, `DTP`,
`HD`, `N3`, `N4`, `REF`, `SE`, `GE`, `IEA`). `seg_order` preserves the
segment's original position within the file — critical because downstream
Silver logic resolves hierarchy and "most recent value" using window
functions ordered by this column.

## Schema

```python
BRONZE_SCHEMA = StructType([
    StructField("file_name", StringType(), True),
    StructField("segment_id", StringType(), True),
    StructField("elements", ArrayType(StringType()), True),
    StructField("raw_segment", StringType(), True),
    StructField("seg_order", LongType(), True),
])
```

`elements` is kept as an **array of strings**, not individually typed
columns — this is what lets a single Bronze schema represent every segment
type without a different column set per segment, at the cost of pushing
positional extraction (`elements[8]` for a member ID, etc.) into Silver.

## The relationship code map

```python
RELATIONSHIP_MAP = {
    "18": "self",
    "01": "spouse",
    "03": "child",
    "19": "domestic_partner",
}
```

Built into a Spark `create_map()` expression once at construction time
(`self.rel_map`) so relationship-code lookups in downstream transforms are
a vectorized Spark SQL expression rather than a Python UDF.

## Also supports in-memory DataFrames

```python
def parse_dataframe(self, df: DataFrame, file_name_col: str = "raw") -> DataFrame:
    rows = []
    for row in df.collect():
        content = row[file_name_col]
        ...
```

Used when 834 content is already loaded into a Spark DataFrame (e.g. read
via `spark.read.text()` from a Unity Catalog Volume) rather than present as
local files — this is the shape the production Databricks notebook job
uses (see [Chapter 21](21-airflow-dag-implementation.md)).
