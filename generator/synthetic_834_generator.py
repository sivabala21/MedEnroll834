"""
generator/synthetic_834_generator.py
Synthetic X12 834 EDI file generator for Databricks.
Generates enrollment files and supports DBFS/Unity Catalog volumes.
"""
import argparse
import hashlib
import os
import random
import string
import time
from datetime import date, datetime, timedelta
from pathlib import Path
from typing import Optional

try:
    from pyspark.sql import SparkSession
    _spark_available = True
except ImportError:
    _spark_available = False

FIRST_NAMES = [
    "JOHN", "JANE", "ROBERT", "MARY", "WILLIAM", "PATRICIA", "MICHAEL",
    "LINDA", "DAVID", "BARBARA", "RICHARD", "SUSAN", "JOSEPH", "JESSICA",
    "THOMAS", "SARAH", "CHARLES", "KAREN", "DANIEL", "LISA", "MATTHEW",
    "NANCY", "MARK", "BETTY", "JAMES", "MARGARET", "PAUL", "SANDRA",
    "ANDREW", "ASHLEY", "JOSHUA", "KIMBERLY", "KENNETH", "EMILY",
    "KEVIN", "DONNA", "BRIAN", "MICHELLE", "GEORGE", "CAROL",
]
LAST_NAMES = [
    "SMITH", "JOHNSON", "WILLIAMS", "BROWN", "JONES", "GARCIA", "MILLER",
    "DAVIS", "RODRIGUEZ", "MARTINEZ", "HERNANDEZ", "LOPEZ", "GONZALEZ",
    "WILSON", "ANDERSON", "THOMAS", "TAYLOR", "MOORE", "JACKSON", "MARTIN",
    "LEE", "PEREZ", "THOMPSON", "WHITE", "HARRIS", "SANCHEZ", "CLARK",
    "RAMIREZ", "LEWIS", "ROBINSON", "WALKER", "YOUNG", "ALLEN", "KING",
    "WRIGHT", "SCOTT", "TORRES", "NGUYEN", "HILL", "FLORES",
]
STREET_NAMES = [
    "MAIN ST", "OAK AVE", "PINE RD", "ELM ST", "CEDAR LN", "MAPLE DR",
    "WASHINGTON BLVD", "PARK AVE", "LAKE VIEW DR", "HILLTOP RD",
]
CITIES = [
    "NEW YORK", "LOS ANGELES", "CHICAGO", "HOUSTON", "PHOENIX", "PHILADELPHIA",
    "SAN ANTONIO", "SAN DIEGO", "DALLAS", "SAN JOSE", "AUSTIN", "JACKSONVILLE",
    "FORT WORTH", "COLUMBUS", "INDIANAPOLIS", "CHARLOTTE", "SAN FRANCISCO",
    "SEATTLE", "DENVER", "WASHINGTON",
]
STATES = [
    "AL", "AK", "AZ", "AR", "CA", "CO", "CT", "DE", "FL", "GA", "HI", "ID",
    "IL", "IN", "IA", "KS", "KY", "LA", "ME", "MD", "MA", "MI", "MN", "MS",
    "MO", "MT", "NE", "NV", "NH", "NJ", "NM", "NY", "NC", "ND", "OH", "OK",
    "OR", "PA", "RI", "SC", "SD", "TN", "TX", "UT", "VT", "VA", "WA", "WV",
    "WI", "WY",
]
PLAN_CODES = ["GOLD", "SILVER", "BRONZE", "PLATINUM", "HDHP", "EPO", "PPO", "HMO"]
RELATIONSHIP = ["18", "01", "03", "19"]
ADR_TYP = ["HOME", "MAILING", "BILLING", "WORK"]


def _random_date(start_year: int = 1970, end_year: int = 2010) -> str:
    start = date(start_year, 1, 1)
    end = date(end_year, 12, 31)
    delta = (end - start).days
    return (start + timedelta(days=random.randint(0, delta))).strftime("%Y%m%d")


def _future_date(years: int = 1) -> str:
    return (date.today() + timedelta(days=365 * years)).strftime("%Y%m%d")


def _random_name() -> tuple:
    first = random.choice(FIRST_NAMES)
    last = random.choice(LAST_NAMES)
    middle = random.choice(["A", "B", "C", None, None, None])
    return first, last, middle


def _checksum_isa(isa_segment: str) -> str:
    total = sum(ord(c) for c in isa_segment)
    return f"{total % 256:02d}"


def _control_number() -> str:
    return f"{random.randint(1, 999999):06d}"


def build_isa(ctrl: str, file_date: str) -> str:
    body = (
        f"*00*          *00*          "
        f"*ZZ*SENDERID     *ZZ*RECEIVERID   "
        f"*{file_date}*1200*^*00501*{ctrl}*0*T*:"
    )
    checksum = _checksum_isa(body)
    return f"ISA{body}{checksum}"


def build_gs(ctrl: str, file_date: str) -> str:
    return f"GS*HC*SENDER*RECEIVER*{file_date}*1200*{ctrl}*X*005010X222A1"


def build_st(ctrl: str) -> str:
    return f"ST*834*{ctrl}"


def build_seg_count(seg_count: int, ctrl: str) -> str:
    return f"SE*{seg_count}*{ctrl}"


def build_ge(txn_count: int, ctrl: str) -> str:
    return f"GE*{txn_count}*{ctrl}"


def build_iea(ctrl_count: int, ctrl: str) -> str:
    return f"IEA*{ctrl_count}*{ctrl}"


def build_hl(hl_id: str, parent_id: str, level: str, child_count: str = "") -> str:
    parts = [f"HL*{hl_id}"]
    parts.append(f"*{parent_id}" if parent_id else "*")
    parts.append(f"*{level}")
    parts.append(f"*{child_count}" if child_count else "*")
    return "".join(parts)


def build_nm1(entity: str, last: str, first: str, middle: Optional[str], id_qual: str, member_id: str) -> str:
    parts = [f"NM1*{entity}", f"*{last}", f"*{first}"]
    parts.append(f"*{middle}" if middle else "*")
    parts.append("*")
    parts.append("*")
    parts.append(f"*{id_qual}")
    parts.append(f"*{member_id}")
    return "".join(parts)


def build_ins(relationship: str) -> str:
    return f"INS*Y*{relationship}*18*00*20*18*00"


def build_dtp(qualifier: str, value: str) -> str:
    return f"DTP*{qualifier}*D8*{value}"


def build_hd(coverage_level: str, plan_code: str) -> str:
    return f"HD*{coverage_level}*Y*{plan_code}"


def build_eb(category: str, start: str, end: str) -> str:
    return f"EB*{category}*{start}*{end}****CI"


def build_addr(meme_id: str, eff_dt: str) -> str:
    street_num = random.randint(100, 9999)
    street = random.choice(STREET_NAMES)
    adr_ln1 = f"{street_num} {street}"
    adr_ln2 = random.choice(["APT " + str(random.randint(1, 500)), "", ""])
    city = random.choice(CITIES)
    state = random.choice(STATES)
    zip_code = f"{random.randint(10000, 99999)}"
    adr_typ = random.choice(ADR_TYP)
    # One X12 segment per element. The writer joins segments with the segment
    # terminator, so this must NOT pre-join them with "~" or the parser will
    # receive several segments glued onto a single logical line.
    return [
        f"N3*{adr_ln1}*{adr_ln2}",
        f"N4*{city}*{state}*{zip_code}*USA",
        f"REF*2L*{meme_id}",
        f"DTM*350*{eff_dt}",
    ]


def build_bgn(ref: str, file_date: str) -> str:
    return f"BGN*00*{ref}*{file_date}*1200**{file_date}"


def build_834_file(
    file_date: str,
    num_subscribers: int = 100,
    dependents_per_sub: int = 2,
    messy: bool = False,
) -> str:
    segments = []
    txn_count = 0
    isa_ctrl = _control_number()
    gs_ctrl = _control_number()

    for s in range(num_subscribers):
        txn_count += 1
        st_ctrl = f"{txn_count:06d}"
        ref = f"BATCH-{file_date}-{s + 1:04d}"
        segments.append(build_st(st_ctrl))
        segments.append(build_bgn(ref, file_date))

        sub_first, sub_last, sub_mid = _random_name()
        sub_id = f"SUB-{file_date}-{s + 1:04d}"
        sub_dob = _random_date(1950, 2005)
        grgr_id = f"GRGR-{random.randint(1, 50):04d}"
        plan_code = random.choice(PLAN_CODES)
        eff_dt = file_date
        term_dt = _future_date(1) if random.random() > 0.3 else ""

        segments.append(build_hl("1", "", "20", str(dependents_per_sub + 1)))
        segments.append(build_nm1("IL", sub_last, sub_first, sub_mid, "0F", sub_id))
        segments.append(build_ins("18"))
        segments.append(build_dtp("291", sub_dob))
        segments.append(build_dtp("472", eff_dt))
        if term_dt:
            segments.append(build_dtp("349", term_dt))
        segments.append(build_hd("COV", plan_code))
        segments.append(build_eb("C", eff_dt, term_dt if term_dt else ""))

        for d in range(dependents_per_sub):
            dep_first, dep_last, dep_mid = _random_name()
            dep_rel = random.choice(RELATIONSHIP)
            dep_dob = _random_date(1990, 2020)
            dep_id = f"DEP-{file_date}-{s + 1:04d}-{d + 1:02d}"
            dep_eff = eff_dt if random.random() > 0.2 else _future_date(0)
            segments.append(build_hl(str(d + 2), "1", "21", ""))
            segments.append(build_nm1("IL", dep_last, dep_first, dep_mid, "0F", dep_id))
            segments.append(build_ins(dep_rel))
            segments.append(build_dtp("291", dep_dob))
            segments.append(build_dtp("348", dep_eff))
            segments.append(build_hd("DEP", plan_code))
            segments.append(build_eb("C", dep_eff, term_dt if term_dt else ""))
            segments.extend(build_addr(dep_id, dep_eff))

        segments.extend(build_addr(sub_id, eff_dt))
        seg_count = len(segments) + 2
        segments.append(build_seg_count(seg_count, st_ctrl))

    isa = build_isa(isa_ctrl, file_date)
    gs = build_gs(gs_ctrl, file_date)
    se = build_seg_count(len(segments) + 2, f"{txn_count:06d}")
    ge = build_ge(txn_count, f"{txn_count:06d}")
    iea = build_iea(1, f"{txn_count:06d}")
    # X12 is a segment-terminator-delimited stream: every segment ends with
    # "~" and a trailing newline is added for readability. The '~' is the real
    # record delimiter; newlines are cosmetic.
    return "\n".join(s + "~" for s in [isa, gs] + segments + [se, ge, iea]) + "\n"


def generate_batch(
    output_dir: str,
    num_files: int = 10,
    subscribers_per_file: int = 100,
    dependents_per_sub: int = 2,
    start_date: Optional[date] = None,
    messy: bool = False,
) -> list:
    output_path = Path(output_dir)
    output_path.mkdir(parents=True, exist_ok=True)
    start_date = start_date or date.today()
    generated = []

    for i in range(num_files):
        file_date = (start_date - timedelta(days=i)).strftime("%Y%m%d")
        filename = f"enrollment_{file_date}_{i + 1:03d}.834"
        filepath = output_path / filename
        content = build_834_file(
            file_date=file_date,
            num_subscribers=subscribers_per_file,
            dependents_per_sub=dependents_per_sub,
            messy=messy,
        )
        filepath.write_text(content, encoding="utf-8")
        generated.append(str(filepath))

    return generated


def generate_and_load_to_dbfs(
    spark: "SparkSession",
    output_dir: str,
    num_files: int = 10,
    subscribers_per_file: int = 100,
    dependents_per_sub: int = 2,
    volume_path: str = "/Volumes/med834enrl/edi/landing/834",
) -> None:
    files = generate_batch(
        output_dir=output_dir,
        num_files=num_files,
        subscribers_per_file=subscribers_per_file,
        dependents_per_sub=dependents_per_sub,
    )
    for fp in files:
        df = spark.createDataFrame([(Path(fp).read_text(encoding="utf-8"),)], ["value"])
        (
            df.write.mode("append")
            .text(volume_path)
        )


def main():
    parser = argparse.ArgumentParser(description="Generate synthetic X12 834 enrollment files")
    parser.add_argument("--output", "-o", default="landing/")
    parser.add_argument("--files", "-f", type=int, default=10)
    parser.add_argument("--subs", "-s", type=int, default=100)
    parser.add_argument("--deps", "-d", type=int, default=2)
    parser.add_argument("--date", type=str, default=None)
    parser.add_argument("--messy", action="store_true")
    parser.add_argument("--dbfs", action="store_true", help="Load directly to DBFS via Spark")
    parser.add_argument("--volume-path", type=str, default="/Volumes/med834enrl/edi/landing/834")
    args = parser.parse_args()

    start_date = None
    if args.date:
        start_date = datetime.strptime(args.date, "%Y%m%d").date()

    if args.dbfs and _spark_available:
        spark = SparkSession.builder.appName("834-Generator").getOrCreate()
        generate_and_load_to_dbfs(
            spark=spark,
            output_dir=args.output,
            num_files=args.files,
            subscribers_per_file=args.subs,
            dependents_per_sub=args.deps,
            volume_path=args.volume_path,
        )
    else:
        generate_batch(
            output_dir=args.output,
            num_files=args.files,
            subscribers_per_file=args.subs,
            dependents_per_sub=args.deps,
            start_date=start_date,
            messy=args.messy,
        )


if __name__ == "__main__":
    main()
