"""Config-driven quality checks over Bronze.

A rule is declared in the source YAML, not in code: the framework runs the
rule's query against Bronze and evaluates the scalar result. Adding a rule is
config, zero new Python. This is the Bronze -> Silver validation gate; Bronze is
schema-on-read (DDIA Ch 4), so contract enforcement lives here on the read side.
"""
from dataclasses import dataclass
from pyspark.sql import functions as F

# Closed set of operators for hard assertions. Not eval(): a typo in the YAML
# fails loud instead of executing arbitrary Python.
_OPS = {
    "eq": lambda a, b: a == b,
    "le": lambda a, b: a <= b,
    "ge": lambda a, b: a >= b,
}

# Closed set of row-level predicates. A check maps a column to a boolean column
# expression that is TRUE when the row is GOOD. not_null: the column is present.
# Adding a check type is one entry here; adding a RULE is still just YAML.
_ROW_CHECKS = {
    "not_null": lambda col: F.col(col).isNotNull(),
}

class QualityError(Exception):
    """Raised when a hard rule fails. Fails the run loud, by design."""


@dataclass
class RuleResult:
    name: str
    severity: str
    observed: float
    passed: bool
    detail: str


def run_quality_checks(bronze_df, rules):
    # Expose Bronze as a temp view so each rule can be plain SQL. Read-only over
    # Bronze, so two runs produce identical results: idempotent by construction.
    bronze_df.createOrReplaceTempView("bronze")
    spark = bronze_df.sparkSession
    results = []

    for rule in [r for r in rules if r.get("type") == "aggregate"]:
        # Every rule query returns a single column named `observed`.
        observed = float(spark.sql(rule["query"]).first()["observed"])
        sev = rule["severity"]

        if sev == "hard":
            op, value = rule["assert"]["op"], rule["assert"]["value"]
            passed = _OPS[op](observed, value)
            detail = f"assert observed {op} {value}"
            if not passed:
                # Loud failure: the run stops here, nothing downstream trusts bad Bronze.
                raise QualityError(
                    f"[{rule['name']}] hard rule failed: observed={observed}, expected {op} {value}"
                )
        elif sev == "soft":
            expected, tolerance = rule["expected"], rule["tolerance"]
            passed = abs(observed - expected) <= tolerance  # report, do not raise
            detail = f"expected {expected} +/- {tolerance}"
        else:
            raise QualityError(f"[{rule['name']}] unknown severity: {sev}")

        results.append(RuleResult(rule["name"], sev, observed, passed, detail))

    return results


def split_quarantine(df, rules, run_id):
    """Partition df into (clean, rejects) using the row-level rules.

    A row is rejected if it fails ANY row rule. Rejects carry the full original
    row plus three context columns, so the table stays inspectable: which rule,
    when, and (via the W2 lineage columns already on the row) which source file.
    Nothing is dropped and nothing crashes the run. Silent drop is the anti-goal.
    """
    row_rules = [r for r in rules if r.get("type") == "row"]
    if not row_rules:
        # No row rules: everything is clean, empty rejects. Keeps callers uniform.
        return df, df.limit(0).withColumn("_reject_rule", F.lit(None).cast("string")) \
                              .withColumn("_rejected_at", F.lit(None).cast("timestamp"))

    # Build one boolean column per rule: TRUE where that rule is VIOLATED. We tag
    # the FIRST violated rule so a row rejected by two rules reports one reason,
    # not a row duplicated across reasons. coalesce picks the first non-null.
    violation_tags = [
        F.when(~_ROW_CHECKS[r["check"]](r["column"]), F.lit(r["name"]))
        for r in row_rules
    ]
    # _reject_rule is null for a clean row (no rule violated), else the first hit.
    tagged = df.withColumn("_reject_rule", F.coalesce(*violation_tags))

    # A row is clean iff no rule fired (_reject_rule is null). Split on that.
    clean = tagged.filter(F.col("_reject_rule").isNull()).drop("_reject_rule")
    rejects = (
        tagged.filter(F.col("_reject_rule").isNotNull())
        # _rejected_at stamps THIS run, so you can tell "broken for weeks" from
        # "started today". _source_file / _source_name already ride the row from W2.
        .withColumn("_rejected_at", F.current_timestamp())
    )
    return clean, rejects


def print_report(results):
    for r in results:
        flag = "PASS" if r.passed else ("FAIL" if r.severity == "hard" else "WARN")
        print(f"{flag:5} {r.name:32} observed={r.observed:.4f}  ({r.detail})")