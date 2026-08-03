"""Config-driven quality checks over Bronze.

A rule is declared in the source YAML, not in code: the framework runs the
rule's query against Bronze and evaluates the scalar result. Adding a rule is
config, zero new Python. This is the Bronze -> Silver validation gate; Bronze is
schema-on-read (DDIA Ch 4), so contract enforcement lives here on the read side.
"""
from dataclasses import dataclass

# Closed set of operators for hard assertions. Not eval(): a typo in the YAML
# fails loud instead of executing arbitrary Python.
_OPS = {
    "eq": lambda a, b: a == b,
    "le": lambda a, b: a <= b,
    "ge": lambda a, b: a >= b,
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

    for rule in rules:
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


def print_report(results):
    for r in results:
        flag = "PASS" if r.passed else ("FAIL" if r.severity == "hard" else "WARN")
        print(f"{flag:5} {r.name:32} observed={r.observed:.4f}  ({r.detail})")