//! `PERCENTILE_CONT` and `PERCENTILE_DISC` with `WITHIN GROUP` written for Spark
//! and Databricks. Both run them exactly since Spark 3.3, so they're kept as
//! written rather than turned into the approximate `PERCENTILE_APPROX`. On
//! Spark 4.0.1, over 1, 2, 3, 4, 10, `PERCENTILE_CONT(0.9)` is 7.6 where
//! `PERCENTILE_APPROX(v, 0.9)` is 10. Presto, Trino and Athena have no
//! `WITHIN GROUP` and still get `APPROX_PERCENTILE`.
use polyglot_sql::DialectType;

fn transpile(sql: &str, source: DialectType, target: DialectType) -> String {
    polyglot_sql::transpile(sql, source, target)
        .unwrap()
        .remove(0)
}

#[test]
fn spark_keeps_exact_percentiles() {
    for target in [DialectType::Spark, DialectType::Databricks] {
        for sql in [
            "SELECT PERCENTILE_CONT(0.9) WITHIN GROUP (ORDER BY v) FROM t",
            "SELECT PERCENTILE_DISC(0.9) WITHIN GROUP (ORDER BY v DESC) FROM t",
            "SELECT g, PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY v) FROM t GROUP BY g",
        ] {
            for source in [DialectType::Generic, target] {
                assert_eq!(
                    transpile(sql, source, target),
                    sql,
                    "{source:?} -> {target:?}"
                );
            }
        }
    }
}

#[test]
fn spark_keeps_exact_percentiles_with_filter() {
    for target in [DialectType::Spark, DialectType::Databricks] {
        assert_eq!(
            transpile(
                "SELECT PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY v) FILTER (WHERE v > 1) FROM t",
                DialectType::Generic,
                target
            ),
            "SELECT PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY v) FILTER(WHERE v > 1) FROM t"
        );
    }
}

#[test]
fn spark_keeps_postgres_percentiles_exact() {
    for target in [DialectType::Spark, DialectType::Databricks] {
        assert_eq!(
            transpile(
                "SELECT PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY amount)",
                DialectType::PostgreSQL,
                target
            ),
            "SELECT PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY amount NULLS LAST)"
        );
    }
}

#[test]
fn trino_still_approximates() {
    for target in [DialectType::Presto, DialectType::Trino, DialectType::Athena] {
        assert_eq!(
            transpile(
                "SELECT PERCENTILE_CONT(0.9) WITHIN GROUP (ORDER BY v) FROM t",
                DialectType::Generic,
                target
            ),
            "SELECT APPROX_PERCENTILE(v, 0.9) FROM t"
        );
    }
}
