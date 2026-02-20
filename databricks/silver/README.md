# Silver Layer Data Quality Expectations

DLT-style data quality validation for the Silver layer with comprehensive expectation rules and quality monitoring.

## Overview

The Silver layer applies **data quality expectations** before writing resolved state to `silver_cows_history`. This ensures the Silver layer maintains high data quality as the **SOURCE OF TRUTH** for cow canonical state.

### Key Features

- ✅ **14 validation rules** covering critical and non-critical data quality
- 🔴 **DROP expectations**: Reject rows with critical failures
- ⚠️ **WARN expectations**: Log issues but allow data through
- 📊 **Quality logging**: Track all failures in `silver_quality_log`
- 📈 **Quality reporting**: Generate failure rate reports and identify problematic events

## Architecture

```
Bronze Events
     ↓
State Resolution (SCD Type 2)
     ↓
Apply Expectations ← expectations.py
     ↓
     ├─→ PASS → silver_cows_history
     └─→ FAIL → silver_quality_log
```

## Expectations

### DROP Expectations (Critical)

These **must pass** or rows are rejected:

1. **valid_cow_id**: `cow_id IS NOT NULL`
2. **valid_tenant_id**: `tenant_id IS NOT NULL`
3. **valid_tag_number**: `tag_number IS NOT NULL AND LENGTH(TRIM(tag_number)) > 0`
4. **valid_status**: `status IN ('active', 'inactive')`
5. **valid_start_at**: `__START_AT IS NOT NULL`
6. **valid_sequence**: `__SEQUENCE_NUMBER IS NOT NULL AND __SEQUENCE_NUMBER >= 0` (allows 0 for SQL Server compatibility)

### WARN Expectations (Non-Critical)

These are **logged but allowed**:

7. **valid_breed**: Breed must be in list of 40 recognized breeds (includes "Black Angus")
8. **valid_birth_date**: `birth_date >= '2000-01-01' AND birth_date <= CURRENT_DATE`
9. **valid_weight**: `weight_kg > 0 AND weight_kg < 2000`
10. **valid_sex**: `sex IN ('male', 'female')` or null
11. **reasonable_name_length**: `LENGTH(name) <= 100` or null
12. **valid_color**: Color must be recognized value or null
13. **valid_deactivation_reason**: Reason must be recognized value or null
14. **dummy_expectation**: Placeholder (always passes)

## Usage

### Running Silver Resolution with Expectations

```bash
# Full refresh with expectations applied
python databricks/silver/resolve_cow_state.py --full-refresh

# Incremental update
python databricks/silver/resolve_cow_state.py
```

**Output:**
```
Applying data quality expectations to 1,234 records...

Applying 14 expectations...

✓ Expectations applied:
  Valid records: 1,200 (97.2%)
  Failed DROP: 34 (2.8%)

Logging quality issues to silver_quality_log...
✓ Wrote quality log records

Writing 1,200 valid records to Silver history table...
✓ Wrote 1,200 records to Silver

⚠️  WARNING: 34 records failed critical expectations and were NOT written
   Check silver_quality_log for details
```

### Viewing Quality Reports

```bash
# Overall summary
python databricks/silver/quality_report.py

# Specific expectation details
python databricks/silver/quality_report.py --expectation valid_breed

# Tenant-specific issues
python databricks/silver/quality_report.py --tenant <tenant_id>

# Show problematic Bronze events
python databricks/silver/quality_report.py --problematic

# Export to CSV
python databricks/silver/quality_report.py --export report.csv
```

### Testing Expectations

```bash
# Run comprehensive test with invalid data
./databricks/silver/test_quality_expectations.sh
```

This creates:
- 1 valid cow (baseline)
- 3 cows with WARN-level issues
- 2 cows with DROP-level issues

Verifies:
- Valid cow is written to Silver
- WARN cows are written with logged warnings
- DROP cows are rejected

## Quality Log Schema

The `silver_quality_log` table tracks all expectation failures:

| Column | Type | Description |
|--------|------|-------------|
| log_id | String | Unique log entry ID |
| run_timestamp | Timestamp | When quality check ran |
| expectation_name | String | Which expectation failed |
| severity | String | DROP or WARN |
| failure_count | Long | Number of failures |
| total_rows | Long | Total rows evaluated |
| failure_rate | String | Percentage failed |
| tenant_id | String | Affected tenant (null for summary) |
| cow_id | String | Affected cow (null for summary) |
| sequence_number | Integer | Event sequence number |
| event_type | String | Event type |
| failure_reason | String | Description of failure |
| created_at | Timestamp | Log entry timestamp |

### Query Examples

```sql
-- Overall pass rate
SELECT 
    COUNT(*) as total_checks,
    SUM(failure_count) as total_failures,
    (1 - SUM(failure_count) / SUM(total_rows)) * 100 as pass_rate_pct
FROM silver_quality_log
WHERE tenant_id IS NULL;

-- Top failing expectations
SELECT 
    expectation_name,
    severity,
    SUM(failure_count) as total_failures,
    AVG(CAST(REPLACE(failure_rate, '%', '') AS DOUBLE)) as avg_failure_rate
FROM silver_quality_log
WHERE tenant_id IS NULL
GROUP BY expectation_name, severity
ORDER BY total_failures DESC;

-- Most problematic tenants
SELECT 
    tenant_id,
    COUNT(DISTINCT expectation_name) as expectations_failed,
    COUNT(*) as total_failures
FROM silver_quality_log
WHERE tenant_id IS NOT NULL
GROUP BY tenant_id
ORDER BY total_failures DESC
LIMIT 10;

-- Recent failures
SELECT *
FROM silver_quality_log
WHERE tenant_id IS NOT NULL
ORDER BY run_timestamp DESC
LIMIT 20;
```

## Adding New Expectations

To add a new expectation, edit `databricks/silver/expectations.py`:

```python
from expectations import Expectation, ExpectationSeverity, SILVER_EXPECTATIONS

# Add to SILVER_EXPECTATIONS list
SILVER_EXPECTATIONS.append(
    Expectation(
        name="my_new_expectation",
        condition="my_column IS NOT NULL AND my_column > 0",
        severity=ExpectationSeverity.DROP,  # or WARN
        description="My column must be positive"
    )
)
```

### Condition Guidelines

- Write as SQL WHERE clause
- Rows **passing** the condition are valid
- Use standard SQL functions
- Reference column names directly
- Support NULL handling with `IS NULL OR`

### Choosing Severity

**DROP (Critical):**
- Required fields (IDs, keys)
- Data integrity constraints
- Must-have values for joins/queries
- Failures break downstream processing

**WARN (Non-Critical):**
- Data quality preferences
- Best practices
- Business rules that can be relaxed
- Issues that don't break functionality

## Integration with Bronze

Quality expectations complement Bronze validation:

**Bronze Layer:**
- Schema validation (JSON structure)
- Event type validation
- UUID format validation
- Timestamp validation
- Rejects to `bronze_rejected_events`

**Silver Layer:**
- Business rule validation
- Value range checks
- Enum validation
- Referential integrity
- Logs to `silver_quality_log`

**Workflow:**
1. Bronze catches malformed/corrupt events
2. Silver catches business rule violations
3. Both layers log issues for monitoring
4. Quality reports span both layers

## Monitoring

### Key Metrics

Track these in your monitoring dashboard:

1. **Overall Pass Rate**: % of events passing all expectations
2. **DROP Failure Rate**: % rejected due to critical issues
3. **WARN Failure Rate**: % with non-critical issues
4. **Top Failing Expectations**: Which rules fail most
5. **Tenant Quality Score**: Per-tenant pass rates
6. **Trend Over Time**: Quality improving or degrading

### Alerts

Set up alerts for:

- Pass rate drops below 95%
- DROP failures exceed 5%
- Same expectation fails consistently
- Specific tenant has repeated issues

### Quality SLA

Recommended targets:

- **Overall pass rate**: ≥ 98%
- **DROP failures**: ≤ 2%
- **WARN failures**: ≤ 10%
- **Time to resolution**: < 24 hours

## Troubleshooting

### High Failure Rates

1. **Check Bronze data quality**
   ```bash
   python databricks/bronze/check_bronze.py
   ```

2. **Identify problematic events**
   ```bash
   python databricks/silver/quality_report.py --problematic
   ```

3. **Review source data**
   - Check SQL Server cow_events table
   - Look for data entry issues
   - Validate upstream systems

4. **Fix and re-run**
   - Correct source data
   - Re-run Bronze ingestion
   - Re-run Silver resolution

### Expectation Too Strict

If an expectation rejects valid data:

1. Review the condition in `expectations.py`
2. Consider changing severity from DROP → WARN
3. Adjust condition to be more permissive
4. Document reason for change
5. Re-run Silver resolution

### Missing Quality Logs

If `silver_quality_log` is empty:

```bash
# Recreate table
python -c "
from pyspark.sql import SparkSession
from delta import configure_spark_with_delta_pip
from expectations import create_quality_log_table

spark = create_spark_session()
create_quality_log_table(spark)
spark.stop()
"

# Re-run resolution
python databricks/silver/resolve_cow_state.py --full-refresh
```

## Best Practices

1. **Start with DROP expectations for critical fields**
2. **Use WARN for business preferences**
3. **Review quality reports weekly**
4. **Investigate DROP failures immediately**
5. **Track WARN failures over time**
6. **Update expectations as business rules evolve**
7. **Document expectation rationale**
8. **Test new expectations before deploying**
9. **Monitor impact on throughput**
10. **Balance quality vs. availability**

## Performance

Expectations add minimal overhead:

- **Evaluation**: ~5-10% increase in processing time
- **Logging**: Async, doesn't block writes
- **Storage**: ~1KB per failure record

For large volumes (>1M events):

- Consider sampling for WARN expectations
- Batch quality log writes
- Partition quality log by date
- Aggregate stats rather than detail records

## Files

- **databricks/silver/expectations.py**: Expectation definitions and application logic
- **databricks/silver/quality_report.py**: Quality reporting tool
- **databricks/silver/test_quality_expectations.sh**: Test script
- **databricks/silver/README.md**: This documentation

## Related Documentation

- [Bronze Validation](../bronze/VALIDATION.md)
- [Silver SCD Type 2](./SCD_TYPE2.md)
- [Data Quality Best Practices](../../docs/DATA_QUALITY.md)
