# Silver Layer Data Quality Expectations - Quick Reference

## Summary

- **Total Expectations**: 14
- **DROP (Critical)**: 6 expectations
- **WARN (Non-Critical)**: 8 expectations

---

## DROP Expectations (Critical Failures - Data Rejected)

### 1. valid_cow_id
- **Condition**: `cow_id IS NOT NULL`
- **Description**: Cow ID must not be null
- **Why Critical**: Required for joining and tracking

### 2. valid_tenant_id
- **Condition**: `tenant_id IS NOT NULL`
- **Description**: Tenant ID must not be null
- **Why Critical**: Required for multi-tenancy isolation

### 3. valid_tag_number
- **Condition**: `tag_number IS NOT NULL AND LENGTH(TRIM(tag_number)) > 0`
- **Description**: Tag number must not be null or empty
- **Why Critical**: Primary identifier for physical cow

### 4. valid_status
- **Condition**: `status IN ('active', 'inactive')`
- **Description**: Status must be 'active' or 'inactive'
- **Why Critical**: Required for filtering current vs historical cows

### 5. valid_start_at
- **Condition**: `__START_AT IS NOT NULL`
- **Description**: Start timestamp must not be null
- **Why Critical**: Required for SCD Type 2 temporal tracking

### 6. valid_sequence
- **Condition**: `__SEQUENCE_NUMBER IS NOT NULL AND __SEQUENCE_NUMBER > 0`
- **Description**: Sequence number must be positive
- **Why Critical**: Required for event ordering

---

## WARN Expectations (Non-Critical - Logged But Allowed)

### 7. valid_breed
- **Condition**: `breed IN (39 recognized breeds)`
- **Description**: Breed should be one of 39 recognized breeds
- **Why WARN**: New breeds may be added; typos can be corrected later
- **Valid Values**: 
  - Beef: Angus, Hereford, Charolais, Simmental, Limousin, Gelbvieh, Red Angus, Brahman, Brangus, Beefmaster, Shorthorn, Longhorn, Highland, Belted Galloway, Devon, Piedmontese, Chianina, Blonde d'Aquitaine, Salers, Tarentaise, Maine-Anjou, Marchigiana, Romagnola, Santa Gertrudis, Senepol, Wagyu, Murray Grey, Dexter, Galloway, Welsh Black
  - Dairy: Holstein, Jersey, Guernsey, Brown Swiss, Ayrshire
  - Other: Mixed, Crossbred, Other, Unknown

### 8. valid_birth_date
- **Condition**: `birth_date >= '2000-01-01' AND birth_date <= CURRENT_DATE`
- **Description**: Birth date should be between 2000-01-01 and today
- **Why WARN**: Historical records may have older dates; data entry errors can be fixed

### 9. valid_weight
- **Condition**: `weight_kg IS NULL OR (weight_kg > 0 AND weight_kg < 2000)`
- **Description**: Weight should be between 0 and 2000 kg (or null)
- **Why WARN**: Extreme values are rare but possible; data can be corrected

### 10. valid_sex
- **Condition**: `sex IS NULL OR sex IN ('male', 'female')`
- **Description**: Sex should be 'male', 'female', or null
- **Why WARN**: Missing sex data doesn't prevent tracking

### 11. reasonable_birth_weight
- **Condition**: `birth_weight_kg IS NULL OR (birth_weight_kg > 10 AND birth_weight_kg < 100)`
- **Description**: Birth weight should be between 10 and 100 kg (or null)
- **Why WARN**: Optional field; data quality issue but not critical

### 12. reasonable_name_length
- **Condition**: `name IS NULL OR LENGTH(name) <= 100`
- **Description**: Name should be 100 characters or less
- **Why WARN**: Very long names are unusual but not invalid

### 13. valid_color
- **Condition**: `color IN (12 recognized colors) OR NULL`
- **Description**: Color should be a recognized value
- **Why WARN**: Optional field; new colors may emerge
- **Valid Values**: Black, Red, White, Brown, Grey, Yellow, Cream, Brindle, Roan, Spotted, Mixed, Other

### 14. valid_deactivation_reason
- **Condition**: `deactivation_reason IN ('sold', 'deceased', 'transferred', 'culled', 'exported', 'other') OR NULL`
- **Description**: Deactivation reason should be a recognized value
- **Why WARN**: Only relevant for inactive cows; new reasons may be added
- **Valid Values**: sold, deceased, transferred, culled, exported, other

---

## Testing Scenarios

### ✅ Should PASS All Expectations
```json
{
  "tag_number": "A-12345",
  "breed": "Angus",
  "sex": "female",
  "birth_date": "2022-01-15",
  "weight_kg": 450.0,
  "status": "active"
}
```

### ⚠️ Should WARN (Invalid Breed)
```json
{
  "tag_number": "A-12346",
  "breed": "AlienBreed",  // Not in list
  "sex": "female",
  "birth_date": "2022-01-15",
  "weight_kg": 450.0,
  "status": "active"
}
```

### ⚠️ Should WARN (Old Birth Date)
```json
{
  "tag_number": "A-12347",
  "breed": "Hereford",
  "sex": "male",
  "birth_date": "1995-01-15",  // Before 2000
  "weight_kg": 500.0,
  "status": "active"
}
```

### ⚠️ Should WARN (Excessive Weight)
```json
{
  "tag_number": "A-12348",
  "breed": "Charolais",
  "sex": "male",
  "birth_date": "2021-01-15",
  "weight_kg": 2500.0,  // > 2000 kg
  "status": "active"
}
```

### 🔴 Should FAIL (Missing Tag Number)
```json
{
  "breed": "Angus",
  "sex": "female",
  "birth_date": "2022-01-15",
  "weight_kg": 450.0,
  "status": "active"
  // Missing tag_number - CRITICAL!
}
```

### 🔴 Should FAIL (Invalid Status)
```json
{
  "tag_number": "A-12350",
  "breed": "Angus",
  "sex": "female",
  "birth_date": "2022-01-15",
  "weight_kg": 450.0,
  "status": "INVALID_STATUS"  // Not 'active' or 'inactive'
}
```

---

## Quality Thresholds

### Target Quality Metrics

| Metric | Target | Alert If |
|--------|--------|----------|
| Overall Pass Rate | ≥ 98% | < 95% |
| DROP Failure Rate | ≤ 2% | > 5% |
| WARN Failure Rate | ≤ 10% | > 20% |
| Breed Validity | ≥ 95% | < 90% |
| Date Validity | ≥ 98% | < 95% |

### Severity Decision Matrix

| Characteristic | DROP | WARN |
|----------------|------|------|
| Required for joins | ✓ | |
| System-critical | ✓ | |
| Data integrity | ✓ | |
| Business rule | | ✓ |
| Optional field | | ✓ |
| Can be corrected later | | ✓ |
| Breaks downstream | ✓ | |
| Quality preference | | ✓ |

---

## Common Failure Patterns

### Pattern 1: Typos in Breed Names
- **Expectation**: valid_breed
- **Examples**: "Anguss", "Holstien", "hereford"
- **Impact**: WARN - data written, quality logged
- **Resolution**: Fix in source system, map common typos

### Pattern 2: Future Birth Dates
- **Expectation**: valid_birth_date
- **Examples**: "2030-01-15"
- **Impact**: WARN - data written, quality logged
- **Resolution**: Validate date entry forms

### Pattern 3: Missing Tag Numbers
- **Expectation**: valid_tag_number
- **Examples**: null, "", "   "
- **Impact**: DROP - data rejected
- **Resolution**: Make tag_number mandatory in source

### Pattern 4: Invalid Status Values
- **Expectation**: valid_status
- **Examples**: "deleted", "archived", "pending"
- **Impact**: DROP - data rejected
- **Resolution**: Map legacy status values

### Pattern 5: Negative Weights
- **Expectation**: valid_weight
- **Examples**: -50.0, -100.0
- **Impact**: WARN - data written, quality logged
- **Resolution**: Add validation in data entry

---

## Monitoring Queries

### Daily Quality Check
```sql
SELECT 
    DATE(run_timestamp) as date,
    COUNT(*) as runs,
    AVG((total_rows - failure_count) / total_rows * 100) as avg_pass_rate
FROM silver_quality_log
WHERE tenant_id IS NULL
GROUP BY DATE(run_timestamp)
ORDER BY date DESC
LIMIT 7;
```

### Failing Expectations
```sql
SELECT 
    expectation_name,
    severity,
    COUNT(*) as failure_occurrences,
    SUM(failure_count) as total_failures
FROM silver_quality_log
WHERE failure_count > 0
GROUP BY expectation_name, severity
ORDER BY total_failures DESC;
```

### Tenant Quality Scores
```sql
SELECT 
    tenant_id,
    COUNT(*) as failures,
    COUNT(DISTINCT expectation_name) as expectations_failed
FROM silver_quality_log
WHERE tenant_id IS NOT NULL
GROUP BY tenant_id
ORDER BY failures DESC
LIMIT 10;
```

---

## Files

- **expectations.py** - Expectation definitions and application logic
- **quality_report.py** - Quality reporting and analysis
- **test_quality_expectations.sh** - Test script with invalid data
- **README.md** - Full documentation
- **EXPECTATIONS_REFERENCE.md** - This quick reference
