# Phase 7B.2 – Regression Execution Evidence

**Project:** `D:\DemandLens`
**Environment used:** `.venv` (Python 3.12.10)
**Command executed:**
```
D:\DemandLens\.venv\Scripts\python.exe -m pytest -q
```
**Interpreter version:**
```
Python 3.12.10 (tags/v3.12.10:0cc8128, Apr  8 2025, 12:21:36) [MSC v.1943 64 bit (AMD64)]
```
**Installed key packages (available in the virtual environment):**
- `pyspark 4.3.0`
- `pyarrow 26.0.1`
- `pytest 8.3.4`

**Test run outcome**
- **Exit code:** `1`
- **Result:** Errors during collection (3 errors), no tests were executed.
- **Error summary** (truncated):
```
ERROR data/temp_validation/run_pyarrow_test.py - PermissionError: [WinError 10013] An attempt was made to access a socket in a way forbidden by its access permissions
ERROR data/temp_validation/test_sql_regex.py - PermissionError: [WinError 10013] An attempt was made to access a socket in a way forbidden by its access permissions
ERROR scripts/test_committer.py - PermissionError: [WinError 10013] An attempt was made to access a socket in a way forbidden by its access permissions
```
The underlying cause is Spark’s attempt to start its Java gateway, which requires opening a local TCP socket. The current sandbox / execution environment blocks socket creation on Windows (`WinError 10013`). This prevents any test that initiates a Spark session from running.

**Implications**
- The full regression suite cannot be executed in the present environment without lifting the socket restriction or moving to a sandbox that permits Spark’s gateway.
- Consequently, we cannot independently confirm the previously claimed “PASS (116 tests)”.
- All other verification steps that do not require Spark (e.g., pure‑Python role‑standardizer checks) remain valid.

**Next steps** (outside the scope of this checkpoint):
1. Request the user or platform administrator to allow local socket creation for Spark, or run the tests in an environment where Spark is permitted.
2. Once the restriction is lifted, re‑run the same command to obtain full test results.
```
```
