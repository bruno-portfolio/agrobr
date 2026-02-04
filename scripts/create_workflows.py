"""Script para criar os workflows do GitHub Actions."""

import os
from pathlib import Path

WORKFLOWS_DIR = Path(__file__).parent.parent / ".github" / "workflows"

TESTS_YML = '''name: Tests

on:
  push:
    branches: [main, develop]
  pull_request:
    branches: [main]

jobs:
  test:
    runs-on: ubuntu-latest
    strategy:
      matrix:
        python-version: ["3.11", "3.12"]

    steps:
      - uses: actions/checkout@v4

      - name: Setup Python ${{ matrix.python-version }}
        uses: actions/setup-python@v5
        with:
          python-version: ${{ matrix.python-version }}
          cache: "pip"

      - name: Install dependencies
        run: |
          pip install -e ".[dev]"

      - name: Run linting
        run: |
          ruff check agrobr/ tests/
          ruff format --check agrobr/ tests/

      - name: Run type checking
        run: |
          mypy agrobr/

      - name: Run tests
        run: |
          pytest tests/ -v --cov=agrobr --cov-report=xml --cov-report=term-missing

      - name: Upload coverage
        uses: codecov/codecov-action@v4
        with:
          files: ./coverage.xml
          fail_ci_if_error: false
'''

HEALTH_CHECK_YML = '''name: Daily Health Check

on:
  schedule:
    - cron: '0 12 * * *'
    - cron: '0 0 * * *'
  workflow_dispatch:

jobs:
  health:
    runs-on: ubuntu-latest
    timeout-minutes: 15

    steps:
      - uses: actions/checkout@v4

      - name: Setup Python
        uses: actions/setup-python@v5
        with:
          python-version: '3.11'
          cache: 'pip'

      - name: Install dependencies
        run: |
          pip install -e ".[dev]"

      - name: Run health checks
        id: health
        run: |
          python scripts/fetch_structures.py --output health_report.json
          echo "report_path=health_report.json" >> $GITHUB_OUTPUT

      - name: Upload report
        uses: actions/upload-artifact@v4
        with:
          name: health-report-${{ github.run_number }}
          path: health_report.json
          retention-days: 30
'''

STRUCTURE_MONITOR_YML = '''name: Structure Monitor

on:
  schedule:
    - cron: '0 */6 * * *'
  workflow_dispatch:

jobs:
  monitor:
    runs-on: ubuntu-latest
    timeout-minutes: 10

    steps:
      - uses: actions/checkout@v4

      - name: Setup Python
        uses: actions/setup-python@v5
        with:
          python-version: '3.11'
          cache: 'pip'

      - name: Install dependencies
        run: pip install -e ".[dev]"

      - name: Fetch current structures
        run: |
          python scripts/fetch_structures.py --output current_structures.json

      - name: Compare with baseline
        id: compare
        run: |
          python scripts/compare_structures.py \\
            --baseline .structures/baseline.json \\
            --current current_structures.json \\
            --threshold 0.85 \\
            --output diff_report.json || echo "drift_detected=true" >> $GITHUB_OUTPUT

      - name: Alert on drift
        if: steps.compare.outputs.drift_detected == 'true'
        run: |
          python scripts/alert_structure_change.py diff_report.json
'''


def main():
    WORKFLOWS_DIR.mkdir(parents=True, exist_ok=True)

    (WORKFLOWS_DIR / "tests.yml").write_text(TESTS_YML)
    print(f"Created {WORKFLOWS_DIR / 'tests.yml'}")

    (WORKFLOWS_DIR / "health_check.yml").write_text(HEALTH_CHECK_YML)
    print(f"Created {WORKFLOWS_DIR / 'health_check.yml'}")

    (WORKFLOWS_DIR / "structure_monitor.yml").write_text(STRUCTURE_MONITOR_YML)
    print(f"Created {WORKFLOWS_DIR / 'structure_monitor.yml'}")


if __name__ == "__main__":
    main()
