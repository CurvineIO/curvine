#!/usr/bin/env python3
# Copyright 2025 OPPO.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#
# Converts nextest JUnit XML output to build-server test_summary.json format
# and writes per-test log files under test_dir/logs/.

import argparse
import json
import os
import xml.etree.ElementTree as ET
from datetime import datetime
from pathlib import Path


def safe_filename(s: str) -> str:
    """Replace :: and other chars for log file names."""
    return s.replace("::", "_").replace("/", "_").replace("\\", "_")


def text_of(el) -> str:
    """Get element text and tail concatenated, or empty string."""
    if el is None:
        return ""
    return (el.text or "") + "".join(
        (c.tail or "") for c in el
    ).strip()


def parse_junit(junit_path: str):
    """Parse JUnit XML; return list of (package, test_file, test_case, status, log_content)."""
    tree = ET.parse(junit_path)
    root = tree.getroot()
    # Support both testsuites (root) and single testsuite
    if root.tag == "testsuites":
        suites = root.findall("testsuite")
    else:
        suites = [root] if root.tag == "testsuite" else []
    results = []
    for suite in suites:
        # classname on testsuite or first testcase: e.g. "curvine-tests::write_cache_test"
        suite_name = suite.get("name") or ""
        for tc in suite.findall("testcase"):
            classname = tc.get("classname") or suite_name
            name = tc.get("name") or "unknown"
            # Parse package and test_file from classname (e.g. "curvine-tests::write_cache_test")
            if "::" in classname:
                parts = classname.split("::", 1)
                package = parts[0]
                test_file = parts[1]
            else:
                package = classname or "workspace"
                test_file = "lib"
            # Status: skipped > failure > passed
            status = "PASSED"
            if tc.find("skipped") is not None:
                status = "SKIPPED"
            elif tc.find("failure") is not None or tc.find("error") is not None:
                status = "FAILED"
            # Collect system-out and system-err for log content
            out_el = tc.find("system-out")
            err_el = tc.find("system-err")
            log_content = ""
            if out_el is not None and out_el.text:
                log_content += out_el.text
            if err_el is not None and err_el.text:
                if log_content:
                    log_content += "\n"
                log_content += err_el.text
            if not log_content and status != "PASSED":
                fail_el = tc.find("failure") or tc.find("error")
                if fail_el is not None and fail_el.text:
                    log_content = fail_el.text
            results.append((package, test_file, name, status, log_content))
    return results


def main():
    parser = argparse.ArgumentParser(
        description="Convert nextest JUnit XML to build-server test_summary.json and per-test logs"
    )
    parser.add_argument(
        "test_dir",
        type=str,
        help="Test output directory (TEST_DIR); junit.xml and logs/ live here",
    )
    parser.add_argument(
        "--junit",
        type=str,
        default=None,
        help="Path to JUnit XML (default: <test_dir>/junit.xml)",
    )
    parser.add_argument(
        "--skipped-count",
        type=int,
        default=0,
        help="Number of skipped tests from nextest run (e.g. when using ci-no-ufs)",
    )
    parser.add_argument(
        "--skipped-list",
        type=str,
        default=None,
        help="Path to file with one skipped test per line (nextest list format: package::test_file test_case)",
    )
    args = parser.parse_args()
    test_dir = Path(args.test_dir)
    junit_path = args.junit or str(test_dir / "junit.xml")
    if not os.path.isfile(junit_path):
        print(f"Error: JUnit file not found: {junit_path}")
        return 1
    results = parse_junit(junit_path)
    # Aggregate by package
    pkg_stats = {}
    for pkg, _tf, _tc, status, log_content in results:
        if pkg not in pkg_stats:
            pkg_stats[pkg] = {"total": 0, "passed": 0, "failed": 0, "skipped": 0}
        pkg_stats[pkg]["total"] += 1
        if status == "PASSED":
            pkg_stats[pkg]["passed"] += 1
        elif status == "FAILED":
            pkg_stats[pkg]["failed"] += 1
        else:
            pkg_stats[pkg]["skipped"] += 1
    # Build packages list (same format as build-server; include skipped for dashboard)
    packages = []
    for name, st in sorted(pkg_stats.items()):
        total = st["total"]
        passed = st["passed"]
        failed = st["failed"]
        skipped = st["skipped"]
        success_rate = (passed * 100 // total) if total else 0
        packages.append({
            "name": name,
            "total": total,
            "passed": passed,
            "failed": failed,
            "skipped": skipped,
            "success_rate": success_rate,
        })
    # Build test_cases and write logs
    test_cases = []
    logs_dir = test_dir / "logs"
    logs_dir.mkdir(parents=True, exist_ok=True)
    for pkg, test_file, test_case, status, log_content in results:
        safe_tf = safe_filename(test_file)
        safe_tc = safe_filename(test_case)
        log_name = f"{safe_tf}_{safe_tc}.log"
        pkg_log_dir = logs_dir / pkg
        pkg_log_dir.mkdir(parents=True, exist_ok=True)
        log_path = pkg_log_dir / log_name
        with open(log_path, "w", encoding="utf-8") as f:
            f.write(log_content or f"(no output) {status}\n")
        rel_log = str(Path("logs") / pkg / log_name)
        test_cases.append({
            "package": pkg,
            "test_file": test_file,
            "test_case": test_case,
            "status": status,
            "log": rel_log,
        })
    total_tests = len(results)
    passed_tests = sum(1 for r in results if r[3] == "PASSED")
    failed_tests = sum(1 for r in results if r[3] == "FAILED")
    skipped_in_junit = sum(1 for r in results if r[3] == "SKIPPED")
    skipped_tests = skipped_in_junit + args.skipped_count
    success_rate = (passed_tests * 100 // total_tests) if total_tests else 0
    summary = {
        "timestamp": datetime.now().isoformat(),
        "total_tests": total_tests,
        "passed_tests": passed_tests,
        "failed_tests": failed_tests,
        "success_rate": success_rate,
        "packages": packages,
        "test_cases": test_cases,
    }
    if skipped_tests > 0:
        summary["skipped_tests"] = skipped_tests
    # Parse skipped list file (from profile filter) for UI to show skipped test names
    skipped_test_cases = []
    if args.skipped_list and os.path.isfile(args.skipped_list):
        with open(args.skipped_list, encoding="utf-8") as f:
            for line in f:
                line = line.strip()
                if not line:
                    continue
                # Format: "package::test_file test_case" (first space separates binary from test name)
                parts = line.split(" ", 1)
                if len(parts) != 2:
                    continue
                binary_part, test_case = parts[0], parts[1]
                if "::" in binary_part:
                    pkg_parts = binary_part.split("::")
                    package = pkg_parts[0]
                    test_file = pkg_parts[-1]
                else:
                    package = binary_part
                    test_file = "lib"
                skipped_test_cases.append({
                    "package": package,
                    "test_file": test_file,
                    "test_case": test_case,
                })
    if skipped_test_cases:
        summary["skipped_test_cases"] = skipped_test_cases
    summary_path = test_dir / "test_summary.json"
    with open(summary_path, "w", encoding="utf-8") as f:
        json.dump(summary, f, indent=2, ensure_ascii=False)
    print(f"Wrote {summary_path} (total={total_tests}, passed={passed_tests}, failed={failed_tests}, success_rate={success_rate}%)")
    if skipped_tests > 0:
        print(f"Skipped: {skipped_tests} (from JUnit: {skipped_in_junit}, not run: {args.skipped_count})")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
