#!/usr/bin/env python3
"""
Test runner for the Bitcoin data pipeline
Runs all tests and provides comprehensive reporting
"""

import subprocess
import sys
import os
import time
from datetime import datetime


def run_command(command, description):
    """Run a command and return success status"""
    print(f"\n{'='*60}")
    print(f"Running: {description}")
    print(f"Command: {command}")
    print(f"{'='*60}")
    
    start_time = time.time()
    try:
        result = subprocess.run(command, shell=True, capture_output=True, text=True)
        end_time = time.time()
        
        print(f"Exit code: {result.returncode}")
        print(f"Duration: {end_time - start_time:.2f} seconds")
        
        if result.stdout:
            print("STDOUT:")
            print(result.stdout)
        
        if result.stderr:
            print("STDERR:")
            print(result.stderr)
        
        return result.returncode == 0
    except Exception as e:
        print(f"Error running command: {e}")
        return False


def main():
    """Main test runner"""
    print("🚀 Bitcoin Data Pipeline Test Suite")
    print(f"Started at: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print("="*80)
    
    # Change to the correct directory
    os.chdir('/Users/davidcuellar/code/BlockBotics/aws-cdk')
    
    # Test results
    results = {}
    
    # 1. Unit Tests
    print("\n📋 Running Unit Tests...")
    unit_tests = [
        "python3 -m pytest tests/unit/test_data_lake_stack.py -v",
        "python3 -m pytest tests/unit/test_ingestion_stack.py -v",
        "python3 -m pytest tests/unit/test_observability_stack.py -v"
    ]
    
    for test in unit_tests:
        test_name = test.split('/')[-1].split('.')[0]
        results[test_name] = run_command(test, f"Unit Test: {test_name}")
    
    # 2. Integration Tests
    print("\n🔗 Running Integration Tests...")
    integration_tests = [
        "python3 -m pytest tests/integration/test_pipeline_integration.py -v",
        "python3 -m pytest tests/integration/test_data_quality.py -v",
        "python3 -m pytest tests/integration/test_performance.py -v"
    ]
    
    for test in integration_tests:
        test_name = test.split('/')[-1].split('.')[0]
        results[test_name] = run_command(test, f"Integration Test: {test_name}")
    
    # 3. Manual Tests
    print("\n🔧 Running Manual Tests...")
    manual_tests = [
        "python3 -m pytest tests/integration/test_pipeline_integration.py::TestPipelineIntegration::test_lambda_function_execution -v",
        "python3 -m pytest tests/integration/test_data_quality.py::TestDataQuality::test_data_completeness -v",
        "python3 -m pytest tests/integration/test_performance.py::TestPerformance::test_lambda_execution_time -v"
    ]
    
    for test in manual_tests:
        test_name = test.split('::')[-1]
        results[f"manual_{test_name}"] = run_command(test, f"Manual Test: {test_name}")
    
    # 4. Summary
    print("\n" + "="*80)
    print("📊 TEST SUMMARY")
    print("="*80)
    
    passed = 0
    failed = 0
    
    for test_name, success in results.items():
        status = "✅ PASS" if success else "❌ FAIL"
        print(f"{status} {test_name}")
        if success:
            passed += 1
        else:
            failed += 1
    
    print(f"\nTotal: {passed + failed} tests")
    print(f"Passed: {passed}")
    print(f"Failed: {failed}")
    print(f"Success Rate: {(passed / (passed + failed) * 100):.1f}%")
    
    # 5. Recommendations
    print("\n💡 RECOMMENDATIONS")
    print("="*80)
    
    if failed == 0:
        print("🎉 All tests passed! Your pipeline is working perfectly.")
        print("✅ Ready for production deployment")
        print("✅ Ready for technical test demonstration")
    else:
        print("⚠️ Some tests failed. Please review the issues:")
        for test_name, success in results.items():
            if not success:
                print(f"   - {test_name}")
        
        print("\n🔧 Suggested actions:")
        print("   1. Check AWS credentials and permissions")
        print("   2. Verify all resources are deployed")
        print("   3. Run individual tests to isolate issues")
        print("   4. Check CloudWatch logs for errors")
    
    print(f"\nCompleted at: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    
    # Exit with appropriate code
    sys.exit(0 if failed == 0 else 1)


if __name__ == "__main__":
    main()
