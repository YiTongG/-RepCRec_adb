
import os
import sys
import argparse
from database_system import DatabaseSystem, load_test_file


def list_test_files(test_dir):
    """
    list all test files in the test directory

    Args:
        test_dir (str): directory path

    Returns:
        list:`
    """
    if not os.path.exists(test_dir):
        print(f"error dit '{test_dir}' not was found")
        return []

    test_files = []
    for file in os.listdir(test_dir):
        file_path = os.path.join(test_dir, file)
        if os.path.isfile(file_path):
            test_files.append(file)
    return test_files


def run_test(test_file):
    """


    Args:
        test_file (str)
    """
    db = DatabaseSystem()
    db.initialize_system()
    commands = load_test_file(test_file)

    if commands:
        print(f"\ntesting: {test_file}")
        print("-" * 50)
        db.execute_test(commands)
        db.print_system_state()


def main():
    # cmd arguments
    parser = argparse.ArgumentParser(description='advanced-database-systems')
    parser.add_argument('--test-dir', default='test', help='default: test)')
    parser.add_argument('--file', help='test file to run')
    parser.add_argument('--list', action='store_true', help='list all test files')

    args = parser.parse_args()

    # default test directory
    test_files = list_test_files(args.test_dir)

    # list all test files
    if args.list:
        if test_files:
            print("\ntest file:")
            for file in test_files:
                print(f"- {file}")
        else:
            print("test files not found")
        return

    # read test file name from command line
    if args.file:
        if args.file in test_files:
            test_file_path = os.path.join(args.test_dir, args.file)
            run_test(test_file_path)
        else:
            print(f"error:  '{args.file}' not exists in test files")
            if test_files:
                print("\ntest files:")
                for file in test_files:
                    print(f"- {file}")
        return

    #
    if test_files:
        for file in test_files:
            test_file_path = os.path.join(args.test_dir, file)
            run_test(test_file_path)
    else:
        print("error: no test files found")


if __name__ == "__main__":
    main()