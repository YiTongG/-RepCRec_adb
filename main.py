from database_system import DatabaseSystem, load_test_file


def main():
    db = DatabaseSystem()
    db.initialize_system()
    test_file = "test/test12"
    commands = load_test_file(test_file)

    if commands:
        db.execute_test(commands)

        db.print_system_state()


if __name__ == "__main__":
    main()
