

## Usage



ReproZip is used to ensure the reproducibility of your test runs.

1. **Setup the ReproZip Environment**  
   Use the following command to set up the environment with `repro-test.rpz`:  
   ```bash
   reprounzip directory setup repro-test.rpz ./example
   ```

2. **Run Tests in the ReproZip Environment**  
   Execute the tests in the ReproZip environment:  
   ```bash
   reprounzip directory run ./example
   ```


The script `main.py` supports the following arguments:

### Running Tests
1. **Default Run**  
   Run all tests in the `test` directory (default behavior):  
   ```bash
   python3 main.py
   ```

2. **Run Tests in a Specific Directory**  
   Specify a directory to run tests:  
   ```bash
   python3 main.py --test-dir <TEST_DIR>
   ```

3. **Run a Specific Test File**  
   Run a specific test file in the `test` directory:  
   ```bash
   python3 main.py --file <FILE>
   ```

4. **List All Tests**  
   List all the tests available in the specified directory:  
   ```bash
   python3 main.py --list
   ```

