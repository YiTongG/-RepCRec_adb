

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
   it contains 28 tests checking using info from reprounzip
   ```bash
   reprounzip  info  repro-test.rpz
   ```
   from course website under the `test` directory. 
   The running result will show up in the terminal.

The script `main.py` supports identifying test and showing the result in the terminal.


### Running Tests
1. **Default Run**  
   Run all tests in the `test` directory (default behavior):  
   ```bash
   python3 main.py
   ```

2. **Run Tests in a Specific Directory**  
   Specify a directory to run tests:  default is `test`
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

