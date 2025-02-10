# Adfire: Accounting platform for achieving FIRE

![Unit Tests](https://github.com/redrossa/adfire/actions/workflows/unit-tests.yaml/badge.svg)

Adfire is a tool to assist personal finance enthusiasts in implementing the FIRE strategy in their lifestyle. This tool
provides a platform for users to record income, expenses and savings, and quickly build custom analyses tailored to 
their needs, or use and share other users' projections.

## Setting up local development environment

1. Clone the repository

   ```shell
   git clone https://github.com/redrossa/adfire.git
   ```

2. Navigate to the project directory

   ```shell
   cd adfire
   ```

3. Set up Python virtual environment

   ```shell
   python -m venv venv
   ```

4. Activate virtual environment

   - On Windows:
     ```shell
     venv\Scripts\activate
     ```
   - On macOS/Linux:
     ```shell
     source venv/bin/activate
     ```

5. Install dependencies

   ```shell
   pip install .
   pip install .[tests]
   ```

6. Install Adfire as a package

   ```shell
   pip install -e .
   ```
   
7. Verify installation

   ```shell
   adfire --version
   ```
8. Run tests

   ```shell
   pytest
   ```

## Getting started with the CLI

1. In an empty directory, initialize with a sample portfolio

   ```shell
   adfire init
   ```

3. Record entries in CSV files, any directory item (and its children) preceded with '.' is ignored

3. Verify records to see errors

   ```shell
   adfire lint
   ```
   
4. Clean up and format records

   ```shell
   adfire format
   ```

5. View reports using PIP installed module packages

   ```shell
   adfire view <MODULE_NAME>
   ```

## Writing custom view modules

1. Create a Python package

2. Add `__main__.py`

   1. Define `global portfolio`, which is the `Portfolio` instance passed from Adfire CLI
   2. Define entrypoint for `__name__ == '__main__'`
   3. Analyze portfolio and output custom reports

3. Install the package with PIP

   ```shell
   pip install -e /path/to/package
   ```

4. Try it out in a formatted portfolio

   ```shell
   adfire view <PACKAGE_NAME>
   ```
