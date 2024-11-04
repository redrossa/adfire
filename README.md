# Adfire: Accounting platform for achieving FIRE

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
   pip install -r requirements.txt
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
