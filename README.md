# Movie Data Processing with PySpark

This project demonstrates data processing tasks using PySpark for movie data analysis. It includes a script to read movie and rating data, perform transformations, and generate insights.

## Table of Contents

- [Installation](#installation)
- [Usage](#usage)
- [Project Structure](#project-structure)
- [Testing](#testing)

## Installation

1. Clone the repository:

   ```bash
   git clone https://github.com/your_username/newday_de_task.git

   ```

2. Install the required dependencies:

```bash
pip install -r requirements.txt
```

## Usage

1. Ensure that you have a Spark environment set up.
2. Prepare your movie and rating data files (`movies.dat` and `ratings.dat`) and place them in the `data/ml-1m/` directory.
3. Submit the `main.py` script to Spark for processing the data:

   ```bash
   spark-submit src/main.py
   ```

4. After the job has completed, check the `output/` directory for the processed data files.

5. To verify the integrity of the generated Parquet files, run the `check_parquet.py` script:

   ```bash
   python utils/check_parquet.py --file output/<name_of_your_parquet_file>
   ```

   Replace `<name_of_your_parquet_file>` with the actual name of the Parquet file you wish to check.

## Logging

This project uses logging to track the execution of data processing scripts. Log files are generated for each run and can be found in the `logs/` directory. Here's how the logging is structured:

- `main_<timestamp>.log`: Logs from the main script execution.
- `data_processing_<timestamp>.log`: Logs from the data processing tasks.
- `test_data_processing_<timestamp>.log`: Logs from running the test cases.

The `<timestamp>` in the filenames will help you identify the logs for each specific execution. You can review these logs to understand the flow of the data processing, monitor for any issues, and debug if necessary.

To configure logging levels or change the format of the log messages, you can modify the `src/logging_config.py` script.

## Project Structure

```
newday_de_task/
├── .github/ # CI/CD workflow configurations
├── data/
│   └── ml-1m/ # Directory for input data files
│       ├── movies.dat # Movie data file
│       └── ratings.dat # Rating data file
├── logs/ # Log files for data processing
├── newday_env/ # Python virtual environment directory
├── output/ # Directory for output data files
├── src/ # Source code directory
│   ├── data_io.py # Script for data input/output operations
│   ├── data_processing.py # Script for data processing
│   ├── logging_config.py # Configuration for logging
│   └── main.py # Main script to run the data processing
├── tests/ # Directory for test files
│   └── test_data_processing.py # Integration test for data processing
├── utils/ # Utility scripts
│   └── check_parquet.py # Script to check Parquet files
├── .gitignore # Git ignore file
├── README.md # Project README file
└── requirements.txt # Project dependencies



```

## Testing

This project includes unit tests and integration tests using pytest. To run the tests:

```
pytest tests
```

## Continuous Integration and Deployment (CI/CD)

This project is equipped with a CI/CD pipeline using GitHub Actions, which is defined in the `.github/workflows/` directory. The pipeline automates the process of running tests, checking code quality, and deploying the code to the production environment upon push or pull request events.

### Workflow Details

- `main.yml`: This workflow is triggered on every push or pull request to the main branch. It ensures that all tests pass and that code adheres to the project's standards before any new code is merged into the main codebase.

The workflows are configured to run automatically, but you can also trigger them manually if needed through the GitHub Actions tab on the repository's homepage.

### Managing Workflows

To modify the CI/CD process, you can edit the existing workflow file:

- Navigate to `.github/workflows/main.yml`
- Make your changes and commit them to the repository.

For more advanced CI/CD operations, such as deploying to a specific server or service, you may need to add additional steps or secrets to your workflow file, which can be managed in the repository settings under the 'Secrets' section.

Please refer to the [GitHub Actions documentation](https://docs.github.com/en/actions) for more detailed instructions on how to create custom workflows or modify the existing ones.
