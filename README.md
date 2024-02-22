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

1.  Ensure that you have a Spark environment set up.
2.  Prepare your movie and rating data files (`movies.dat` and `ratings.dat`) and place them in the `data` directory.
3.  Run the `read_data.py` script to process the data:

    ```bash
    python read_data.py
    ```

4.  Check the `output` directory for the processed data files (`movie_ratings` and `top_movies`).

## Project Structure

```
newday_de_task/
│
├── data/ # Directory for input data files
│ ├── movies.dat # Movie data file
│ └── ratings.dat # Rating data file
│
├── output/ # Directory for output data files
│
├── src/ # Source code directory
│ └── read_data.py # Script to read and process data
│
├── tests/ # Directory for test files
│ └── test_data_processing.py # Integration test for data processing
│
├── .gitignore # Git ignore file
├── README.md # Project README file
└── requirements.txt # Project dependencies



```

## Testing

This project includes unit tests and integration tests using pytest. To run the tests:

```
pytest tests
```
