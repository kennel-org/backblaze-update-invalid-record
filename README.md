# Update Backblaze Drive Stats Invalid Record

This project is designed to update invalid records in the Backblaze drive statistics database. It connects to a MariaDB database, performs various data updates, and outputs the results to CSV files.

## Prerequisites

- R environment with the following packages installed:
  - `RMariaDB`
  - `tictoc`
  - `yaml`

## Configuration

The project uses a configuration file `config.yml` to store database connection details. A sample configuration file `config.sample.yml` is provided as a template. You need to create a `config.yml` file with your database credentials:

```yaml
db_connection:
  username: your_username
  password: your_password
  host: your_host
  port: your_port
  dbname: your_dbname
```

## Usage

1. Ensure that the required directories (`outfile`, `infile`, `tmp`) exist. The script will create them if they are missing.
2. Run the `update_invalid_record.R` script using R or RStudio.
3. The script will:
   - Connect to the MariaDB database using the credentials provided in `config.yml`.
   - Perform various SQL updates to correct invalid records in the `drive_stats` table.
   - Output initial and final status reports to CSV files in the `outfile` directory.

## Output

The script generates several CSV files in the `outfile` directory, including:
- Initial and final status reports of the `drive_stats` table.
- A report of records with invalid capacity values.

## License

This project is licensed under the MIT License.
