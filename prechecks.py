import pandas as pd

# This class performs pre-checks on the data before it is loaded into the database.
class PreChecks:
    # The constructor takes a DataFrame and a table name for context in error messages.
    def __init__(self, df, table_name):
        self.df = df
        self.table_name = table_name
        self.status_code = 200
        self.status_message = 'OK'

    # This method checks for duplicate rows based on the specified key columns.
    def check_duplicates(self,key_columns):
        duplicated_values = self.df[self.df.duplicated(subset=key_columns)]
        if not duplicated_values.empty:
            self.status_message =f"Duplicates found in table '{self.table_name}' for keys {key_columns}"
            self.status_code = 409
            raise ValueError(f"[{self.status_code}] {self.status_message}")

    # This method checks for missing values in the specified key columns.
    def check_missing_keys(self, key_columns):
        for key in key_columns:
            if self.df[key].isnull().any():
                self.status_message = f"Missing values in {self.table_name}.{key}"
                self.status_code = 400
                print(f"[{self.status_code}] {self.status_message}")

    # This method runs all checks in sequence. If any check fails, it will raise an error or print a message.
    def run_all(self, key_colunms):
        self.check_duplicates(key_colunms)
        self.check_missing_keys(key_colunms)
        if self.status_code == 200:
            print(f"All checks passed for {self.table_name}")