from pathlib import Path
from typing import List, Dict, Any
import pandas as pd
from loguru import logger


class Results:
    def __init__(
        self,
        results_dir: Path,
        file_name: str,
        force_write: bool = False,
    ):
        self.results_dir = results_dir
        self.results_dir.mkdir(parents=True, exist_ok=True)
        self._records: List[Dict[str, Any]] = []
        self.force_write = force_write
        self.file_name = file_name
        self.out_path = self.results_dir / self.file_name
    def check_file_exists(self):
        if self.out_path.exists() and not self.force_write:
            return True
        return False

    def __iter__(self):
        return iter(self._records)

    def __next__(self):
        return next(self._records)

    def __len__(self):
        return len(self._records)

    def __enter__(self):
        logger.debug(f"Entering Results context. self.out_path: {self.out_path}")
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        logger.debug(f"Exiting Results context. self.out_path: {self.out_path}")
        self.save()

    def get_top(
        self, key: str, descending: bool = True, n: int = 1
    ) -> List[Dict[str, Any]]:
        """Get the top N results."""
        df = self.to_df()
        return (
            df.sort_values(by=key, ascending=not descending)
            .iloc[:n]
            .to_dict(orient="records")
        )

    def get_random(self, n: int = 1) -> List[Dict[str, Any]]:
        """Get a random sample of N results."""
        df = self.to_df()
        return df.sample(n).to_dict(orient="records")

    def get_head(self, n: int = 1) -> List[Dict[str, Any]]:
        """Get the first N results."""
        df = self.to_df()
        return df.head(n).to_dict(orient="records")

    def get_all(self) -> List[Dict[str, Any]]:
        """Get all results."""
        return self.to_df().to_dict(orient="records")

    def add(self, record: Dict[str, Any]):
        """Add a new result record."""
        assert isinstance(record, dict), "Record must be a dictionary"
        self._records.append(record)
        logger.debug(f"Added result: {record}")

    def to_df(self) -> pd.DataFrame:
        """Convert results to a pandas DataFrame."""
        return pd.DataFrame(self._records)

    def find(self, key: str, value: Any) -> Dict[str, Any]:
        """Find a record by key and value."""
        return next((record for record in self._records if record[key] == value), None)

    def find_all(self, key: str, value: Any) -> List[Dict[str, Any]]:
        """Find all records by key and value."""
        return [record for record in self._records if record[key] == value]

    def show(self, n: int = 5):
        """Display the top N rows of the results."""
        df = self.to_df()
        print(df.head(n))

    def from_csv(self, csv_path: Path):
        """Load results from a CSV file."""
        df = pd.read_csv(csv_path)
        self._records = df.to_dict(orient="records")
        return self

    def load(self):
        """Load the results from the CSV file."""
        if self.check_file_exists() and not self._records:
            df = pd.read_csv(self.out_path)
            self._records = df.to_dict(orient="records")
        return self

    def save(self):
        """Save the results as a CSV file."""
        if self.check_file_exists():
            return
        try:
            df = self.to_df()
            df.to_csv(self.out_path, index=False)
            logger.info(f"Results saved to CSV: {self.out_path}")
        except Exception as e:
            logger.error(f"Failed to save results to CSV: {e}")
