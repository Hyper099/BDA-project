"""Orchestrate the full data -> ETL -> training workflow."""
from data.generate_data import generate_dataset, save_dataset
from data.pyspark_pipeline import run_pipeline
from models.train_model import train_and_select_best
from utils.logger import get_logger

logger = get_logger(__name__)


def main() -> None:
    df = generate_dataset(num_rows=5000, seed=42)
    path, count = save_dataset(df)
    logger.info("Generated dataset: %s (%d rows)", path, count)

    processed = run_pipeline()
    logger.info("Processed data saved to: %s", processed)

    result = train_and_select_best()
    logger.info("Training complete. Best model: %s", result["best_model"])


if __name__ == "__main__":
    main()
