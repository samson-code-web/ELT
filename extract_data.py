from datetime import datetime, timedelta
import gzip
import io
import logging
import httpx
import polars as pl

logger = logging.getLogger(f"elt.{__name__}")

def retrieve_data(start_date: datetime, end_date: datetime) -> io.BytesIO | None:
    delta = timedelta(hours=1)
    frames: list[pl.DataFrame] = []

    with httpx.Client(timeout=15.0) as client:
        current_date = start_date

        while current_date < end_date:
            date_part = current_date.strftime("%Y-%m-%d")
            hour_part = current_date.hour
            url = f"https://data.gharchive.org/{date_part}-{hour_part}.json.gz"
            logger.info("Downloading: %s", url)

            try:
                with client.stream("GET", url) as response:
                    response.raise_for_status()
                    raw_buffer = io.BytesIO()
                    for chunk in response.iter_raw(chunk_size=1024 * 1024):  # 1 MB chunks
                        raw_buffer.write(chunk)

                raw_buffer.seek(0)

                with gzip.GzipFile(fileobj=raw_buffer, mode="rb") as gz:
                    df_hour = pl.read_ndjson(gz, infer_schema_length=None)

                if not df_hour.is_empty():
                    frames.append(df_hour)

                raw_buffer.close()

            except httpx.HTTPStatusError as e:
                logger.warning("Skipping missing hour %s-%s: %s", date_part, hour_part, e)
            except httpx.RequestError as e:
                logger.error("Network error downloading %s-%s: %s", date_part, hour_part, e)
            except Exception as e:
                logger.exception("Failed parsing %s-%s: %s", date_part, hour_part, e)

            current_date += delta

    if not frames:
        logger.warning("No data retrieved between %s and %s", start_date, end_date)
        return None

    final_df = pl.concat(frames, how="diagonal_relaxed")
    frames.clear()

    parquet_buffer = io.BytesIO()
    logger.info("Writing to parquet")
    final_df.write_parquet(parquet_buffer, compression="snappy")
    logger.info("Parquet written ")
    parquet_buffer.seek(0)

    return parquet_buffer
