def write_snapshot(self, df: pd.DataFrame, max_retries: int = 3) -> int:
    if df.empty:
        logger.warning("write_snapshot called with empty DataFrame.")
        return 0

    from google.cloud.bigquery import LoadJobConfig, WriteDisposition

    job_config = LoadJobConfig(

        write_disposition = WriteDisposition.WRITE_APPEND,
    )

    logger.info("Writing %d rows to %s via load job...", len(df), self.table_ref)

    job = self._client.load_table_from_dataframe(
        df, self.table_ref, job_config=job_config
    )
    job.result()  

    logger.info("Load job complete. Rows written: %d", len(df))
    return len(df)
