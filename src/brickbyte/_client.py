"""
Internal Client class for brickbyte.
"""
import logging
import os
import shutil
import subprocess
import threading
import uuid
from pathlib import Path
from typing import Callable, Dict, List, Optional, Union

from brickbyte import SyncResult

logger = logging.getLogger("brickbyte")


class VirtualEnvManager:
    """Manages isolated Python virtual environments for source connectors."""

    def __init__(self, env_dir: str):
        self.env_dir = env_dir

    def create_virtualenv(self):
        import virtualenv

        virtualenv.cli_run([self.env_dir])

    def install_source(self, source: str, override_install: Optional[str] = None):
        library = override_install or f"airbyte-{source}"
        subprocess.check_call(
            [os.path.join(self.env_dir, "bin", "pip"), "install", library],
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
        )

    def delete_virtualenv(self):
        if os.path.exists(self.env_dir):
            shutil.rmtree(self.env_dir)

    @property
    def bin_path(self):
        return os.path.join(self.env_dir, "bin")


class Client:
    """
    brickbyte Client - Sync data from any source connector to Databricks.

    Uses a streaming architecture to bypass local disk storage and
    write directly to Unity Catalog.

    Supports automatic credential discovery from Databricks Secrets:
        - Default scope: "brickbyte"
        - Key convention: "{source-name}/{field}" (e.g., "source-s3/aws_access_key_id")
        - Optional YAML profiles for credential reuse across sources
    """

    def __init__(
        self,
        base_venv_directory: Optional[str] = None,
        secrets_scope: str = "brickbyte",
        profiles: Optional[str] = None,
    ):
        self._base_venv_directory = base_venv_directory or str(Path.home())
        self._source_env_managers: Dict[str, VirtualEnvManager] = {}

        from brickbyte.credentials import CredentialResolver

        self._credential_resolver = CredentialResolver(
            secrets_scope=secrets_scope,
            profiles_path=profiles,
        )

    def _setup_source(self, source: str, source_install: Optional[str] = None):
        """Install source connector in isolated venv."""
        if source in self._source_env_managers:
            return

        path = os.path.join(self._base_venv_directory, f"brickbyte-{source}")
        manager = VirtualEnvManager(path)
        manager.create_virtualenv()
        manager.install_source(source, source_install)
        self._source_env_managers[source] = manager

    def _get_source_exec_path(self, source: str) -> str:
        """Get path to source connector executable."""
        return os.path.join(self._source_env_managers[source].bin_path, source)

    def _validate_sync_params(self, mode: str):
        """Validate sync parameters."""
        valid_modes = ("append", "overwrite")
        if mode not in valid_modes:
            if mode == "merge":
                raise NotImplementedError("Merge mode is not yet supported.")
            raise ValueError(
                f"Invalid mode '{mode}'. Must be one of: {', '.join(valid_modes)}"
            )

    def preview(
        self,
        source: str,
        source_config: dict,
        catalog: str,
        schema: str,
        streams: Optional[List[str]] = None,
        source_install: Optional[str] = None,
        sample_size: int = 5,
    ):
        """
        Preview a sync operation.

        Args:
            source: Source connector name
            source_config: Configuration dictionary for the source
            catalog: Unity Catalog name
            schema: Target schema name
            streams: List of streams to preview (None = all streams)
            source_install: Override source installation
            sample_size: Number of sample records per stream

        Returns:
            PreviewResult with detailed comparison
        """
        import airbyte as ab

        from brickbyte.preview import PreviewEngine

        merged_config = self._credential_resolver.merge_credentials(source, source_config)

        try:
            logger.info(f"Setting up {source}...")
            self._setup_source(source, source_install)

            ab_source = ab.get_source(
                source,
                config=merged_config,
                local_executable=self._get_source_exec_path(source),
            )
            ab_source.check()

            if streams:
                ab_source.select_streams(streams)
            else:
                ab_source.select_all_streams()

            selected = list(ab_source.get_selected_streams())

            logger.info("Generating preview (streaming)...")
            engine = PreviewEngine(catalog=catalog, schema=schema)
            result = engine.preview(
                ab_source=ab_source,
                streams=selected,
                sample_size=sample_size,
            )

            return result

        finally:
            self.cleanup()

    def sync(
        self,
        source: str,
        source_config: dict,
        catalog: str,
        schema: str,
        staging_volume: Optional[str] = None,
        streams: Optional[List[str]] = None,
        mode: str = "overwrite",
        flatten: bool = False,
        enrich_metadata: bool = False,
        enrich_model: Optional[str] = None,
        warehouse_id: Optional[str] = None,
        source_install: Optional[str] = None,
        cleanup: bool = False,
        buffer_size_records: int = 50000,
        buffer_size_mb: int = 100,
        continue_on_error: bool = False,
        timeout_seconds: Optional[int] = None,
        incremental: bool = False,
        deduplicate: bool = False,
        dedup_keys: Optional[Union[List[str], Dict[str, List[str]]]] = None,
        max_parallel_streams: int = 1,
        progress_callback: Optional[Callable] = None,
    ) -> SyncResult:
        """
        Sync data from a source connector to Databricks (Streaming).

        Args:
            source: Source connector name (e.g., "source-github")
            source_config: Configuration dictionary for the source connector
            catalog: Unity Catalog name (e.g., "main")
            schema: Target schema name (e.g., "bronze")
            staging_volume: Unity Catalog Volume path (REQUIRED for remote)
            streams: List of streams to sync. None = all streams (default)
            mode: Write mode ("overwrite" or "append")
            flatten: If True, flatten record fields into columns.
                    If False (default), store as JSON in 'data' column.
            enrich_metadata: If True, use AI to generate column descriptions
            enrich_model: Foundation Model endpoint for enrichment
            warehouse_id: SQL warehouse ID (optional, auto-discovered)
            source_install: Override source installation (e.g., custom git URL)
            cleanup: Whether to cleanup venvs after sync (default: False)
            buffer_size_records: Records per micro-batch (default: 50k)
            buffer_size_mb: Max batch size in MB (default: 100MB)
            continue_on_error: If True, continue syncing other streams if one fails
            timeout_seconds: Optional timeout in seconds for the sync operation
            incremental: If True, use incremental sync with state management
            deduplicate: If True, deduplicate records after sync
            dedup_keys: Column(s) to use as dedup keys (required when deduplicate=True)
            max_parallel_streams: Max number of streams to write in parallel (default: 1)
            progress_callback: Optional callback for progress reporting

        Returns:
            SyncResult with records_written, streams_synced, failed_streams, enriched_tables
        """
        import airbyte as ab

        from brickbyte._sanitize import sanitize_stream_name
        from brickbyte.writers import create_streaming_writer

        self._validate_sync_params(mode)
        merged_config = self._credential_resolver.merge_credentials(source, source_config)

        run_id = str(uuid.uuid4())

        # Normalize dedup_keys if deduplicate is enabled
        normalized_dedup_keys = None
        if deduplicate:
            normalized_dedup_keys = self._normalize_dedup_keys(dedup_keys, streams)

        # Set up timeout
        cancel_event = None
        timer = None
        if timeout_seconds is not None:
            cancel_event = threading.Event()
            timer = threading.Timer(timeout_seconds, cancel_event.set)
            timer.daemon = True
            timer.start()

        writer = None
        try:
            logger.info(f"Setting up {source}...")
            self._setup_source(source, source_install)

            logger.info(f"Configuring {source}...")
            ab_source = ab.get_source(
                source,
                config=merged_config,
                local_executable=self._get_source_exec_path(source),
            )

            logger.info("Validating source connection...")
            ab_source.check()

            if streams:
                ab_source.select_streams(streams)
            else:
                ab_source.select_all_streams()

            selected = list(ab_source.get_selected_streams())

            # Sanitize stream names upfront and check for collisions
            sanitized_map = {}
            for stream in selected:
                sanitized = sanitize_stream_name(stream)
                if sanitized in sanitized_map and sanitized_map[sanitized] != stream:
                    raise ValueError(
                        f"Stream name collision after sanitization: "
                        f"'{sanitized_map[sanitized]}' and '{stream}' both map to '{sanitized}'"
                    )
                sanitized_map[sanitized] = stream

            # If dedup_keys is a dict, validate keys match selected streams (original names)
            if deduplicate and isinstance(normalized_dedup_keys, dict):
                for dk_stream in normalized_dedup_keys:
                    if dk_stream == "__all__":
                        continue
                    if dk_stream not in selected:
                        # Check if user used sanitized name by mistake
                        # sanitized_map: sanitized_name -> original_name
                        if dk_stream in sanitized_map:
                            orig = sanitized_map[dk_stream]
                            raise ValueError(
                                f"dedup_keys key '{dk_stream}' is a sanitized name. "
                                f"Use the original Airbyte stream name '{orig}' instead."
                            )
                        raise ValueError(
                            f"dedup_keys key '{dk_stream}' does not match any selected stream. "
                            f"Selected streams: {selected}"
                        )

            # Load incremental state if needed
            state_manager = None
            if incremental:
                from brickbyte._state import StateManager

                state_manager = StateManager(catalog=catalog, schema=schema)
                # TODO: load and pass state to PyAirbyte

            via_msg = f" via {staging_volume}" if staging_volume else " (Native Spark)"
            logger.info(
                f"Streaming {len(selected)} streams to {catalog}.{schema}{via_msg}..."
            )

            # Set up progress reporter
            progress_reporter = None
            if progress_callback is not None:
                from brickbyte._progress import ProgressReporter

                progress_reporter = ProgressReporter(
                    total_streams=len(selected),
                    callback=progress_callback,
                )

            total_records = 0
            failed_streams: List[str] = []
            successful_streams: List[str] = []
            lock = threading.Lock()

            if max_parallel_streams > 1:
                import concurrent.futures

                executor = concurrent.futures.ThreadPoolExecutor(
                    max_workers=max_parallel_streams
                )
                futures = []
                in_flight = 0
                in_flight_lock = threading.Lock()

                def _write_stream_records(stream_name, records_list, _run_id, _mode):
                    """Write a list of records in a thread-owned writer."""
                    thread_writer = create_streaming_writer(
                        catalog=catalog,
                        schema=schema,
                        staging_volume=staging_volume,
                        warehouse_id=warehouse_id,
                        buffer_size_records=buffer_size_records,
                        buffer_size_mb=buffer_size_mb,
                        flatten=flatten,
                        run_id=_run_id,
                        dedup_keys=normalized_dedup_keys,
                    )
                    try:
                        if _mode == "overwrite":
                            thread_writer.safe_overwrite_begin(stream_name, _run_id)

                        for record in records_list:
                            thread_writer.write_record(stream_name, record)
                        thread_writer.flush_stream(stream_name)

                        if _mode == "overwrite":
                            thread_writer.safe_overwrite_finish(stream_name, _run_id)

                        return len(records_list)
                    finally:
                        thread_writer.close()
                        with in_flight_lock:
                            nonlocal in_flight
                            in_flight -= 1

                for stream_name in selected:
                    logger.info(f"  Streaming: {stream_name}")

                    try:
                        records_generator = ab_source.get_records(stream_name)
                        records_list = []
                        accumulated_size = 0
                        oversized = False

                        for record in records_generator:
                            if cancel_event and cancel_event.is_set():
                                raise TimeoutError(
                                    f"Sync timed out after {timeout_seconds} seconds"
                                )

                            record_size = sum(
                                len(str(v).encode("utf-8")) for v in record.values()
                            )
                            accumulated_size += record_size
                            records_list.append(record)

                            if accumulated_size >= buffer_size_mb * 1024 * 1024:
                                # Oversized: switch to synchronous mode
                                oversized = True
                                break

                        if oversized:
                            # Process synchronously in main thread
                            sync_writer = create_streaming_writer(
                                catalog=catalog,
                                schema=schema,
                                staging_volume=staging_volume,
                                warehouse_id=warehouse_id,
                                buffer_size_records=buffer_size_records,
                                buffer_size_mb=buffer_size_mb,
                                flatten=flatten,
                                run_id=run_id,
                                dedup_keys=normalized_dedup_keys,
                            )
                            try:
                                if mode == "overwrite":
                                    sync_writer.safe_overwrite_begin(stream_name, run_id)

                                for rec in records_list:
                                    sync_writer.write_record(stream_name, rec)
                                # Continue consuming remaining records
                                count = len(records_list)
                                for record in records_generator:
                                    if cancel_event and cancel_event.is_set():
                                        raise TimeoutError(
                                            f"Sync timed out after {timeout_seconds} seconds"
                                        )
                                    sync_writer.write_record(stream_name, record)
                                    count += 1
                                sync_writer.flush_stream(stream_name)

                                if mode == "overwrite":
                                    sync_writer.safe_overwrite_finish(stream_name, run_id)

                                with lock:
                                    total_records += count
                                    successful_streams.append(stream_name)
                                logger.info(f"    {count} records streamed (sync)")
                            finally:
                                sync_writer.close()
                        else:
                            # Wait until in-flight count is below limit
                            import time

                            while True:
                                with in_flight_lock:
                                    if in_flight < max_parallel_streams:
                                        in_flight += 1
                                        break
                                time.sleep(0.01)

                            future = executor.submit(
                                _write_stream_records,
                                stream_name,
                                records_list,
                                run_id,
                                mode,
                            )
                            futures.append((stream_name, future))

                    except Exception as e:
                        error_name = type(e).__name__
                        logger.error(f"  Failed to stream {stream_name}: {e}")
                        failed_streams.append(stream_name)

                        is_fatal = "ConnectorFailed" in error_name
                        if is_fatal or not continue_on_error:
                            # Cancel remaining futures
                            for _, f in futures:
                                f.cancel()
                            raise

                # Collect results from futures
                for stream_name, future in futures:
                    try:
                        count = future.result()
                        with lock:
                            total_records += count
                            successful_streams.append(stream_name)
                        logger.info(f"    {count} records streamed")
                    except Exception as e:
                        logger.error(f"  Failed to stream {stream_name}: {e}")
                        with lock:
                            failed_streams.append(stream_name)
                        if not continue_on_error:
                            for _, f in futures:
                                f.cancel()
                            raise

                executor.shutdown(wait=True)

            else:
                # Sequential processing (default)
                writer = create_streaming_writer(
                    catalog=catalog,
                    schema=schema,
                    staging_volume=staging_volume,
                    warehouse_id=warehouse_id,
                    buffer_size_records=buffer_size_records,
                    buffer_size_mb=buffer_size_mb,
                    flatten=flatten,
                    run_id=run_id,
                    dedup_keys=normalized_dedup_keys,
                )

                for stream_name in selected:
                    logger.info(f"  Streaming: {stream_name}")

                    if mode == "overwrite":
                        writer.safe_overwrite_begin(stream_name, run_id)

                    try:
                        records_generator = ab_source.get_records(stream_name)
                        count = 0
                        for record in records_generator:
                            if cancel_event and cancel_event.is_set():
                                raise TimeoutError(
                                    f"Sync timed out after {timeout_seconds} seconds"
                                )

                            writer.write_record(stream_name, record)
                            count += 1
                            if count % 10000 == 0:
                                logger.info(f"    ...streamed {count} records")

                            if (
                                cancel_event
                                and count % 1000 == 0
                                and cancel_event.is_set()
                            ):
                                raise TimeoutError(
                                    f"Sync timed out after {timeout_seconds} seconds"
                                )

                        writer.flush_stream(stream_name)

                        if mode == "overwrite":
                            writer.safe_overwrite_finish(stream_name, run_id)

                        logger.info(f"    {count} records streamed")
                        total_records += count
                        successful_streams.append(stream_name)

                        if progress_reporter:
                            progress_reporter.stream_completed(stream_name, count)

                        # Save incremental state on success
                        if state_manager and incremental:
                            # TODO: save state from PyAirbyte
                            pass

                    except Exception as e:
                        error_name = type(e).__name__
                        logger.error(f"  Failed to stream {stream_name}: {e}")
                        failed_streams.append(stream_name)

                        is_fatal = "ConnectorFailed" in error_name
                        if is_fatal:
                            raise
                        if not continue_on_error:
                            raise

            if failed_streams:
                if continue_on_error:
                    logger.warning(
                        f"Completed with {len(failed_streams)} failed streams: "
                        f"{failed_streams}"
                    )
                else:
                    raise RuntimeError(
                        f"Sync failed. Failed streams: {failed_streams}"
                    )

            # Run deduplication if enabled
            if deduplicate and normalized_dedup_keys and successful_streams:
                from brickbyte._dedup import deduplicate_stream
                from brickbyte._sanitize import sanitize_stream_name as _sanitize

                for stream_name in successful_streams:
                    sanitized = _sanitize(stream_name)
                    stream_keys = normalized_dedup_keys.get(stream_name)
                    if stream_keys is None:
                        continue

                    table_name = f"`{catalog}`.`{schema}`.`{sanitized}`"
                    if flatten:
                        deduplicate_stream(
                            executor=writer if writer else None,
                            table_name=table_name,
                            key_columns=stream_keys,
                            run_id_col="_run_id",
                            extracted_at_col="_extracted_at",
                            record_id_col="_record_id",
                            flatten=True,
                        )
                    else:
                        dk_cols = [f"_dk_{i}" for i in range(len(stream_keys))]
                        deduplicate_stream(
                            executor=writer if writer else None,
                            table_name=table_name,
                            key_columns=dk_cols,
                            run_id_col="run_id",
                            extracted_at_col="extracted_at",
                            record_id_col="record_id",
                            flatten=False,
                        )

            enriched_tables = []
            if enrich_metadata and successful_streams:
                logger.info("Enriching metadata with AI...")
                from brickbyte.enrichment import enrich_table

                model = enrich_model or "databricks-meta-llama-3-3-70b-instruct"
                for stream_name in successful_streams:
                    try:
                        enrich_table(
                            catalog=catalog,
                            schema=schema,
                            table=stream_name,
                            apply_to_catalog=True,
                            model_name=model,
                        )
                        enriched_tables.append(stream_name)
                    except Exception as e:
                        logger.warning(
                            f"  Warning: Could not enrich {stream_name}: {e}"
                        )

            return SyncResult(
                records_written=total_records,
                streams_synced=successful_streams,
                failed_streams=failed_streams,
                enriched_tables=enriched_tables,
            )

        finally:
            if timer is not None:
                timer.cancel()
            if writer is not None:
                writer.close()
            if cleanup:
                self.cleanup()

    def _normalize_dedup_keys(
        self,
        dedup_keys: Optional[Union[List[str], Dict[str, List[str]]]],
        streams: Optional[List[str]],
    ) -> Dict[str, List[str]]:
        """Normalize dedup_keys to Dict[str, List[str]]."""
        if dedup_keys is None:
            raise ValueError(
                "dedup_keys is required when deduplicate=True. "
                "Provide a list of column names or a dict mapping stream names to column lists."
            )

        if isinstance(dedup_keys, list):
            if len(dedup_keys) == 0:
                raise ValueError("dedup_keys must be non-empty")
            # Will be expanded to all selected streams later
            return {"__all__": dedup_keys}

        if isinstance(dedup_keys, dict):
            for stream_name, keys in dedup_keys.items():
                if not isinstance(keys, list) or len(keys) == 0:
                    raise ValueError(
                        f"dedup_keys for stream '{stream_name}' must be non-empty"
                    )
            return dedup_keys

        raise ValueError("dedup_keys must be a list or dict")

    def cleanup(self):
        """Remove virtual environments."""
        for manager in self._source_env_managers.values():
            manager.delete_virtualenv()
        self._source_env_managers.clear()

    def list_configured_sources(self) -> List[str]:
        """List all sources that have credentials configured."""
        return self._credential_resolver.list_available_sources()

    def validate_credentials(self, source: str) -> bool:
        """Check if credentials are configured for a source."""
        return self._credential_resolver.validate(source)
