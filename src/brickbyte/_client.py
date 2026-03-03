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
from typing import Any, Callable, Dict, List, Optional, Union

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
            dedup_keys: Column(s) to use as dedup keys (required when deduplicate=True;
                        ignored when deduplicate=False)
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
        elif dedup_keys is not None:
            logger.info("dedup_keys provided but deduplicate=False; ignoring dedup_keys")

        # Set up timeout
        cancel_event = None
        timer = None
        if timeout_seconds is not None:
            cancel_event = threading.Event()
            timer = threading.Timer(timeout_seconds, cancel_event.set)
            timer.daemon = True
            timer.start()

        writer = None
        progress_reporter = None
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

            if normalized_dedup_keys is not None and "__all__" in normalized_dedup_keys:
                all_keys = normalized_dedup_keys["__all__"]
                normalized_dedup_keys = {s: all_keys for s in selected}

            if deduplicate and isinstance(normalized_dedup_keys, dict):
                for dk_stream in normalized_dedup_keys:
                    if dk_stream not in selected:
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

            state_manager = None
            stream_states: Dict[str, dict] = {}
            if incremental:
                from brickbyte._state import StateManager

                state_manager = StateManager(
                    catalog=catalog,
                    schema=schema,
                    staging_volume=staging_volume,
                    warehouse_id=warehouse_id,
                )
                for stream_name in selected:
                    saved = state_manager.get_state(source, stream_name)
                    if saved is not None:
                        stream_states[stream_name] = saved
                        logger.info(
                            f"  Loaded incremental state for {stream_name}"
                        )
                self._apply_incremental_state(ab_source, stream_states)

            via_msg = f" via {staging_volume}" if staging_volume else " (Native Spark)"
            logger.info(
                f"Streaming {len(selected)} streams to {catalog}.{schema}{via_msg}..."
            )

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

            # Common writer-creation kwargs used by both paths
            _writer_kwargs = dict(
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
                    thread_writer = create_streaming_writer(**_writer_kwargs)
                    try:
                        if _mode == "overwrite":
                            thread_writer.safe_overwrite_begin(stream_name, _run_id)

                        for record in records_list:
                            thread_writer.write_record(stream_name, record)
                        thread_writer.flush_stream(stream_name)

                        if _mode == "overwrite":
                            thread_writer.safe_overwrite_finish(stream_name, _run_id)

                        return stream_name, len(records_list), thread_writer
                    except Exception:
                        thread_writer.close()
                        with in_flight_lock:
                            nonlocal in_flight
                            in_flight -= 1
                        raise

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
                                oversized = True
                                break

                        if oversized:
                            sync_writer = create_streaming_writer(**_writer_kwargs)
                            try:
                                if mode == "overwrite":
                                    sync_writer.safe_overwrite_begin(stream_name, run_id)

                                for rec in records_list:
                                    sync_writer.write_record(stream_name, rec)
                                count = len(records_list)
                                for record in records_generator:
                                    if cancel_event and cancel_event.is_set():
                                        raise TimeoutError(
                                            f"Sync timed out after {timeout_seconds} seconds"
                                        )
                                    sync_writer.write_record(stream_name, record)
                                    count += 1
                                    if progress_reporter and count % 5000 == 0:
                                        progress_reporter.record_processed(stream_name, count)
                                sync_writer.flush_stream(stream_name)

                                if mode == "overwrite":
                                    sync_writer.safe_overwrite_finish(stream_name, run_id)

                                self._run_dedup_for_stream(
                                    stream_name, deduplicate, normalized_dedup_keys,
                                    flatten, catalog, schema, sync_writer,
                                )

                                with lock:
                                    total_records += count
                                    successful_streams.append(stream_name)

                                if progress_reporter:
                                    progress_reporter.stream_completed(stream_name, count)

                                self._save_incremental_state(
                                    state_manager=state_manager,
                                    incremental=incremental,
                                    ab_source=ab_source,
                                    source=source,
                                    stream_name=stream_name,
                                    run_id=run_id,
                                    records_written=count,
                                )

                                logger.info(f"    {count} records streamed (sync)")
                            finally:
                                sync_writer.close()
                        else:
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
                            for _, f in futures:
                                f.cancel()
                            raise

                # Collect results from futures
                for stream_name, future in futures:
                    try:
                        _sname, count, thread_writer = future.result()
                        try:
                            self._run_dedup_for_stream(
                                _sname, deduplicate, normalized_dedup_keys,
                                flatten, catalog, schema, thread_writer,
                            )
                        finally:
                            thread_writer.close()
                            with in_flight_lock:
                                in_flight -= 1

                        with lock:
                            total_records += count
                            successful_streams.append(_sname)

                        if progress_reporter:
                            progress_reporter.stream_completed(_sname, count)

                        self._save_incremental_state(
                            state_manager=state_manager,
                            incremental=incremental,
                            ab_source=ab_source,
                            source=source,
                            stream_name=_sname,
                            run_id=run_id,
                            records_written=count,
                        )

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
                writer = create_streaming_writer(**_writer_kwargs)

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

                            if progress_reporter and count % 5000 == 0:
                                progress_reporter.record_processed(stream_name, count)

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

                        self._run_dedup_for_stream(
                            stream_name, deduplicate, normalized_dedup_keys,
                            flatten, catalog, schema, writer,
                        )

                        logger.info(f"    {count} records streamed")
                        total_records += count
                        successful_streams.append(stream_name)

                        if progress_reporter:
                            progress_reporter.stream_completed(stream_name, count)

                        self._save_incremental_state(
                            state_manager=state_manager,
                            incremental=incremental,
                            ab_source=ab_source,
                            source=source,
                            stream_name=stream_name,
                            run_id=run_id,
                            records_written=count,
                        )

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

            enriched_tables = []
            if enrich_metadata and successful_streams:
                logger.info("Enriching metadata with AI...")
                from brickbyte._sanitize import sanitize_stream_name as _sanitize
                from brickbyte.enrichment import enrich_table

                model = enrich_model or "databricks-meta-llama-3-3-70b-instruct"
                for stream_name in successful_streams:
                    try:
                        sanitized = _sanitize(stream_name)
                        enrich_table(
                            catalog=catalog,
                            schema=schema,
                            table=sanitized,
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
            if progress_reporter is not None:
                try:
                    progress_reporter.close()
                except Exception as e:
                    logger.debug(f"Failed to close progress reporter: {e}")
            if writer is not None:
                writer.close()
            if cleanup:
                self.cleanup()

    def _run_dedup_for_stream(
        self,
        stream_name: str,
        deduplicate: bool,
        normalized_dedup_keys: Optional[Dict[str, List[str]]],
        flatten: bool,
        catalog: str,
        schema: str,
        executor_writer,
    ):
        """Run dedup for a single stream using the provided writer as executor."""
        if not deduplicate or not normalized_dedup_keys:
            return

        stream_keys = normalized_dedup_keys.get(stream_name)
        if stream_keys is None:
            return

        from brickbyte._dedup import deduplicate_stream
        from brickbyte._sanitize import sanitize_stream_name

        sanitized = sanitize_stream_name(stream_name)
        table_name = f"`{catalog}`.`{schema}`.`{sanitized}`"
        dk_cols = [f"_dk_{i}" for i in range(len(stream_keys))]

        if flatten:
            deduplicate_stream(
                executor=executor_writer,
                table_name=table_name,
                key_columns=dk_cols,
                run_id_col="_run_id",
                extracted_at_col="_extracted_at",
                record_id_col="_record_id",
                flatten=True,
            )
        else:
            deduplicate_stream(
                executor=executor_writer,
                table_name=table_name,
                key_columns=dk_cols,
                run_id_col="run_id",
                extracted_at_col="extracted_at",
                record_id_col="record_id",
                flatten=False,
            )

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
            self._validate_dedup_key_list(dedup_keys, context="dedup_keys")
            return {"__all__": dedup_keys}

        if isinstance(dedup_keys, dict):
            for stream_name, keys in dedup_keys.items():
                if not isinstance(keys, list) or len(keys) == 0:
                    raise ValueError(
                        f"dedup_keys for stream '{stream_name}' must be non-empty"
                    )
                self._validate_dedup_key_list(
                    keys,
                    context=f"dedup_keys for stream '{stream_name}'",
                )
            return dedup_keys

        raise ValueError("dedup_keys must be a list or dict")

    def _validate_dedup_key_list(self, keys: List[str], context: str) -> None:
        """Validate dedup key identifier safety."""
        from brickbyte._sanitize import validate_identifier

        for key in keys:
            if not isinstance(key, str) or not key:
                raise ValueError(f"{context} must contain non-empty string keys")
            try:
                validate_identifier(key)
            except ValueError as e:
                raise ValueError(f"{context} contains invalid key '{key}': {e}") from e

    def _apply_incremental_state(
        self,
        ab_source: Any,
        stream_states: Dict[str, dict],
    ) -> None:
        """Apply previously saved stream states to the source before reading."""
        if not stream_states:
            return

        for method_name in ("set_stream_state", "set_state_for_stream"):
            method = getattr(ab_source, method_name, None)
            if not callable(method):
                continue
            try:
                for stream_name, state in stream_states.items():
                    method(stream_name, state)
                logger.info(
                    f"Applied incremental state for {len(stream_states)} stream(s)"
                )
                return
            except TypeError:
                continue

        set_state = getattr(ab_source, "set_state", None)
        if callable(set_state):
            state_payload = {
                "streams": [
                    {
                        "stream": {"name": stream_name},
                        "stream_state": state,
                    }
                    for stream_name, state in stream_states.items()
                ]
            }
            for payload in (state_payload, stream_states):
                try:
                    set_state(payload)
                    logger.info(
                        f"Applied incremental state for {len(stream_states)} stream(s)"
                    )
                    return
                except TypeError:
                    continue

        raise NotImplementedError(
            "incremental=True requires source state injection support "
            "(set_stream_state/set_state_for_stream/set_state)."
        )

    def _extract_incremental_state(
        self,
        ab_source: Any,
        stream_name: str,
        run_id: str,
        records_written: int,
    ) -> dict:
        """Extract connector-emitted stream state when available."""
        fallback_state = {"run_id": run_id, "records": records_written}

        for method_name in ("get_stream_state", "stream_state"):
            method = getattr(ab_source, method_name, None)
            if not callable(method):
                continue
            try:
                state = method(stream_name)
                if state is not None:
                    return state
            except TypeError:
                continue
            except Exception as e:
                logger.debug(f"Could not read stream state via {method_name}: {e}")

        get_state = getattr(ab_source, "get_state", None)
        if callable(get_state):
            for args in ((stream_name,), tuple()):
                try:
                    state = get_state(*args)
                except TypeError:
                    continue
                except Exception as e:
                    logger.debug(f"Could not read state via get_state: {e}")
                    break
                if state is None:
                    continue
                if isinstance(state, dict):
                    if stream_name in state:
                        return state[stream_name]
                    streams_state = state.get("streams")
                    if isinstance(streams_state, dict) and stream_name in streams_state:
                        return streams_state[stream_name]
                return state

        return fallback_state

    def _save_incremental_state(
        self,
        state_manager,
        incremental: bool,
        ab_source: Any,
        source: str,
        stream_name: str,
        run_id: str,
        records_written: int,
    ) -> None:
        """Persist state for a successfully synced stream."""
        if not incremental or state_manager is None:
            return

        state = self._extract_incremental_state(
            ab_source=ab_source,
            stream_name=stream_name,
            run_id=run_id,
            records_written=records_written,
        )
        state_manager.save_state(
            source=source,
            stream_name=stream_name,
            state=state,
            run_id=run_id,
        )

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
