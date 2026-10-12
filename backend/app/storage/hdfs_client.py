"""
app/storage/hdfs_client.py
---------------------------
High-level HDFS storage client using the ``hdfs`` Python library (WebHDFS).

The ``hdfs`` library talks to HDFS exclusively through the NameNode's WebHDFS
REST API on port 9870.  This is simpler and more reliable for local dev than
native libhdfs bindings — no JNI, no Hadoop client JAR on the Python path.

Design notes
------------
* ``write_dataframe`` serialises through a local Parquet temp file because the
  ``hdfs`` library does not natively write Parquet.  The round-trip is:
  pandas → local temp Parquet → HDFS upload.  The temp file is always deleted
  in a ``finally`` block.
* ``read_dataframe`` does the reverse: HDFS download → local temp path →
  pandas read_parquet.
* ``health_check`` first verifies root-directory status, then optionally
  enriches the response with live capacity stats from the NameNode's JMX
  endpoint.  JMX failures are caught and logged rather than re-raised so the
  health check remains useful even if JMX is unreachable.
* Every public method raises one of the custom exceptions from
  ``app.storage.exceptions`` rather than propagating ``hdfs`` library
  internals — callers never need to import the ``hdfs`` package.
"""

from __future__ import annotations

import logging
import os
import tempfile
from typing import Any

import httpx
import pandas as pd

from app.core.config import get_settings
from app.storage.exceptions import (
    HdfsConnectionError,
    HdfsPathNotFoundError,
    HdfsReadError,
    HdfsWriteError,
)

logger = logging.getLogger(__name__)


class HdfsStorageClient:
    """
    Thin wrapper around ``hdfs.InsecureClient`` providing DataFrame-aware
    read/write helpers and a structured ``health_check()`` method.

    Parameters
    ----------
    namenode_url : str | None
        WebHDFS base URL (e.g. ``http://namenode:9870``).  If ``None``,
        read from ``Settings.HDFS_NAMENODE_URL``.
    user : str | None
        Hadoop username for SIMPLE authentication.  If ``None``,
        read from ``Settings.HDFS_USER``.
    replication : int | None
        Replication factor for new files.  If ``None``,
        read from ``Settings.HDFS_DEFAULT_REPLICATION``.
    """

    def __init__(
        self,
        namenode_url: str | None = None,
        user: str | None = None,
        replication: int | None = None,
    ) -> None:
        """Initialise the client from settings or explicit overrides."""
        settings = get_settings()
        self._namenode_url = namenode_url or settings.HDFS_NAMENODE_URL
        self._user = user or settings.HDFS_USER
        self._replication = replication if replication is not None else settings.HDFS_DEFAULT_REPLICATION
        # Defer actual client construction to first use so that importing this
        # class never fails when the ``hdfs`` package is absent (unit test envs).
        self._client = None

    # ── Client factory ─────────────────────────────────────────────────────────

    @property
    def _hdfs_client(self):
        """
        Lazy accessor for the underlying ``hdfs.InsecureClient`` instance.

        Built on first access so that importing this module never triggers an
        ``ImportError`` even when the ``hdfs`` package is not installed.
        """
        if self._client is None:
            self._client = self._build_client()
        return self._client

    def _build_client(self):
        """
        Instantiate an ``hdfs.InsecureClient``.

        Import is deferred to here so the module can be imported even if the
        ``hdfs`` package is not installed (unit tests mock this method).

        Returns
        -------
        hdfs.InsecureClient
            Configured WebHDFS client.

        Raises
        ------
        HdfsConnectionError
            If the ``hdfs`` package is not installed.
        """
        try:
            from hdfs import InsecureClient  # type: ignore[import-untyped]

            return InsecureClient(self._namenode_url, user=self._user)
        except ImportError as exc:
            raise HdfsConnectionError(
                "The 'hdfs' package is not installed. "
                "Add 'hdfs==2.7.3' to requirements.txt.",
                namenode_url=self._namenode_url,
            ) from exc

    # ── Public API ─────────────────────────────────────────────────────────────

    def write_dataframe(self, df: pd.DataFrame, hdfs_path: str) -> dict[str, Any]:
        """
        Write *df* to HDFS as a Parquet file at *hdfs_path*.

        Workflow: ``df → local temp Parquet → HDFS upload(overwrite=True)``.

        Parameters
        ----------
        df : pandas.DataFrame
            DataFrame to persist.
        hdfs_path : str
            Destination HDFS path, e.g. ``/data/sales/output.parquet``.

        Returns
        -------
        dict
            ``{"hdfs_path": str, "rows_written": int, "size_bytes": int}``.

        Raises
        ------
        HdfsWriteError
            If the upload fails for any reason.
        """
        tmp_path: str | None = None
        try:
            with tempfile.NamedTemporaryFile(suffix=".parquet", delete=False) as f:
                tmp_path = f.name

            df.to_parquet(tmp_path, index=False)
            file_size = os.path.getsize(tmp_path)

            # Ensure parent directory exists
            parent = hdfs_path.rsplit("/", 1)[0] if "/" in hdfs_path else "/"
            try:
                self._hdfs_client.makedirs(parent)
            except Exception:
                pass  # directory may already exist

            self._hdfs_client.upload(hdfs_path, tmp_path, overwrite=True)
            logger.info(
                "Wrote %d rows (%d bytes) to HDFS path '%s'.",
                len(df),
                file_size,
                hdfs_path,
            )
            return {
                "hdfs_path": hdfs_path,
                "rows_written": len(df),
                "size_bytes": file_size,
            }
        except (HdfsConnectionError, HdfsWriteError):
            raise
        except Exception as exc:
            raise HdfsWriteError(
                f"Failed to write DataFrame to HDFS path '{hdfs_path}': {exc}",
                hdfs_path=hdfs_path,
            ) from exc
        finally:
            if tmp_path and os.path.exists(tmp_path):
                os.unlink(tmp_path)

    def read_dataframe(self, hdfs_path: str) -> pd.DataFrame:
        """
        Read a Parquet file from HDFS and return it as a pandas DataFrame.

        Workflow: ``HDFS download → local temp path → pd.read_parquet``.

        Parameters
        ----------
        hdfs_path : str
            Source HDFS path.

        Returns
        -------
        pandas.DataFrame

        Raises
        ------
        HdfsPathNotFoundError
            If *hdfs_path* does not exist on HDFS.
        HdfsReadError
            If the download or Parquet parsing fails.
        """
        hdfs_path = self._normalize_hdfs_path(hdfs_path)
        if not self.path_exists(hdfs_path):
            raise HdfsPathNotFoundError(
                f"HDFS path does not exist: '{hdfs_path}'",
                hdfs_path=hdfs_path,
            )

        tmp_path: str | None = None
        try:
            status = self._hdfs_client.status(hdfs_path)
            if status and status.get("type") == "DIRECTORY":
                with tempfile.TemporaryDirectory() as directory:
                    frames = []
                    for name in self.list_directory(hdfs_path):
                        if not str(name).startswith("part-") or not str(name).endswith(".parquet"):
                            continue
                        local_path = os.path.join(directory, str(name))
                        self._hdfs_client.download(
                            f"{hdfs_path.rstrip('/')}/{name}",
                            local_path,
                            overwrite=True,
                        )
                        frames.append(pd.read_parquet(local_path))
                    if not frames:
                        raise HdfsReadError(
                            f"No Spark Parquet part files found at '{hdfs_path}'.",
                            hdfs_path=hdfs_path,
                        )
                    return pd.concat(frames, ignore_index=True)

            with tempfile.NamedTemporaryFile(suffix=".parquet", delete=False) as f:
                tmp_path = f.name

            self._hdfs_client.download(hdfs_path, tmp_path, overwrite=True)
            df = pd.read_parquet(tmp_path)
            logger.info(
                "Read %d rows from HDFS path '%s'.", len(df), hdfs_path
            )
            return df
        except (HdfsPathNotFoundError, HdfsReadError):
            raise
        except Exception as exc:
            raise HdfsReadError(
                f"Failed to read DataFrame from HDFS path '{hdfs_path}': {exc}",
                hdfs_path=hdfs_path,
            ) from exc
        finally:
            if tmp_path and os.path.exists(tmp_path):
                os.unlink(tmp_path)

    @staticmethod
    def _normalize_hdfs_path(hdfs_path: str) -> str:
        """Convert a Spark hdfs:// URI to the WebHDFS client path form."""
        if hdfs_path.startswith("hdfs://"):
            without_scheme = hdfs_path.split("://", 1)[1]
            slash = without_scheme.find("/")
            return without_scheme[slash:] if slash >= 0 else "/"
        return hdfs_path

    def content_summary(self, path: str) -> dict[str, int] | None:
        """
        Query WebHDFS GETCONTENTSUMMARY for *path*.

        Parameters
        ----------
        path : str
            HDFS path to query (can be an HDFS URI or absolute path).

        Returns
        -------
        dict[str, int] | None
            ``{"fileCount": int, "length": int, "directoryCount": int}``
            or ``None`` if the path does not exist (HTTP 404) or on error.
        """
        try:
            norm_path = self._normalize_hdfs_path(path)
            if not norm_path.startswith("/"):
                norm_path = f"/{norm_path}"
            url = f"{self._namenode_url}/webhdfs/v1{norm_path}?op=GETCONTENTSUMMARY&user.name={self._user}"
            resp = httpx.get(url, timeout=5.0)
            if resp.status_code == 404:
                return None
            resp.raise_for_status()
            data = resp.json()
            cs = data.get("ContentSummary")
            if not isinstance(cs, dict):
                return None
            return {
                "fileCount": int(cs.get("fileCount", 0)),
                "length": int(cs.get("length", 0)),
                "directoryCount": int(cs.get("directoryCount", 0)),
            }
        except Exception as exc:
            logger.debug("content_summary failed for '%s': %s", path, exc)
            return None

    def path_exists(self, hdfs_path: str) -> bool:
        """
        Return ``True`` if *hdfs_path* exists on HDFS, ``False`` otherwise.

        Uses ``client.status(path, strict=False)`` which returns ``None``
        instead of raising when the path is absent.

        Parameters
        ----------
        hdfs_path : str
            HDFS path to test.

        Returns
        -------
        bool
        """
        try:
            status = self._hdfs_client.status(hdfs_path, strict=False)
            return status is not None
        except Exception as exc:
            logger.warning("path_exists check failed for '%s': %s", hdfs_path, exc)
            return False

    def list_directory(self, hdfs_path: str) -> list[str]:
        """
        Return the names of files/directories directly under *hdfs_path*.

        Parameters
        ----------
        hdfs_path : str
            HDFS directory path to list.

        Returns
        -------
        list[str]
            File/directory names (not full paths).

        Raises
        ------
        HdfsPathNotFoundError
            If *hdfs_path* does not exist.
        HdfsReadError
            If the listing fails for any other reason.
        """
        try:
            entries = self._hdfs_client.list(hdfs_path)
            return list(entries)
        except Exception as exc:
            msg = str(exc)
            if "not found" in msg.lower() or "does not exist" in msg.lower():
                raise HdfsPathNotFoundError(
                    f"HDFS directory does not exist: '{hdfs_path}'",
                    hdfs_path=hdfs_path,
                ) from exc
            raise HdfsReadError(
                f"Failed to list HDFS directory '{hdfs_path}': {exc}",
                hdfs_path=hdfs_path,
            ) from exc

    def delete_path(self, hdfs_path: str, recursive: bool = False) -> bool:
        """
        Delete *hdfs_path* from HDFS.

        Parameters
        ----------
        hdfs_path : str
            HDFS path to delete.
        recursive : bool
            If ``True``, delete non-empty directories recursively.

        Returns
        -------
        bool
            ``True`` if the path was deleted, ``False`` if it did not exist.
        """
        try:
            result = self._hdfs_client.delete(hdfs_path, recursive=recursive)
            if result:
                logger.info("Deleted HDFS path '%s' (recursive=%s).", hdfs_path, recursive)
            return bool(result)
        except Exception as exc:
            logger.warning("Failed to delete HDFS path '%s': %s", hdfs_path, exc)
            return False

    def delete(self, hdfs_path: str, recursive: bool = False) -> bool:
        """Alias for delete_path (WebHDFS op=DELETE)."""
        return self.delete_path(hdfs_path, recursive=recursive)

    def health_check(self) -> dict[str, Any]:
        """
        Verify NameNode connectivity and return live cluster statistics.

        Steps:
        1. Call ``client.status("/")`` — fastest HDFS reachability check.
        2. If step 1 succeeds, hit the JMX endpoint to extract capacity
           and live DataNode count.  JMX failures are caught gracefully.

        Returns
        -------
        dict
            ``{"connected": bool, "namenode_url": str, "error": str | None,
            "capacity_total_gb": float | None, "capacity_used_gb": float | None,
            "num_live_datanodes": int | None}``
        """
        import requests  # stdlib-adjacent; already in requirements.txt

        result: dict[str, Any] = {
            "connected": False,
            "namenode_url": self._namenode_url,
            "error": None,
            "capacity_total_gb": None,
            "capacity_used_gb": None,
            "num_live_datanodes": None,
        }

        # ── Step 1: WebHDFS root status ────────────────────────────────────────
        try:
            self._hdfs_client.status("/")
            result["connected"] = True
        except Exception as exc:
            result["error"] = str(exc)
            logger.warning("HDFS health check failed: %s", exc)
            return result

        # ── Step 2: JMX capacity stats (best-effort) ───────────────────────────
        try:
            jmx_url = (
                f"{self._namenode_url}/jmx"
                "?qry=Hadoop:service=NameNode,name=FSNamesystem"
            )
            resp = requests.get(jmx_url, timeout=5)
            resp.raise_for_status()
            beans = resp.json().get("beans", [])
            if beans:
                bean = beans[0]
                total = bean.get("CapacityTotal", 0)
                used = bean.get("CapacityUsed", 0)
                result["capacity_total_gb"] = round(total / 1024**3, 3)
                result["capacity_used_gb"] = round(used / 1024**3, 6)
                result["num_live_datanodes"] = bean.get("NumLiveDataNodes")
        except Exception as exc:
            logger.debug("HDFS JMX stats unavailable: %s", exc)
            # JMX failure is non-fatal — cluster is still reachable

        return result


# Alias HdfsClient to HdfsStorageClient for backward compatibility and prompt compliance
HdfsClient = HdfsStorageClient
