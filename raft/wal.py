"""Write-Ahead Log (WAL) Storage Manager for disk-backed persistence.

Reference: Raft Paper Section 5.3 (Log Replication) and 5.4 (Safety).
Provides simple append-only file I/O with explicit fsync to guarantee durability
of currentTerm, votedFor, and log entries before responding to RPCs.
"""

from __future__ import annotations

import json
import logging
import os
from typing import Any, Dict, List, Optional, Tuple

from raft.state import LogEntry

logger = logging.getLogger(__name__)


class WALStorage:
    """An append-only Write-Ahead Log manager using newline-delimited JSON records."""

    def __init__(self, filepath: str) -> None:
        self.filepath = filepath
        self._fd: Optional[int] = None
        self._file = None
        self._open_file()

    def _open_file(self) -> None:
        """Open the WAL file in append mode and get its file descriptor for fsync."""
        # Ensure the directory exists
        os.makedirs(os.path.dirname(os.path.abspath(self.filepath)), exist_ok=True)
        self._file = open(self.filepath, "a", encoding="utf-8")
        self._fd = self._file.fileno()

    def _sync(self) -> None:
        """Force write of OS buffers to disk."""
        if self._file and self._fd is not None:
            self._file.flush()
            os.fsync(self._fd)

    def _write_record(self, record: Dict[str, Any]) -> None:
        """Write a single JSON record to the log and sync to disk."""
        if not self._file:
            return
        line = json.dumps(record) + "\n"
        self._file.write(line)
        self._sync()

    def append_term_vote(self, term: int, voted_for: Optional[str]) -> None:
        """Persist currentTerm and votedFor."""
        self._write_record({
            "type": "METADATA",
            "term": term,
            "voted_for": voted_for,
        })

    def append_entry(self, entry: LogEntry) -> None:
        """Persist a single log entry."""
        self._write_record({
            "type": "ENTRY",
            "index": entry.index,
            "term": entry.term,
            "command": entry.command,
        })

    def append_entries(self, entries: List[LogEntry]) -> None:
        """Persist multiple log entries in a single sync operation."""
        if not self._file or not entries:
            return
        for entry in entries:
            line = json.dumps({
                "type": "ENTRY",
                "index": entry.index,
                "term": entry.term,
                "command": entry.command,
            }) + "\n"
            self._file.write(line)
        self._sync()

    def truncate_log(self, from_index: int) -> None:
        """Persist a log truncation event (delete entries from 'from_index' onwards)."""
        self._write_record({
            "type": "TRUNCATE",
            "from_index": from_index,
        })

    def recover(self) -> Tuple[int, Optional[str], List[LogEntry]]:
        """Read the WAL file sequentially to reconstruct the durable state.
        
        Returns:
            Tuple of (current_term, voted_for, log_entries)
        """
        current_term = 0
        voted_for: Optional[str] = None
        log: List[LogEntry] = []

        if not os.path.exists(self.filepath):
            return current_term, voted_for, log

        # Close the append handle briefly to read cleanly, though reading a separate fd is fine.
        with open(self.filepath, "r", encoding="utf-8") as f:
            for line_idx, line in enumerate(f):
                line = line.strip()
                if not line:
                    continue
                try:
                    record = json.loads(line)
                    rtype = record.get("type")
                    if rtype == "METADATA":
                        current_term = record.get("term", current_term)
                        voted_for = record.get("voted_for")
                    elif rtype == "ENTRY":
                        entry = LogEntry(
                            index=record["index"],
                            term=record["term"],
                            command=record["command"]
                        )
                        # The WAL is append-only, but logic in RaftNode might append already-existing 
                        # indices if we had a TRUNCATE followed by APPEND. We build the log sequentially.
                        # It is safer if we just append, since truncate_log handles deletions.
                        # We assume the log being rebuilt matches the index.
                        if entry.index <= len(log):
                            log[entry.index - 1] = entry
                        else:
                            log.append(entry)
                    elif rtype == "TRUNCATE":
                        from_index = record["from_index"]
                        # Log is 1-indexed, so from_index 3 means keep indices 1, 2. (Python slice: log[:2])
                        if from_index > 0:
                            log = log[:from_index - 1]
                except json.JSONDecodeError:
                    logger.error("Corrupted WAL record at %s:%d", self.filepath, line_idx)
                    # We can stop or ignore; Raft relies on exact consistency.
                    pass

        return current_term, voted_for, log

    def close(self) -> None:
        """Close the file descriptor safely."""
        if self._file:
            self._file.flush()
            if self._fd is not None:
                try:
                    os.fsync(self._fd)
                except OSError:
                    pass
            self._file.close()
            self._file = None
            self._fd = None

    def destroy(self) -> None:
        """Close and delete the underlying file (used for test cleanup)."""
        self.close()
        if os.path.exists(self.filepath):
            os.remove(self.filepath)
