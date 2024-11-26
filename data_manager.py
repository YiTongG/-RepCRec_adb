from dataclasses import dataclass
from typing import Dict, Set, List, Optional
from enum import Enum
import time
from collections import defaultdict


class Operation(Enum):
    READ = "READ"
    WRITE = "WRITE"


@dataclass
class Version:
    value: any
    timestamp: float
    transaction_id: str


@dataclass
class Operation_Record:
    operation: Operation
    item: str
    timestamp: float
    version: Version


@dataclass
class CommittedTransaction:
    transaction_id: str
    write_set: Set[str]
    read_set: Set[str]
    commit_timestamp: float


@dataclass
class SiteHistory:
    """TRACK"""

    def __init__(self):
        self.last_failure_time: float = 0
        self.last_recovery_time: float = 0

    def record_failure(self, timestamp: float):
        """
        record site failure event
        :param timestamp:
        :return:
        """
        self.last_failure_time = timestamp

    def record_recovery(self, timestamp: float):
        """
        record site recovery event
        :param timestamp:
        :return:
        """
        self.last_recovery_time = timestamp


class Site:
    def __init__(self, site_id: str):
        self.site_id = site_id
        self.is_up = True
        self.data: Dict[str, Version] = {}
        self.history = SiteHistory()
        self.last_commit_time: Dict[str, float] = defaultdict(float)
        # Track replicated variables that need a new write after recovery
        self.needs_write_after_recovery: Set[str] = set()

    def fail(self):
        """Handle site failure
        Mark all replicated variables as needing a write after recovery
        """
        self.is_up = False
        self.history.record_failure(time.time())
        print(f"{self.site_id} failed at {self.history.last_failure_time}")
        # Mark all replicated variables as needing a write after recovery
        self.needs_write_after_recovery = {var for var in self.data.keys()
                                           if int(var[1:]) % 2 == 0}

    def recover(self):
        """Handle site recovery
        Mark site as up, record recovery time, and clear needs_write_after_recovery
        """
        self.is_up = True
        self.history.record_recovery(time.time())
        print(f"{self.site_id} recovered at {self.history.last_recovery_time}")
        # Don't clear needs_write_after_recovery - wait for actual writes

    def can_serve_variable(self, variable: str) -> bool:
        """
        check if the site can serve the variable
        :param variable:
        :return:
        """
        if not self.is_up:
            return False
        if variable not in self.data:
            return False

        var_index = int(variable[1:])
        is_replicated = var_index % 2 == 0

        if not is_replicated:
            return True

        return variable not in self.needs_write_after_recovery


class Transaction:
    def __init__(self, transaction_id: str):
        self.transaction_id = transaction_id
        self.start_time = time.time()
        self.operations: List[Operation_Record] = []
        self.read_set: Set[str] = set()
        self.write_set: Set[str] = set()
        self.sites_written: Set[str] = set()
        self.accessed_sites: Set[str] = set()  # Track all sites accessed by this transaction
        self.read_sites: Dict[str, Set[str]] = defaultdict(set)  # variable -> set of sites where it was read
        self.will_abort = False

    def record_site_access(self, site_id: str, operation_type: str, variable: str = None):
        """Record that this transaction accessed a particular site
        operation_type: 'read' or 'write'
        variable: the variable being accessed
        """
        self.accessed_sites.add(site_id)
        if operation_type == 'read' and variable:
            self.read_sites[variable].add(site_id)
