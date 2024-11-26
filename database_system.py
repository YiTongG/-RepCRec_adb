from data_manager import *
from typing import Dict, Set, List, Optional
from collections import defaultdict
import time

from data_manager import *
from typing import Dict, Set, List, Optional
from collections import defaultdict
import time


def parse_test_commands(lines: List[str]) -> List[dict]:
    """Parse test commands
    :param lines:
    :return:
    """
    commands = []
    for line in lines:
        # Remove comments and whitespace
        line = line.split('//')[0].strip()
        if not line:
            continue
        line = line.replace(" ", "")
        # Parse commands
        if line.startswith('begin'):
            tx_id = line[6:-1]  # Extract transaction ID
            commands.append({'command': 'begin', 'transaction': tx_id})

        elif line.startswith('end'):
            tx_id = line[4:-1]  # Extract transaction ID
            commands.append({'command': 'end', 'transaction': tx_id})

        elif line.startswith('fail'):
            site_num = line[5:-1]  # Extract site number
            commands.append({'command': 'fail', 'site': site_num})

        elif line.startswith('recover'):
            site_num = line[8:-1]  # Extract site number
            commands.append({'command': 'recover', 'site': site_num})

        elif line.startswith('R'):
            # Parse read operation R(T2,x2)
            parts = line[2:-1].split(',')
            tx_id = parts[0]
            var = parts[1]
            commands.append({
                'command': 'read',
                'transaction': tx_id,
                'variable': var
            })

        elif line.startswith('W'):
            # Parse write operation W(T1,x2,10)
            parts = line[2:-1].split(',')
            tx_id = parts[0]
            var = parts[1]
            value = int(parts[2])
            commands.append({
                'command': 'write',
                'transaction': tx_id,
                'variable': var,
                'value': value
            })

    return commands


def load_test_file(filename: str):
    """Load test cases from file"""
    print(f"\nLoading test file: {filename}")
    try:
        with open(filename, 'r') as file:
            return parse_test_commands(file.readlines())
    except FileNotFoundError:
        print(f"Error: Test file '{filename}' not found")
        return []


class DatabaseSystem:
    def __init__(self):
        self.sites: Dict[str, Site] = {}
        self.active_transactions: Dict[str, Transaction] = {}
        self.committed_transactions: List[CommittedTransaction] = []
        self.rw_edges: Dict[str, Set[str]] = defaultdict(set)
        self.waiting_transactions: Dict[str, Set[str]] = defaultdict(set)  # transaction_id -> waiting_for_sites

    def execute_test(self, commands: List[dict]):
        """Execute test commands"""
        print("\nExecuting test commands...")
        for cmd in commands:
            command_type = cmd['command']

            if command_type == 'begin':
                # print(f"\nbegin({cmd['transaction']})")
                self.start_transaction(cmd['transaction'])

            elif command_type == 'end':
                # print(f"\nend({cmd['transaction']})")
                self.commit(cmd['transaction'])

            elif command_type == 'fail':
                # print(f"\nfail({cmd['site']})")
                site_id = f"site{cmd['site']}"
                self.sites[site_id].fail()

            elif command_type == 'recover':
                site_id = f"site{cmd['site']}"
                self.sites[site_id].recover()

            elif command_type == 'read':
                # print(f"\nR({cmd['transaction']},{cmd['variable']})")
                self.read(cmd['transaction'], cmd['variable'])

            elif command_type == 'write':
                # print(f"\nW({cmd['transaction']},{cmd['variable']},{cmd['value']})")
                self.write(cmd['transaction'], cmd['variable'], cmd['value'])

            time.sleep(0.01)  #

    def _get_variable_sites(self, variable: str) -> List[Site]:
        """
        GET the sites that can serve the variable
        :param variable:
        :return:
        """
        # Extract variable index
        var_index = int(variable[1:])
        available_sites = []

        if var_index % 2 == 0:  # Even index variable
            # On all available sites
            available_sites = [site for site in self.sites.values() if site.is_up]
        else:  # Odd index variable
            # Calculate site number: 1 + (index mod 10)
            site_num = 1 + (var_index % 10)
            site_id = f"site{site_num}"
            site = self.sites.get(site_id)
            if site and site.is_up:
                available_sites.append(site)

        return available_sites

    def start_transaction(self, transaction_id: str) -> Transaction:
        """Start a new transaction"""
        transaction = Transaction(transaction_id)
        self.active_transactions[transaction_id] = transaction
        return transaction

    def initialize_system(self):
        """
        INIT the database system
        :return:
        """
        print("Initializing database system...")

        #
        for i in range(1, 11):
            site_id = f"site{i}"
            self.sites[site_id] = Site(site_id)

        #
        for i in range(1, 21):  # x1 to x20
            value = 10 * i  # Initial value is 10i

            if i % 2 == 0:

                for site in self.sites.values():
                    site.data[f"x{i}"] = Version(value, 0, "init")

            else:

                site_num = 1 + (i % 10)
                site_id = f"site{site_num}"
                self.sites[site_id].data[f"x{i}"] = Version(value, 0, "init")
                # print(f"Variable x{i} (value={value}) placed at site {site_num}")

    def _check_site_availability(self, transaction_id: str) -> bool:
        """
        check if the sites are available for the transaction
        :param transaction_id:
        :return:
        """
        transaction = self.active_transactions[transaction_id]

        # First check if the sites written by the transaction have failed
        for op in transaction.operations:
            if op.operation == Operation.WRITE:
                var = op.item
                var_index = int(var[1:])
                is_replicated = var_index % 2 == 0

                if is_replicated:
                    #
                    available_count = 0
                    written_sites = [site for site in self.sites.values()
                                     if site.site_id in transaction.sites_written]

                    for site in written_sites:
                        if (site.site_id in transaction.sites_written and
                                site.history.last_failure_time > op.timestamp):
                            return False

                        if site.is_up:
                            available_count += 1

                    if available_count == 0:
                        # print(f"No available sites for replicated variable {var}")
                        return False
                else:
                    # For non-replicated variables, if the written site fails, it must be aborted
                    site_num = 1 + (var_index % 10)
                    site = self.sites[f"site{site_num}"]
                    if site.site_id in transaction.sites_written:
                        if site.history.last_failure_time > op.timestamp:
                            return False

        # Check the sites accessed by read operations
        for op in transaction.operations:
            if op.operation == Operation.READ:
                var = op.item
                var_index = int(var[1:])
                is_replicated = var_index % 2 == 0
                read_sites = transaction.read_sites.get(var, set())

                if is_replicated:
                    has_available_site = False
                    for site in self.sites.values():
                        if (site.is_up and
                                site.can_serve_variable(var) and
                                (site.history.last_failure_time == 0 or
                                 site.history.last_failure_time < op.timestamp)):
                            has_available_site = True
                            break
                    if not has_available_site:
                        return False
                else:
                    site_num = 1 + (var_index % 10)
                    site = self.sites[f"site{site_num}"]
                    # If the site failed after the read operation, or has recovered, allow to continue
                    if (site.history.last_failure_time > op.timestamp and
                            not site.is_up):
                        return False

        return True

    def read(self, transaction_id: str, item: str) -> Optional[any]:
        """Read operation based on snapshot isolation and  concurrency control"""

        transaction = self.active_transactions[transaction_id]
        is_retry = transaction_id in self.waiting_transactions

        # Check will_abort flag
        if transaction.will_abort:
            print(f"Transaction {transaction_id} marked for abort")
            return None
        # target_sites = self._get_variable_sites(item)
        read_timestamp = transaction.start_time
        var_index = int(item[1:])

        is_replicated = var_index % 2 == 0

        # Get the sites that can serve the variable
        target_sites = list(self.sites.values()) if is_replicated else [self.sites[f"site{1 + (var_index % 10)}"]]
        print(f"Target sites: {[s.site_id for s in target_sites]}")

        valid_sites = []
        for site in target_sites:
            # print(f"\nAnalyzing site {site.site_id}:")
            # print(f"Current status: {'UP' if site.is_up else 'DOWN'}")

            if is_replicated:
                last_commit = site.last_commit_time.get(item, 0)
                # print(f"Last commit time for {item} at {site.site_id}: {last_commit}")
                # print(last_commit, read_timestamp, site.history.last_failure_time, site.history.last_recovery_time)
                was_continuously_up = (site.history.last_failure_time == 0 or

                                       (
                                               site.history.last_failure_time > read_timestamp and
                                               site.history.last_recovery_time <= last_commit)
                                       )

                if was_continuously_up:
                    valid_sites.append(site)
                    # print(f"Site {site.site_id} was continuously available")
            else:
                valid_sites.append(site)

        # print(f"\nValidated sites: {[s.site_id for s in valid_sites]}")

        available_sites = [site for site in valid_sites if site.is_up]

        if not available_sites:
            down_valid_sites = [site for site in valid_sites if not site.is_up]
            if down_valid_sites and not is_retry:
                print(f"Transaction {transaction_id} is waiting for sites: {[s.site_id for s in down_valid_sites]}")
                self.waiting_transactions[transaction_id].update([s.site_id for s in down_valid_sites])
                op = Operation_Record(Operation.READ, item, time.time(), Version(-1, 0, "pending"))
                transaction.operations.append(op)

                return None
            else:
                print(f"No available sites for {item}")
                self._abort_transaction(transaction_id)
                return None

        # If valid sites are found, clear the waiting status
        if transaction_id in self.waiting_transactions:
            print(f"Clearing waiting status for transaction {transaction_id}")
            del self.waiting_transactions[transaction_id]
        # First find the initial version
        initial_value = 10 * int(item[1:])  # Calculate the initial value of the variable
        initial_version = Version(initial_value, 0, "init")

        # Collect all versions before the transaction start time
        valid_versions = []
        for site in available_sites:
            version = site.data.get(item)
            if version and version.timestamp <= read_timestamp:
                valid_versions.append((version, site))

        # If no other versions, use the initial version
        if not valid_versions:
            latest_version = initial_version
            latest_site = available_sites[0]
        else:
            # Select the latest valid version
            latest_version, latest_site = max(
                valid_versions,
                key=lambda x: x[0].timestamp
            )

        # Record the read operation
        transaction.record_site_access(latest_site.site_id, 'read', item)
        operation = Operation_Record(Operation.READ, item, time.time(), latest_version)
        transaction.operations.append(operation)
        transaction.read_set.add(item)
        transaction.read_sites[item].add(latest_site.site_id)

        for active_tid, active_trans in self.active_transactions.items():
            if active_tid != transaction_id and item in active_trans.write_set:
                self.rw_edges[active_tid].add(transaction_id)

        print(f"{transaction_id} read {item}={latest_version.value} "
              f"(written by {latest_version.transaction_id}) from site {latest_site.site_id}")
        return latest_version.value

    def write(self, transaction_id: str, item: str, value: any) -> bool:
        """Enhanced write operation with proper site tracking"""
        transaction = self.active_transactions[transaction_id]
        target_sites = self._get_variable_sites(item)

        #
        available_sites = [
            site for site in target_sites
            if site.is_up and item not in site.needs_write_after_recovery
        ]

        if not available_sites:
            return False

        # Create new version
        version = Version(value, time.time(), transaction_id)
        operation = Operation_Record(Operation.WRITE, item, time.time(), version)
        transaction.operations.append(operation)
        transaction.write_set.add(item)

        # Only record the valid sites to be written
        for site in available_sites:
            transaction.record_site_access(site.site_id, 'write')
            transaction.sites_written.add(site.site_id)

        # Update read-write dependencies
        for active_tid, active_trans in self.active_transactions.items():
            if active_tid != transaction_id and item in active_trans.read_set:
                self.rw_edges[transaction_id].add(active_tid)

        return True

    def _check_write_conflicts(self, transaction_id: str) -> bool:
        """
        first-committer-wins
        """
        transaction = self.active_transactions[transaction_id]

        # check First-committer-wins
        for committed_tx in self.committed_transactions:
            if transaction.write_set & committed_tx.write_set:
                return True
        return False

    def _has_cycle(self, transaction_id: str) -> bool:
        """Detect if there is a dangerous read-write cycle"""
        visited = set()
        path = []

        def detect_cycle(current_tid: str) -> bool:
            if current_tid in path:
                # Found a cycle
                cycle_start = path.index(current_tid)
                cycle = path[cycle_start:] + [current_tid]
                # print(f"Found cycle: {' -> '.join(cycle)}")

                # Verify if it is a complete read-write cycle
                is_valid_cycle = True
                for i in range(len(cycle) - 1):
                    current = cycle[i]
                    next_tx = cycle[i + 1]
                    if next_tx not in self.rw_edges[current]:
                        # print(f"Not a pure RW cycle: missing edge {current} -> {next_tx}")
                        is_valid_cycle = False
                        break

                if is_valid_cycle:
                    # Check if the current transaction is the last one
                    tx_ops = self.active_transactions[transaction_id].operations
                    tx_last_time = max(op.timestamp for op in tx_ops)

                    for tid in cycle:
                        if tid == transaction_id:
                            continue
                        other_tx = self.active_transactions.get(tid)
                        if other_tx and other_tx.operations:
                            other_last_time = max(op.timestamp for op in other_tx.operations)
                            if other_last_time > tx_last_time:
                                return False
                    return True

                return False

            visited.add(current_tid)
            path.append(current_tid)

            for next_tid in self.rw_edges.get(current_tid, set()):
                if detect_cycle(next_tid):
                    return True

            path.pop()
            return False

        return detect_cycle(transaction_id)

    def check_waiting_transactions(self, transaction_id: str) -> bool:
        """Check if all required sites for a transaction are up.
        Returns True if all required sites are up, False if any are still down.
        """
        if transaction_id in self.waiting_transactions:
            waiting_sites = self.waiting_transactions[transaction_id]
            for site_id in waiting_sites:
                if not self.sites[site_id].is_up:
                    print(f"{transaction_id} is still waiting for site {site_id} to be up.")
                    return False
            # All required sites are up, so remove from waiting transactions and return True
            print(f"All required sites for {transaction_id} are now up.")
            del self.waiting_transactions[transaction_id]
            return True

    def commit(self, transaction_id: str) -> bool:
        """ commit operation with proper write handling"""
        transaction = self.active_transactions[transaction_id]
        print(transaction.operations)
        print(f"\nProcessing end({transaction_id})...")
        # Check if transaction is marked for abort
        # Check site availability and pending reads

        if transaction_id in self.waiting_transactions:
            if not self.check_waiting_transactions(transaction_id):
                return False

        if transaction.will_abort:
            print(f"Cannot commit {transaction_id}: marked for abort")
            self._abort_transaction(transaction_id)
            return False

        if not self._check_site_availability(transaction_id):
            self._abort_transaction(transaction_id)
            return False

        # Check waiting status
        if transaction_id in self.waiting_transactions and self.waiting_transactions[transaction_id]:
            # print(f"{transaction_id} is still waiting for sites: {self.waiting_transactions[transaction_id]}")
            return False

        # Check the availability of the sites to be written
        for site_id in transaction.sites_written:
            if not self.sites[site_id].is_up:
                # print(f"{transaction_id} aborted because site {site_id} failed")
                self._abort_transaction(transaction_id)
                return False

        if self._has_cycle(transaction_id):
            # print(f"{transaction_id} aborted due to dangerous cycle")
            self._abort_transaction(transaction_id)
            return False

        if self._check_write_conflicts(transaction_id):
            # print(f"Transaction {transaction_id} aborted due to write conflict")
            self._abort_transaction(transaction_id)
            return False

        # Execute write operations, but only write to appropriate sites
        commit_time = time.time()
        affected_sites = []  # To store sites affected by each write for display

        for op in transaction.operations:
            if op.operation == Operation.WRITE:
                var = op.item
                var_index = int(var[1:])
                is_replicated = var_index % 2 == 0

                target_sites = self._get_variable_sites(var)
                for site in target_sites:
                    # Only write to sites that are up and do not need write after recovery
                    if is_replicated:
                        if site.is_up and var not in site.needs_write_after_recovery:
                            site.data[var] = Version(op.version.value, commit_time, transaction_id)
                            site.last_commit_time[var] = commit_time
                            affected_sites.append(site.site_id)

                            # print(f"Written {var}={op.version.value} to site {site.site_id}")
                    else:  # Non-replicated variable
                        if site.is_up:
                            site.data[var] = Version(op.version.value, commit_time, transaction_id)
                            site.last_commit_time[var] = commit_time
                            affected_sites.append(site.site_id)
                            # print(f"Written {var}={op.version.value} to site {site.site_id}")
            elif op.operation == Operation.READ:
                item = op.item
                print(f"{transaction_id} is now able to complete read of {item}")

                # check if the variable is replicated
                var_index = int(item[1:])
                is_replicated = var_index % 2 == 0
                target_sites = list(self.sites.values()) if is_replicated else [
                    self.sites[f"site{1 + (var_index % 10)}"]]
                available_sites = [site for site in target_sites if site.is_up]

                if not available_sites:
                    print(f"Still no available sites for {item}")
                    return False

                # find the latest version
                latest_version = None
                latest_site = None
                for site in available_sites:
                    if item in site.data:
                        version = site.data[item]
                        if latest_version is None or version.timestamp > latest_version.timestamp:
                            latest_version = version
                            latest_site = site

                if latest_version:
                    print(f"{transaction_id} read {item}={latest_version.value} "
                          f"(written by {latest_version.transaction_id}) from site {latest_site.site_id}")
                else:
                    print(f"No valid version found for {item}")
                    return False

        if affected_sites:
            print(f"Sites affected by writes for {transaction_id}: {', '.join(affected_sites)}")
        committed_tx = CommittedTransaction(
            transaction_id=transaction_id,
            write_set=transaction.write_set.copy(),
            read_set=transaction.read_set.copy(),
            commit_timestamp=commit_time
        )
        self.committed_transactions.append(committed_tx)

        print(f"{transaction_id} commit")
        del self.active_transactions[transaction_id]
        if transaction_id in self.waiting_transactions:
            del self.waiting_transactions[transaction_id]
        return True

    def _abort_transaction(self, transaction_id: str):
        """
        abort the transaction
        :param transaction_id:
        :return:
        """
        if transaction_id in self.active_transactions:
            del self.active_transactions[transaction_id]
        if transaction_id in self.rw_edges:
            del self.rw_edges[transaction_id]
        for edges in self.rw_edges.values():
            edges.discard(transaction_id)
        print(f"{transaction_id} abort")

    def print_system_state(self):
        """Prints the current system state with specified formatting."""
        print("\n=== Current System State ===")
        for site_id, site in sorted(self.sites.items()):
            if site.is_up:
                data_str = ", ".join(f"{var}: {version.value}" for var, version in sorted(site.data.items()))
                print(f"site {site_id} – {data_str}")
            else:
                print(f"site {site_id} – DOWN")
