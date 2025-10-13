
from typing import Optional, Sequence
from cassandra.cluster import Cluster, Session, ExecutionProfile, EXEC_PROFILE_DEFAULT
from cassandra.policies import RoundRobinPolicy
from cassandra.query import tuple_factory
from cassandra import ConsistencyLevel
from cassandra.pool import HostDistance

def get_cas_session(contact_points: Sequence[str],
                    keyspace: str,
                    port: int = 9042,
                    request_timeout: float = 30.0) -> Session:
    """
    Create and return a Cassandra Session.

    Args:
        contact_points (Sequence[str]): List of Cassandra nodes (IPs/hostnames).
        keyspace (str): Keyspace to connect to (e.g., "thingsboard").
        port (int): Native transport port, default 9042.
        request_timeout (float): Default request timeout in seconds.

    Returns:
        cassandra.cluster.Session: Connected session with tuple row factory.

    Example:
        session = get_cas_session(["127.0.0.1"], "thingsboard", 9042)
        rows = session.execute("SELECT now() FROM system.local")
    """
    profile = ExecutionProfile(
        load_balancing_policy=RoundRobinPolicy(),
        request_timeout=request_timeout,
        row_factory=tuple_factory
    )

    cluster = Cluster(
        contact_points=list(contact_points),
        port=port,
        execution_profiles={EXEC_PROFILE_DEFAULT: profile},
    )

    session = cluster.connect(keyspace)
    # session.cluster.get_core_connections_per_host(HostDistance.LOCAL)
    return session


def close_cas_session(session: Optional[Session]) -> None:
    """
    Close Cassandra session and its cluster safely.

    Args:
        session (Optional[Session]): Session to close.

    Returns:
        None

    Example:
        close_cas_session(session)
    """
    if session:
        cluster = session.cluster
        session.shutdown()
        if cluster:
            cluster.shutdown()


def cas_smoke_test(session: Session) -> None:
    """
    Run a lightweight connectivity test and raise on failure.

    Args:
        session (Session): Cassandra session to test.

    Returns:
        None

    Example:
        cas_smoke_test(session)
    """
    session.execute("SELECT now() FROM system.local")
