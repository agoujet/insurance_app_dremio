from pyarrow import flight

DREMIO_HOST       = "data.dremio.cloud"
DREMIO_PORT       = "443"
DREMIO_TOKEN      = "bmmrEiPRTrOr35T22Axxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx"
DREMIO_PROJECT_ID = "92a89f04-3e80-4b37-9xxxxxxxxxxxxxxxxxx"
TABLE_PATH        = "Applications.Insurance.bronze"
VIEW_PATH         = "Applications.Insurance.silver"


# ---------------------------------------------------------------------------
# Cookie middleware to inject project_id (required by Dremio Cloud)
# ---------------------------------------------------------------------------

class _ProjectCookieFactory(flight.ClientMiddlewareFactory):
    def __init__(self, project_id):
        self.project_id = project_id

    def start_call(self, info):
        return _ProjectCookieMiddleware(self.project_id)


class _ProjectCookieMiddleware(flight.ClientMiddleware):
    def __init__(self, project_id):
        self.project_id = project_id

    def sending_headers(self):
        return {b"cookie": f"project_id={self.project_id}".encode("utf-8")}


# ---------------------------------------------------------------------------
# Shared client
# ---------------------------------------------------------------------------

_client: flight.FlightClient | None = None
_options: flight.FlightCallOptions | None = None


def _get_client():
    global _client, _options
    if _client is None:
        middleware = [_ProjectCookieFactory(DREMIO_PROJECT_ID)]
        _client = flight.FlightClient(
            f"grpc+tls://{DREMIO_HOST}:{DREMIO_PORT}",
            disable_server_verification=True,
            middleware=middleware,
        )
        headers = [(b"authorization", f"bearer {DREMIO_TOKEN}".encode("utf-8"))]
        _options = flight.FlightCallOptions(headers=headers)
    return _client, _options


# ---------------------------------------------------------------------------
# Public helpers
# ---------------------------------------------------------------------------

def run_query(sql: str) -> list[dict]:
    """Execute a SELECT and return a list of dicts."""
    client, options = _get_client()
    info = client.get_flight_info(flight.FlightDescriptor.for_command(sql), options)
    reader = client.do_get(info.endpoints[0].ticket, options)
    table = reader.read_all()
    columns = table.column_names
    return [dict(zip(columns, row)) for row in zip(*(table.column(c).to_pylist() for c in columns))]


def run_dml(sql: str) -> None:
    """Execute an INSERT / UPDATE / MERGE statement and wait for completion."""
    client, options = _get_client()
    info = client.get_flight_info(flight.FlightDescriptor.for_command(sql), options)
    # Drain the result to block until the DML job finishes
    if info.endpoints:
        reader = client.do_get(info.endpoints[0].ticket, options)
        reader.read_all()  # blocks until Dremio completes the job
