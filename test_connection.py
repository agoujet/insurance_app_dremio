from pyarrow import flight
from pyarrow.flight import FlightClient
import time
import requests
import os
import pandas as pd

## Arrow Endpoint
location = "grpc+tls://data.dremio.cloud:443"

## Auth Token
token = "bmmrEiPRTrOr35T22AJxxxxxxxxxxxxxxxxxx"

### Your project_id 
project_id = "92a89f04-3e80-4b37-9bd7xxxxxxxxxxxxxxxx"

## Headers for Arrow Requests
headers = [(b"authorization", f"bearer {token}".encode("utf-8"))]

## Query with a limit for my session on the laptop 
query = """
SELECT * FROM Applications.Insurance.bronze.CUSTOMERS LIMIT 2
"""

# Inject project_id into cookie middleware
class ProjectCookieMiddlewareFactory(flight.ClientMiddlewareFactory):
    def __init__(self, project_id):
        self.project_id = project_id

    def start_call(self, info):
        return ProjectCookieMiddleware(self.project_id)

class ProjectCookieMiddleware(flight.ClientMiddleware):
    def __init__(self, project_id):
        self.project_id = project_id

    def sending_headers(self):
        return {b"cookie": f"project_id={self.project_id}".encode("utf-8")}

# Create middleware with your project_id
middleware = [ProjectCookieMiddlewareFactory(project_id)]

# Create Arrow Flight client
# client = FlightClient(location=(location), disable_server_verification=True)
client = FlightClient(location=(location), disable_server_verification=True, middleware=middleware)

# Create Flight call options
options = flight.FlightCallOptions(headers=headers)

# Send the query
start = time.perf_counter()
flight_info = client.get_flight_info(flight.FlightDescriptor.for_command(query),options)


# Retrieve query results
results = client.do_get(flight_info.endpoints[0].ticket, options)

# Convert results to pandas DataFrame
df = results.read_pandas()
end = time.perf_counter()
temps_execution = end - start
print(f"Temps d'exécution : {temps_execution:.2f} secondes")
    
# Display the first 15 rows as a table
print(df.head(15).to_markdown())

