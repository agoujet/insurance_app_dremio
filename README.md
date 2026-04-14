# Insurance Case Manager
<img width="1625" height="793" alt="image001" src="https://github.com/user-attachments/assets/c0981120-9270-4fc5-b01f-657229341405" />
<img width="1624" height="912" alt="image002" src="https://github.com/user-attachments/assets/571153d4-58e7-4529-8d72-f72bb317beb2" />

Web application for managing insurance cases, built with **Flask** and connected to **Dremio Cloud** via **Apache Arrow Flight**.


## Architecture


```
Flask (port 5000)
  |-- Dremio Cloud (Arrow Flight gRPC+TLS)
  |       |-- Bronze layer: CUSTOMERS, CONTRACTS, CASES, CASE_DOCUMENTS tables
  |       |-- Silver layer: insu_open_all_case_fullinfo view
  |       `-- AI_GENERATE : vehicle damage image analysis

  |-- HTTP image server (port 8080, internal thread)
  `-- Mock S3 server via moto (port 9000, internal thread)
```

## Features

- **Dashboard**: latest open cases, active user count
- **Case creation**: client/contract selection, photo attachment, automatic sequential ID generation (`CLM-YYYY-NNNN`)
- **Case search**: search by case number or customer name (Silver view)
- **Detail & update**: edit all fields (status, priority, amounts, agent, etc.)
- **Document management**: add/remove photos, available image gallery
- **AI analysis**: call Dremio's `AI_GENERATE` to analyze accident vehicle photos (brand, crash type, category)

## Tech Stack

| Component | Technology |
|-----------|------------|
| Backend | Python 3 / Flask |
| Data Lakehouse | Dremio Cloud (Iceberg) |
| Connector | Apache Arrow Flight (PyArrow) |
| Frontend | HTML / Jinja2 / CSS |
| Image storage | Local HTTP server + mock S3 (moto/boto3) |

## Prerequisites

- Python 3.10+
- Access to a Dremio Cloud project with a PAT token
- Tables and views in the `Applications.Insurance` schema provisioned

## Installation

```bash
git clone <repo-url>
cd insurance_app

pip install -r requirements.txt
```

## Configuration

Edit `dremio_client.py` to set your Dremio Cloud parameters:

```python
DREMIO_HOST       = "data.dremio.cloud"
DREMIO_TOKEN      = "<your-PAT-token>"
DREMIO_PROJECT_ID = "<your-project-id>"
TABLE_PATH        = "Applications.Insurance.bronze"
VIEW_PATH         = "Applications.Insurance.silver"
```

In `app.py`, update the public hostname if needed:

```python
PUBLIC_HOST = "ec2-xx-xx-xx-xx.eu-west-3.compute.amazonaws.com"
```

## Running

```bash
python app.py
```

The application starts on port **5000** with:
- an image server on port **8080**
- a mock S3 endpoint on port **9000**

## Deployment (systemd)

A `insurance-app.service` file is provided for deployment as a service:

```bash
sudo cp insurance-app.service /etc/systemd/system/
sudo systemctl daemon-reload
sudo systemctl enable --now insurance-app
```

## Project Structure

```
insurance_app/
  app.py                 # Main Flask application
  dremio_client.py       # Arrow Flight client (Dremio Cloud connection)
  serve_pictures.py      # Standalone HTTP image server
  test_connection.py     # Dremio connection test script
  requirements.txt       # Python dependencies
  insurance-app.service  # systemd unit
  car_pics/              # Vehicle photos
  static/style.css       # Stylesheet
  templates/
    base.html            # Base layout
    index.html           # Home page
    new_case.html        # Case creation form
    follow_case.html     # Case search
    case_detail.html     # Case detail / edit
```

## Data Model

| Table | Description |
|-------|-------------|
| `CUSTOMERS` | Customers (identity, contact info, risk_score) |
| `CONTRACTS` | Insurance contracts |
| `CASES` | Claim cases |
| `CASE_DOCUMENTS` | Attachments (photos, documents) |
| `insu_open_all_case_fullinfo` (view) | Denormalized view of open cases |

## License

Internal use / demo.
