"""
Simple HTTP file server — serves a local folder over HTTP.
Used to expose demo pictures to the Flask app (or any browser).

Usage:
    python serve_pictures.py                  # serves ./pictures on port 8080
    python serve_pictures.py --dir /my/pics   # custom folder
    python serve_pictures.py --port 9000      # custom port
"""

import argparse
import os
from http.server import HTTPServer, SimpleHTTPRequestHandler
from pathlib import Path


class CORSHandler(SimpleHTTPRequestHandler):
    """SimpleHTTPRequestHandler with CORS headers so the Flask app can fetch images."""

    def end_headers(self):
        self.send_header("Access-Control-Allow-Origin", "*")
        self.send_header("Cache-Control", "no-cache")
        super().end_headers()

    def log_message(self, fmt, *args):
        print(f"  [{self.address_string()}] {fmt % args}")


def main():
    parser = argparse.ArgumentParser(description="Serve a picture folder over HTTP")
    parser.add_argument("--dir",  default="pictures", help="Folder to serve (default: ./pictures)")
    parser.add_argument("--port", default=8080, type=int, help="Port (default: 8080)")
    args = parser.parse_args()

    folder = Path(args.dir).resolve()
    folder.mkdir(parents=True, exist_ok=True)   # create folder if missing

    os.chdir(folder)   # SimpleHTTPRequestHandler serves from cwd

    server = HTTPServer(("0.0.0.0", args.port), CORSHandler)
    print(f"\n  Picture server running")
    print(f"  Folder : {folder}")
    print(f"  URL    : http://localhost:{args.port}/")
    print(f"\n  Press Ctrl+C to stop.\n")

    try:
        server.serve_forever()
    except KeyboardInterrupt:
        print("\n  Server stopped.")


if __name__ == "__main__":
    main()
