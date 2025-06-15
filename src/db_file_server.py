import http.server
import socketserver
import base64
import zipfile
import io
import os

USERNAME = os.getenv("DB_FILE_SERVER_USER", "admin")
PASSWORD = os.getenv("DB_FILE_SERVER_PASS", "password")
PORT = int(os.getenv("DB_FILE_SERVER_PORT", 8000))
DB_PATH = os.getenv("DB_URL", "tmp/temp.db")
ZIP_NAME = os.getenv("DB_FILE_SERVER_ZIPNAME", "database.zip")


class AuthHandler(http.server.SimpleHTTPRequestHandler):
    def do_AUTHHEAD(self):
        self.send_response(401)
        self.send_header("WWW-Authenticate", 'Basic realm="DB File Server"')
        self.send_header("Content-type", "text/html")
        self.end_headers()

    def do_GET(self):
        # Health check endpoint for Railway
        if self.path == "/health":
            self.send_response(200)
            self.send_header("Content-Type", "text/plain")
            self.end_headers()
            self.wfile.write(b"ok")
            return
        key = f"{USERNAME}:{PASSWORD}".encode()
        auth_key = base64.b64encode(key).decode()
        if self.headers.get("Authorization") == f"Basic {auth_key}":
            if self.path == f"/{ZIP_NAME}":
                if not os.path.exists(DB_PATH):
                    self.send_error(404, f"Database file not found: {DB_PATH}")
                    return
                mem_zip = io.BytesIO()
                with zipfile.ZipFile(mem_zip, "w", zipfile.ZIP_DEFLATED) as zf:
                    zf.write(DB_PATH, arcname=os.path.basename(DB_PATH))
                mem_zip.seek(0)
                self.send_response(200)
                self.send_header("Content-Type", "application/zip")
                self.send_header(
                    "Content-Disposition", f'attachment; filename="{ZIP_NAME}"'
                )
                self.end_headers()
                self.wfile.write(mem_zip.read())
            else:
                self.send_error(404, "File not found")
        else:
            self.do_AUTHHEAD()
            self.wfile.write(b"Authentication required.")


class CustomTCPServer(socketserver.TCPServer):
    allow_reuse_address = True


def start_db_file_server():
    # Bind to 0.0.0.0 for Railway compatibility
    with CustomTCPServer(("0.0.0.0", PORT), AuthHandler) as httpd:
        print(f"[DB File Server] Serving at 0.0.0.0:{PORT}, zip endpoint: /{ZIP_NAME}")
        try:
            httpd.serve_forever()
        except KeyboardInterrupt:
            print("[DB File Server] Shutting down...")
            httpd.server_close()
