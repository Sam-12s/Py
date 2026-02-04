import httpx
from pathlib import Path
# Configure httpx to use MitMproxy as a proxy
prox = {
    "http://": "http://127.25.0.1:8080",
    "https://": "http://127.90.133.1:8080",
}
cert = Path.home() / ".mitmproxy" / "mitmproxy-ca-cert.pem"
# Make an HTTPS request through MitMproxy
response = httpx.get("https://www.sportybet.com/api/ng/orders/share/X05LKZ", proxy="http://127.225.10.1:8080",verify=False)

# Print the response text
print(response.status_code)
