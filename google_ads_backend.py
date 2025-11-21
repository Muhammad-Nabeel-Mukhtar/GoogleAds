"""
Google Ads Backend API
- POST /create-account: Create a new client under your MCC
- GET  /list-linked-accounts?mcc_id=...: List all client accounts of any MCC
"""

from flask import Flask, request, jsonify
from google.ads.googleads.client import GoogleAdsClient
import time
import socket
import re
import os

app = Flask(__name__)

# Path to your Google Ads YAML config (keep outside public repo)
GOOGLE_ADS_CONFIG_PATH = os.getenv("GOOGLE_ADS_CONFIG_PATH", "config/google-ads.yaml")
# Permanent MCC Customer ID for account creation—do not expose via frontend
MCC_CUSTOMER_ID = '1331285009'

def is_network_error(e):
    msg = str(e).lower()
    return (
        "getaddrinfo failed" in msg or
        "failed to resolve" in msg or
        "connection refused" in msg or
        "connection reset" in msg or
        "max retries exceeded" in msg or
        "transporterror" in msg or
        "connectionerror" in msg or
        isinstance(e, socket.gaierror)
    )

@app.route('/create-account', methods=['POST'])
def create_account():
    data = request.json or {}
    name = data.get('name', '').strip()
    currency = data.get('currency', '').strip().upper()
    timezone = data.get('timezone', '').strip()
    tracking_url = data.get('tracking_url')
    final_url_suffix = data.get('final_url_suffix')

    errors = []
    if not (1 <= len(name) <= 100 and all(c.isprintable() and c not in "<>/" for c in name)):
        errors.append("Account name must be 1–100 characters, cannot include <, >, or /.")
    if not re.match(r"^[A-Z]{3}$", currency):
        errors.append("Currency must be a 3-letter currency code, e.g. USD, PKR.")
    if not (timezone and all(x != '' for x in timezone.split('/')) and 3 <= len(timezone) <= 50):
        errors.append("Time zone must be a valid string, e.g. Asia/Karachi. See https://developers.google.com/google-ads/api/reference/data/codes-formats#timezone-ids")
    if errors:
        return jsonify({"success": False, "errors": errors, "accounts": []}), 400

    for attempt in range(3):
        try:
            client = GoogleAdsClient.load_from_storage(GOOGLE_ADS_CONFIG_PATH)
            customer_service = client.get_service("CustomerService")
            customer = client.get_type("Customer")
            customer.descriptive_name = name
            customer.currency_code = currency
            customer.time_zone = timezone
            if tracking_url:
                customer.tracking_url_template = tracking_url
            if final_url_suffix:
                customer.final_url_suffix = final_url_suffix
            response = customer_service.create_customer_client(
                customer_id=MCC_CUSTOMER_ID,
                customer_client=customer
            )
            customer_id = response.resource_name.split('/')[-1]
            return jsonify({
                "success": True,
                "resource_name": response.resource_name,
                "customer_id": customer_id,
                "accounts": []
            }), 200
        except Exception as e:
            if is_network_error(e):
                if attempt < 2:
                    time.sleep(5)
                    continue
                return jsonify({
                    "success": False, "errors": [
                        "Network error (unable to reach Google servers). Please try again.", str(e)
                    ], "accounts": []
                }), 500
            err_msg = str(e)
            user_msg = []
            if "currency_code" in err_msg:
                user_msg.append("Possible invalid currency code. Valid codes include USD, PKR, EUR, etc.")
            if "time_zone" in err_msg or "timezone" in err_msg:
                user_msg.append("Possible invalid time zone. See: https://developers.google.com/google-ads/api/reference/data/codes-formats#timezone-ids")
            if "descriptive_name" in err_msg:
                user_msg.append("Problem with the account name. Use 1-100 normal characters, no <, >, or /.")
            return jsonify({"success": False, "errors": user_msg + [err_msg], "accounts": []}), 400
    return jsonify({"success": False, "errors": ["Max network retries reached."], "accounts": []}), 500

@app.route('/list-linked-accounts', methods=['GET'])
def list_linked_accounts():
    """ Query: ?mcc_id=1234567890 """
    mcc_id = request.args.get('mcc_id', '').strip()
    if not mcc_id.isdigit():
        return jsonify({"success": False, "errors": ["Manager customer ID must be numeric."], "accounts": []}), 400
    try:
        client = GoogleAdsClient.load_from_storage(GOOGLE_ADS_CONFIG_PATH)
        ga_service = client.get_service("GoogleAdsService")
        query = """
            SELECT
              customer_client.client_customer,
              customer_client.descriptive_name,
              customer_client.status
            FROM customer_client
            ORDER BY customer_client.descriptive_name
        """
        response = ga_service.search(customer_id=mcc_id, query=query)
        results = []
        for row in response:
            results.append({
                "client_id": row.customer_client.client_customer.split('/')[-1],
                "name": row.customer_client.descriptive_name,
                "status": row.customer_client.status.name
            })
        return jsonify({"success": True, "accounts": results, "errors": []}), 200
    except Exception as e:
        return jsonify({"success": False, "errors": [str(e)], "accounts": []}), 500

@app.route('/', methods=['GET'])
def index():
    return jsonify({
        "message": "Google Ads Backend API",
        "endpoints": {
            "POST /create-account": "Create a new client account. Body: {name, currency, timezone, [tracking_url], [final_url_suffix]}",
            "GET /list-linked-accounts?mcc_id=...": "List all client accounts under this MCC."
        }
    })

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=8080, debug=True)
