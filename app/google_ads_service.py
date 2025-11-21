from google.ads.googleads.client import GoogleAdsClient
import time
import socket
import re
import os

# Your permanent MCC Customer ID for all new account creations
MCC_CUSTOMER_ID = '1331285009'

# Path to Google Ads config; adjust as needed for your deployment
CONFIG_PATH = os.getenv("GOOGLE_ADS_CONFIG_PATH", "config/google-ads.yaml")

VALID_CURRENCIES = ['USD', 'PKR', 'EUR', 'INR', 'GBP']  # Example subset

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

def validate_account_input(name, currency, timezone):
    """
    Validates input for account creation.
    """
    errors = []
    if not (1 <= len(name) <= 100 and all(c.isprintable() and c not in "<>/" for c in name)):
        errors.append("Account name must be 1–100 characters, cannot include <, >, or /.")
    if not re.match(r"^[A-Z]{3}$", currency):
        errors.append("Currency must be a 3-letter code, e.g. USD, PKR.")
    if not (timezone and all(x != '' for x in timezone.split('/')) and 3 <= len(timezone) <= 50):
        errors.append("Time zone must be of the form Region/City (e.g. Asia/Karachi). Full list: https://developers.google.com/google-ads/api/reference/data/codes-formats#timezone-ids")
    return errors

def create_customer_account(name, currency, timezone, tracking_url=None, final_url_suffix=None, max_retries=3):
    """
    Creates a new client account in Google Ads under the configured MCC.

    Returns (success: bool, dict: result)
        - On success: (True, { "resource_name": ..., "customer_id": ..., "errors": [], "accounts": [] })
        - On error:   (False, { "errors": [...], "accounts": [] })
    """
    errors = validate_account_input(name, currency, timezone)
    if errors:
        return False, {"errors": errors, "accounts": []}

    for attempt in range(max_retries):
        try:
            client = GoogleAdsClient.load_from_storage(CONFIG_PATH)
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
            return True, {
                "resource_name": response.resource_name,
                "customer_id": customer_id,
                "errors": [],
                "accounts": []
            }
        except Exception as e:
            if is_network_error(e):
                if attempt < max_retries - 1:
                    time.sleep(5)
                    continue
                return False, {
                    "errors": ["Network error: unable to reach Google servers. Please try again.", str(e)],
                    "accounts": []
                }
            err_msg = str(e)
            user_msg = []
            if "currency_code" in err_msg:
                user_msg.append("Check your currency code. Valid codes include USD, PKR, EUR, etc.")
            if "time_zone" in err_msg or "timezone" in err_msg:
                user_msg.append("Possible invalid time zone. See: https://developers.google.com/google-ads/api/reference/data/codes-formats#timezone-ids")
            if "descriptive_name" in err_msg:
                user_msg.append("Problem with the account name. Use 1-100 characters (no <, >, /).")
            return False, {"errors": user_msg + [err_msg], "accounts": []}
    return False, {"errors": ["Max network retries reached."], "accounts": []}

def list_linked_accounts(mcc_id):
    """
    Lists all (client) accounts under the provided manager (MCC) account.

    Returns (success: bool, dict: result)
        - On success: (True, { "accounts": [ {client_id, name, status}... ], "errors": [] })
        - On error:   (False, { "errors": [...], "accounts": [] })
    """
    if not mcc_id.isdigit():
        return False, {"errors": ["Manager customer ID must be numeric."], "accounts": []}
    try:
        client = GoogleAdsClient.load_from_storage(CONFIG_PATH)
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
        return True, {"accounts": results, "errors": []}
    except Exception as e:
        return False, {"errors": [str(e)], "accounts": []}
