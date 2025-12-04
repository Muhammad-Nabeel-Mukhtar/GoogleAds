"""
Google Ads Backend - Flask API for Client Onboarding

5 Core Endpoints:
  1. POST /create-account       - Create new Google Ads account
  2. POST /update-email         - Update account email
  3. GET  /list-linked-accounts - List all linked accounts
  4. POST /assign-billing-setup - Assign billing to account
  5. POST /approve-topup        - Approve account topup
"""

import os
import json
import time
from datetime import datetime
from flask import Flask, request, jsonify
from google.ads.googleads.client import GoogleAdsClient
from google.ads.googleads.errors import GoogleAdsException

# ============================================================================
# CONFIGURATION
# ============================================================================

app = Flask(__name__)

# Environment variables
GOOGLE_ADS_CONFIG_PATH = os.getenv("GOOGLE_ADS_CONFIG_PATH", "google-ads.yaml")
PAYMENTS_ACCOUNT_ID = os.getenv("PAYMENTS_ACCOUNT_ID")
PAYMENTS_PROFILE_ID = os.getenv("PAYMENTS_PROFILE_ID")

# Validate critical env vars
if not PAYMENTS_ACCOUNT_ID:
    print("[ERROR] PAYMENTS_ACCOUNT_ID env var not set!")
if not PAYMENTS_PROFILE_ID:
    print("[ERROR] PAYMENTS_PROFILE_ID env var not set!")

print(f"[INIT] Payments Account ID: {PAYMENTS_ACCOUNT_ID}")
print(f"[INIT] Payments Profile ID: {PAYMENTS_PROFILE_ID}")

# ============================================================================
# UTILITIES
# ============================================================================

def load_google_ads_client():
    """Load Google Ads client and return (client, mcc_id)."""
    try:
        client = GoogleAdsClient.load_from_storage(GOOGLE_ADS_CONFIG_PATH)

        # Read login_customer_id from YAML config
        import yaml
        with open(GOOGLE_ADS_CONFIG_PATH, "r") as f:
            config = yaml.safe_load(f)

        mcc_id = str(config.get("login_customer_id", "")).replace("-", "").strip()
        print(f"[CONFIG] Loaded MCC ID from config: {mcc_id}")
        return client, mcc_id
    except Exception as e:
        print(f"[ERROR] Failed to load Google Ads client: {str(e)}")
        raise


def is_network_error(exception):
    """Basic heuristic for network-related errors."""
    s = str(exception).lower()
    return any(k in s for k in ["connection", "timeout", "network", "refused", "reset"])


# ============================================================================
# ENDPOINT 1: CREATE ACCOUNT
# ============================================================================

@app.route("/create-account", methods=["POST"])
def create_account():
    """
    POST /create-account

    Creates a new Google Ads account linked to the MCC.

    Expected JSON:
    {
        "email": "user@example.com",
        "account_name": "Account Name"
    }
    """
    data = request.json or {}
    email = str(data.get("email", "")).strip()
    account_name = str(data.get("account_name", "")).strip()

    if not email or "@" not in email:
        return jsonify({"success": False, "errors": ["Valid email required."]}), 400

    if not account_name:
        return jsonify({"success": False, "errors": ["Account name required."]}), 400

    try:
        client, mcc_id = load_google_ads_client()
        customer_service = client.get_service("CustomerService")

        print("\n[CREATE-ACCOUNT] Starting...")
        print(f"[CREATE-ACCOUNT] MCC ID: {mcc_id}")
        print(f"[CREATE-ACCOUNT] Email: {email}")
        print(f"[CREATE-ACCOUNT] Account Name: {account_name}")

        # Create customer operation
        operation = client.get_type("CustomerOperation")
        customer = operation.create
        customer.descriptive_name = account_name
        customer.auto_tagging_enabled = True

        print("[CREATE-ACCOUNT] Calling CustomerService.create_customer_client...")

        # Create the customer under MCC
        response = customer_service.create_customer_client(
            customer_id=mcc_id,
            customer_client_operation=operation,
        )

        # Extract new customer ID
        new_resource_name = response.resource_name  # e.g. "customers/1234567890"
        new_customer_id = new_resource_name.split("/")[-1]

        print(f"[CREATE-ACCOUNT] SUCCESS! New Account ID: {new_customer_id}\n")

        return jsonify({
            "success": True,
            "customer_id": new_customer_id,
            "email": email,
            "account_name": account_name,
            "message": "✅ Account created successfully.",
            "next_step": "Update email and assign billing",
            "timestamp": datetime.utcnow().isoformat() + "Z",
        }), 200

    except GoogleAdsException as e:
        error_details = [f"{err.error_code.name}: {err.message}" for err in e.failure.errors]
        print(f"[CREATE-ACCOUNT] ERROR: {error_details}")
        return jsonify({"success": False, "errors": error_details}), 400

    except Exception as e:
        print(f"[CREATE-ACCOUNT] EXCEPTION: {str(e)}")
        return jsonify({"success": False, "errors": [str(e)]}), 500


# ============================================================================
# ENDPOINT 2: UPDATE EMAIL
# ============================================================================

@app.route("/update-email", methods=["POST"])
def update_email():
    """
    POST /update-email

    Updates the email address for a Google Ads account.

    Expected JSON:
    {
        "customer_id": "1234567890",
        "email": "newemail@example.com"
    }
    """
    data = request.json or {}
    customer_id = str(data.get("customer_id", "")).strip()
    email = str(data.get("email", "")).strip()

    if not customer_id or not customer_id.isdigit():
        return jsonify({"success": False, "errors": ["Valid numeric customer_id required."]}), 400

    if not email or "@" not in email:
        return jsonify({"success": False, "errors": ["Valid email required."]}), 400

    try:
        client, _ = load_google_ads_client()
        customer_service = client.get_service("CustomerService")

        print("\n[UPDATE-EMAIL] Starting...")
        print(f"[UPDATE-EMAIL] Customer ID: {customer_id}")
        print(f"[UPDATE-EMAIL] New Email: {email}")

        # Prepare update operation
        operation = client.get_type("CustomerOperation")
        customer = operation.update
        customer.resource_name = f"customers/{customer_id}"
        customer.email_address = email

        # Field mask
        client.copy_from(
            operation.update_mask,
            {"paths": ["email_address"]},
        )

        print("[UPDATE-EMAIL] Calling CustomerService.mutate_customer...")

        customer_service.mutate_customer(
            customer_id=customer_id,
            operation=operation,
        )

        print("[UPDATE-EMAIL] SUCCESS! Email updated\n")

        return jsonify({
            "success": True,
            "customer_id": customer_id,
            "email": email,
            "message": "✅ Email updated successfully.",
            "timestamp": datetime.utcnow().isoformat() + "Z",
        }), 200

    except GoogleAdsException as e:
        error_details = [f"{err.error_code.name}: {err.message}" for err in e.failure.errors]
        print(f"[UPDATE-EMAIL] ERROR: {error_details}")
        return jsonify({"success": False, "errors": error_details}), 400

    except Exception as e:
        print(f"[UPDATE-EMAIL] EXCEPTION: {str(e)}")
        return jsonify({"success": False, "errors": [str(e)]}), 500


# ============================================================================
# ENDPOINT 3: LIST LINKED ACCOUNTS
# ============================================================================

@app.route("/list-linked-accounts", methods=["GET"])
def list_linked_accounts():
    """
    GET /list-linked-accounts

    Lists all accounts linked to the MCC.
    """
    try:
        client, mcc_id = load_google_ads_client()
        ga_service = client.get_service("GoogleAdsService")

        print("\n[LIST-ACCOUNTS] Starting...")
        print(f"[LIST-ACCOUNTS] MCC ID: {mcc_id}")

        query = """
            SELECT
              customer.id,
              customer.descriptive_name,
              customer.currency_code,
              customer.time_zone,
              customer.tracking_url_template,
              customer.auto_tagging_enabled,
              customer.status
            FROM customer
            ORDER BY customer.id
        """

        print("[LIST-ACCOUNTS] Executing query...")
        response = ga_service.search(customer_id=mcc_id, query=query)

        accounts = []
        for row in response:
            c = row.customer
            account = {
                "customer_id": c.id,
                "name": c.descriptive_name,
                "currency": c.currency_code,
                "timezone": c.time_zone,
                "auto_tagging": c.auto_tagging_enabled,
                "status": c.status.name,
            }
            accounts.append(account)
            print(f"[LIST-ACCOUNTS] Found: {account['customer_id']} - {account['name']}")

        print(f"[LIST-ACCOUNTS] SUCCESS! Found {len(accounts)} accounts\n")

        return jsonify({
            "success": True,
            "mcc_id": mcc_id,
            "accounts_count": len(accounts),
            "accounts": accounts,
            "message": f"Found {len(accounts)} linked accounts.",
            "timestamp": datetime.utcnow().isoformat() + "Z",
        }), 200

    except GoogleAdsException as e:
        error_details = [f"{err.error_code.name}: {err.message}" for err in e.failure.errors]
        print(f"[LIST-ACCOUNTS] ERROR: {error_details}")
        return jsonify({"success": False, "errors": error_details}), 400

    except Exception as e:
        print(f"[LIST-ACCOUNTS] EXCEPTION: {str(e)}")
        return jsonify({"success": False, "errors": [str(e)]}), 500


# ============================================================================
# ENDPOINT 4: ASSIGN BILLING SETUP
# ============================================================================

@app.route("/assign-billing-setup", methods=["POST"])
def assign_billing_setup():
    """
    POST /assign-billing-setup

    Assigns billing setup to a client account using official Google Ads API pattern.
    Links an existing payments account (manager-owned) to the child account.

    Expected JSON:
    {
        "customer_id": "1234567890"
    }
    """
    data = request.json or {}
    customer_id = str(data.get("customer_id", "")).strip()

    if not customer_id or not customer_id.isdigit():
        return jsonify({"success": False, "errors": ["Valid numeric customer_id required."]}), 400

    if not PAYMENTS_ACCOUNT_ID:
        return jsonify({
            "success": False,
            "errors": ["PAYMENTS_ACCOUNT_ID not configured in environment."],
        }), 500

    try:
        client, mcc_customer_id = load_google_ads_client()
        billing_setup_service = client.get_service("BillingSetupService")

        print("\n[BILLING] Starting...")
        print(f"[BILLING] MCC ID: {mcc_customer_id}")
        print(f"[BILLING] Client ID: {customer_id}")
        print(f"[BILLING] Payments Account ID: {PAYMENTS_ACCOUNT_ID}")

        # Resource name of manager-owned payments account
        payments_account_resource = (
            f"customers/{mcc_customer_id}/paymentsAccounts/{PAYMENTS_ACCOUNT_ID}"
        )
        print(f"[BILLING] Resource: {payments_account_resource}")

        # Create billing setup operation
        operation = client.get_type("BillingSetupOperation")
        billing_setup = operation.create

        # Official pattern: use payments_account, not payments_account_info
        billing_setup.payments_account = payments_account_resource
        billing_setup.start_date_time = datetime.utcnow().strftime("%Y-%m-%d")

        print("[BILLING] Calling BillingSetupService.mutate_billing_setup...")

        response = billing_setup_service.mutate_billing_setup(
            customer_id=customer_id,
            operation=operation,
        )

        new_resource = response.result.resource_name
        print(f"[BILLING] SUCCESS: {new_resource}\n")

        return jsonify({
            "success": True,
            "customer_id": customer_id,
            "mcc_id": mcc_customer_id,
            "payments_account_id": PAYMENTS_ACCOUNT_ID,
            "new_billing_setup": new_resource,
            "message": "✅ Billing setup assigned successfully.",
            "status": "PENDING",
            "timestamp": datetime.utcnow().isoformat() + "Z",
        }), 200

    except GoogleAdsException as e:
        error_details = [f"{err.error_code.name}: {err.message}" for err in e.failure.errors]
        print(f"[BILLING] ERROR: {error_details}")

        if any("BILLING_SETUP_ALREADY_EXISTS" in err.message for err in e.failure.errors):
            return jsonify({
                "success": False,
                "errors": ["Account already has a billing setup."],
            }), 400

        if any("INVALID_PAYMENTS_ACCOUNT" in err.message for err in e.failure.errors):
            return jsonify({
                "success": False,
                "errors": [
                    "Invalid payments account for this MCC or region. "
                    "Confirm in the Google Ads UI that this payments account "
                    "is allowed to be used for this child account."
                ],
            }), 400

        return jsonify({"success": False, "errors": error_details}), 400

    except Exception as e:
        print(f"[BILLING] EXCEPTION: {str(e)}")
        return jsonify({"success": False, "errors": [str(e)]}), 500


# ============================================================================
# ENDPOINT 5: APPROVE TOPUP (PLACEHOLDER LOGIC)
# ============================================================================

@app.route("/approve-topup", methods=["POST"])
def approve_topup():
    """
    POST /approve-topup

    Approves a topup request for a Google Ads account.
    (Placeholder: implement real billing/topup logic as needed.)

    Expected JSON:
    {
        "customer_id": "1234567890",
        "amount": 1000
    }
    """
    data = request.json or {}
    customer_id = str(data.get("customer_id", "")).strip()
    amount = data.get("amount", 0)

    if not customer_id or not customer_id.isdigit():
        return jsonify({"success": False, "errors": ["Valid numeric customer_id required."]}), 400

    if not isinstance(amount, (int, float)) or amount <= 0:
        return jsonify({"success": False, "errors": ["Valid amount (> 0) required."]}), 400

    try:
        client, mcc_id = load_google_ads_client()

        print("\n[TOPUP] Starting...")
        print(f"[TOPUP] Customer ID: {customer_id}")
        print(f"[TOPUP] Amount: {amount}")
        print(f"[TOPUP] MCC ID: {mcc_id}")

        # Placeholder: in production, integrate real payment flow
        topup_id = f"topup_{customer_id}_{int(time.time())}"

        print(f"[TOPUP] SUCCESS: Topup {topup_id} approved\n")

        return jsonify({
            "success": True,
            "customer_id": customer_id,
            "amount": amount,
            "topup_id": topup_id,
            "message": f"✅ Topup of {amount} approved for account {customer_id}.",
            "status": "APPROVED",
            "timestamp": datetime.utcnow().isoformat() + "Z",
        }), 200

    except Exception as e:
        print(f"[TOPUP] EXCEPTION: {str(e)}")
        return jsonify({"success": False, "errors": [str(e)]}), 500


# ============================================================================
# HEALTH CHECK
# ============================================================================

@app.route("/health", methods=["GET"])
def health():
    """Simple health check."""
    return jsonify({
        "status": "healthy",
        "timestamp": datetime.utcnow().isoformat() + "Z",
    }), 200


# ============================================================================
# ERROR HANDLERS
# ============================================================================

@app.errorhandler(404)
def not_found(error):
    return jsonify({"success": False, "errors": ["Endpoint not found."]}), 404


@app.errorhandler(500)
def internal_error(error):
    return jsonify({"success": False, "errors": ["Internal server error."]}), 500


# ============================================================================
# MAIN
# ============================================================================

if __name__ == "__main__":
    port = int(os.getenv("PORT", 5000))
    app.run(host="0.0.0.0", port=port, debug=False)
