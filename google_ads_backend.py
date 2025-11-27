from flask import Flask, request, jsonify
from flask_cors import CORS
from google.ads.googleads.client import GoogleAdsClient
from google.ads.googleads.errors import GoogleAdsException
import time
import socket
import re
import os
from datetime import datetime

app = Flask(__name__)
CORS(app)

GOOGLE_ADS_CONFIG_PATH = os.getenv("GOOGLE_ADS_CONFIG_PATH", "google-ads.yaml")

def load_google_ads_client():
    """
    Load Google Ads client and derive MCC (manager) customer ID from config.
    Uses login_customer_id from google-ads.yaml.
    """
    client = GoogleAdsClient.load_from_storage(GOOGLE_ADS_CONFIG_PATH)
    # login_customer_id may be bytes or str depending on lib version
    login_cid = client.login_customer_id
    if login_cid is None:
        raise ValueError(
            "login_customer_id is not set in google-ads.yaml. "
            "Set it to your manager (MCC) account ID without dashes."
        )
    # Normalize to string without dashes
    mcc_id = str(login_cid).replace("-", "").strip()
    return client, mcc_id

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
    email = data.get('email', '').strip()

    errors = []
    if not (1 <= len(name) <= 100 and all(c.isprintable() and c not in "<>/" for c in name)):
        errors.append("Account name must be 1–100 characters, cannot include <, >, or /.")
    if not re.match(r"^[A-Z]{3}$", currency):
        errors.append("Currency must be a 3-letter currency code, e.g. USD, PKR.")
    if not (timezone and all(x != '' for x in timezone.split('/')) and 3 <= len(timezone) <= 50):
        errors.append("Time zone must be a valid string, e.g. Asia/Karachi.")
    if not email or not re.match(r"^[^@]+@[^@]+\.[^@]+$", email):
        errors.append("Valid access email is required.")
    if errors:
        return jsonify({"success": False, "errors": errors, "accounts": []}), 400

    for attempt in range(3):
        try:
            client, mcc_customer_id = load_google_ads_client()
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
                customer_id=mcc_customer_id,
                customer_client=customer
            )
            customer_id = response.resource_name.split('/')[-1]

            # AUTOMATE USER INVITATION TO CLIENT ACCOUNT (Role is statically READ_ONLY)
            invitation_service = client.get_service("CustomerUserAccessInvitationService")
            invitation_operation = client.get_type("CustomerUserAccessInvitationOperation")
            invitation = invitation_operation.create
            invitation.email_address = email
            invitation.access_role = "READ_ONLY"
            invitation_service.mutate_customer_user_access_invitation(
                customer_id=customer_id,
                operation=invitation_operation
            )

            return jsonify({
                "success": True,
                "resource_name": response.resource_name,
                "customer_id": customer_id,
                "invite_sent": True,
                "invited_email": email,
                "role": "READ_ONLY",
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
            if "email" in err_msg or "access_email" in err_msg:
                user_msg.append("Problem with the provided client email address. Must be valid.")
            return jsonify({"success": False, "errors": user_msg + [err_msg], "accounts": []}), 400
    return jsonify({"success": False, "errors": ["Max network retries reached."], "accounts": []}), 500

@app.route('/list-linked-accounts', methods=['GET'])
def list_linked_accounts():
    # mcc_id comes from YAML (login_customer_id), not from query anymore
    try:
        client, mcc_id = load_google_ads_client()
    except Exception as e:
        return jsonify({"success": False, "errors": [str(e)], "accounts": []}), 500

    try:
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

@app.route('/update-email', methods=['POST'])
def update_email():
    """
    Expects JSON: { "customer_id": "CLIENT_CUSTOMER_ID", "email": "new@email.com" }
    Removes previous dashboard/invite email access, then invites the new one.
    """
    data = request.json or {}
    customer_id = str(data.get('customer_id', '')).strip()
    new_email = str(data.get('email', '')).strip()

    errors = []
    if not customer_id or not customer_id.isdigit():
        errors.append("Valid numeric Google Ads customer_id is required.")
    if not new_email or not re.match(r"^[^@]+@[^@]+\.[^@]+$", new_email):
        errors.append("Valid access email is required.")
    if errors:
        return jsonify({"success": False, "errors": errors}), 400

    for attempt in range(3):
        try:
            client, _ = load_google_ads_client()
            user_access_service = client.get_service("CustomerUserAccessService")
            ga_service = client.get_service("GoogleAdsService")

            # 1. Find active user accesses (READ_ONLY) for the client account
            query = """
                SELECT
                  customer_user_access.resource_name,
                  customer_user_access.email_address,
                  customer_user_access.access_role
                FROM customer_user_access
                WHERE customer_user_access.access_role = READ_ONLY
            """
            response = ga_service.search(customer_id=customer_id, query=query)
            accesses_removed = []
            op_list = []
            for row in response:
                if row.customer_user_access.email_address != new_email:
                    access_resource = row.customer_user_access.resource_name
                    op = client.get_type("CustomerUserAccessOperation")
                    op.remove = access_resource
                    op_list.append(op)
                    accesses_removed.append(row.customer_user_access.email_address)

            # Remove all other READ_ONLY user accesses (except new_email)
            if op_list:
                user_access_service.mutate_customer_user_accesses(
                    customer_id=customer_id,
                    operations=op_list
                )

            # 2. Send invitation to new email
            invitation_service = client.get_service("CustomerUserAccessInvitationService")
            invitation_operation = client.get_type("CustomerUserAccessInvitationOperation")
            invitation = invitation_operation.create
            invitation.email_address = new_email
            invitation.access_role = "READ_ONLY"
            invitation_service.mutate_customer_user_access_invitation(
                customer_id=customer_id,
                operation=invitation_operation
            )

            return jsonify({
                "success": True,
                "customer_id": customer_id,
                "invite_sent": True,
                "invited_email": new_email,
                "access_removed": accesses_removed,
                "role": "READ_ONLY"
            }), 200

        except Exception as e:
            if is_network_error(e):
                if attempt < 2:
                    time.sleep(5)
                    continue
                return jsonify({
                    "success": False, "errors": [
                        "Network error (unable to reach Google servers). Please try again.", str(e)
                    ]
                }), 500
            err_msg = str(e)
            user_msg = []
            if "email" in err_msg or "access_email" in err_msg:
                user_msg.append("Problem with the provided client email address. Must be valid.")
            if "customer_id" in err_msg:
                user_msg.append("Problem with the provided client customer_id — must be the customer (not manager) account id.")
            return jsonify({"success": False, "errors": user_msg + [err_msg]}), 400
    return jsonify({"success": False, "errors": ["Max network retries reached."]}), 500

@app.route('/approve-topup', methods=['POST'])
def approve_topup():
    """
    POST /approve-topup

    When admin approves a user's top-up/deposit request, this endpoint assigns
    a spending limit to the Google Ads client account via Account Budget.

    Expected JSON:
    {
        "customer_id": "1234567890",
        "topup_amount": 100,
        "currency": "USD"
    }
    """
    data = request.json or {}
    customer_id = str(data.get('customer_id', '')).strip()
    topup_amount = data.get('topup_amount')
    currency = str(data.get('currency', '')).strip().upper()

    # Step 1: validate input
    errors = []
    if not customer_id or not customer_id.isdigit():
        errors.append("Valid numeric customer_id is required (client's Google Ads account ID).")
    if topup_amount is None:
        errors.append("topup_amount is required.")
    else:
        try:
            topup_amount = float(topup_amount)
            if topup_amount <= 0:
                errors.append("topup_amount must be greater than 0.")
        except (ValueError, TypeError):
            errors.append("topup_amount must be a valid number.")
    if not re.match(r"^[A-Z]{3}$", currency):
        errors.append("Currency must be a 3-letter currency code (e.g., USD, PKR, EUR).")

    if errors:
        return jsonify({"success": False, "errors": errors}), 400

    # Step 2: convert to micros
    spending_limit_micros = int(topup_amount * 1_000_000)

    for attempt in range(3):
        try:
            client, _ = load_google_ads_client()
            ga_service = client.get_service("GoogleAdsService")
            proposal_service = client.get_service("AccountBudgetProposalService")

            # Step 3: check existing account_budget in client account
            budget_query = """
                SELECT
                    account_budget.id,
                    account_budget.resource_name,
                    account_budget.status,
                    account_budget.approved_spending_limit_micros,
                    account_budget.proposed_spending_limit_micros,
                    account_budget.billing_setup,
                    account_budget.pending_proposal
                FROM account_budget
                ORDER BY account_budget.id
            """
            budget_response = ga_service.search(customer_id=customer_id, query=budget_query)

            existing_budget = None
            for row in budget_response:
                existing_budget = row.account_budget
                break

            # Step 4: build proposal operation
            operation = client.get_type("AccountBudgetProposalOperation")
            proposal = operation.create

            enums = client.get_type("AccountBudgetProposalTypeEnum")
            time_enums = client.get_type("TimeTypeEnum")

            if existing_budget:
                # UPDATE existing budget
                proposal.proposal_type = enums.UPDATE
                proposal.account_budget = existing_budget.resource_name
                proposal.proposed_spending_limit_micros = spending_limit_micros
                proposal.proposed_notes = (
                    f"Updated via /approve-topup. New limit: {topup_amount} {currency}."
                )
                proposal_type_name = "UPDATE"
            else:
                # CREATE new budget (needs billing_setup from client)
                billing_query = """
                    SELECT
                        billing_setup.id,
                        billing_setup.resource_name,
                        billing_setup.status
                    FROM billing_setup
                    ORDER BY billing_setup.id
                """
                billing_response = ga_service.search(customer_id=customer_id, query=billing_query)

                billing_setup_resource = None
                for row in billing_response:
                    status_name = row.billing_setup.status.name
                    if status_name in ("APPROVED", "ACTIVE"):
                        billing_setup_resource = row.billing_setup.resource_name
                        break

                if not billing_setup_resource:
                    return jsonify({
                        "success": False,
                        "errors": [
                            "No active/approved billing setup found for this client. "
                            "Client must be on monthly invoicing before assigning an account budget."
                        ]
                    }), 400

                proposal.proposal_type = enums.CREATE
                proposal.billing_setup = billing_setup_resource
                proposal.proposed_spending_limit_micros = spending_limit_micros
                proposal.proposed_name = f"Top-up budget: {topup_amount} {currency}"
                proposal.proposed_notes = (
                    f"Created via /approve-topup. Limit: {topup_amount} {currency}."
                )
                proposal.proposed_start_time_type = time_enums.NOW
                proposal.proposed_end_time_type = time_enums.FOREVER
                proposal_type_name = "CREATE"

            # Step 5: send proposal
            response = proposal_service.mutate_account_budget_proposal(
                customer_id=customer_id,
                operation=operation
            )

            resource_name = response.result.resource_name
            proposal_id = resource_name.split("/")[-1]

            return jsonify({
                "success": True,
                "customer_id": customer_id,
                "topup_amount": topup_amount,
                "currency": currency,
                "spending_limit_micros": spending_limit_micros,
                "proposal_type": proposal_type_name,
                "proposal_id": proposal_id,
                "status": "PENDING",
                "account_budget_proposal_resource": resource_name,
                "message": (
                    f"Spending limit of {topup_amount} {currency} has been submitted to Google Ads "
                    f"(proposal type: {proposal_type_name}). It will take effect once approved."
                ),
                "timestamp": datetime.utcnow().isoformat() + "Z"
            }), 200

        except GoogleAdsException as e:
            err_msg = str(e)
            user_msg = []
            if "MUTATE_NOT_ALLOWED" in err_msg:
                user_msg.append("Mutate not allowed. Ensure the client is on monthly invoicing and billing is configured.")
            if "INVALID_ARGUMENT" in err_msg:
                user_msg.append("Invalid customer_id or incompatible billing configuration.")
            if "RESOURCE_NOT_FOUND" in err_msg:
                user_msg.append("Required billing or budget resource not found for this client.")
            if "PERMISSION_DENIED" in err_msg:
                user_msg.append("Permission denied. Verify MCC has access to the client account.")
            if not user_msg:
                user_msg.append(f"Google Ads API error: {err_msg}")
            return jsonify({"success": False, "errors": user_msg}), 400

        except Exception as e:
            if is_network_error(e):
                if attempt < 2:
                    time.sleep(5)
                    continue
                return jsonify({
                    "success": False,
                    "errors": [
                        "Network error (unable to reach Google servers). Please try again.",
                        str(e)
                    ]
                }), 500
            return jsonify({
                "success": False,
                "errors": [f"Unexpected error: {str(e)}"]
            }), 500

    return jsonify({
        "success": False,
        "errors": ["Max network retries reached. Please try again later."]
    }), 500

@app.route('/', methods=['GET'])
def index():
    return jsonify({
        "message": "Google Ads Backend API",
        "endpoints": {
            "POST /create-account": "Create a new client account and send dashboard invite (READ_ONLY only). Body: {name, currency, timezone, email, [tracking_url], [final_url_suffix]}",
            "GET /list-linked-accounts": "List all client accounts under the MCC from google-ads.yaml login_customer_id.",
            "POST /update-email": "Remove previous READ_ONLY email(s) and send a new invite with updated email. Body: {customer_id, email}",
            "POST /approve-topup": "When admin approves a user's top-up, assign/adjust account-level spending limit for that client. Body: {customer_id, topup_amount, currency}"
        }
    })

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=8080, debug=True)
