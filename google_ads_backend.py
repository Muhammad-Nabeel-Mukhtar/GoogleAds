from flask import Flask, request, jsonify
from flask_cors import CORS
from google.ads.googleads.client import GoogleAdsClient
from google.ads.googleads.errors import GoogleAdsException
import time
import socket
import re
import os
from datetime import datetime
import logging
from pathlib import Path


import yaml
from pathlib import Path
from app.payments import payments_bp
import sys

logger = logging.getLogger('google.ads.googleads.client')
logger.addHandler(logging.StreamHandler(sys.stdout))
logger.setLevel(logging.DEBUG)  # or INFO


app = Flask(__name__)

CORS(app)

def load_leptage_config() -> None:
    """
    Load config/leptage.yaml into app.config["LEPTAGE_CONFIG"].
    """
    root = Path(__file__).resolve().parent  # project root (folder with google_ads_backend.py)
    config_path = root / "config" / "leptage.yaml"

    if not config_path.exists():
        print(f"[WARN] Leptage config file not found: {config_path}")
        app.config["LEPTAGE_CONFIG"] = {}
        return

    with config_path.open("r", encoding="utf-8") as f:
        data = yaml.safe_load(f) or {}

    app.config["LEPTAGE_CONFIG"] = data


# Call it once at startup
load_leptage_config()

# Register payments blueprint (all /api/payments* and webhook routes)
app.register_blueprint(payments_bp)

GOOGLE_ADS_CONFIG_PATH = os.getenv("GOOGLE_ADS_CONFIG_PATH", "google-ads.yaml")

def load_google_ads_client():
    """Load Google Ads client and derive MCC customer ID from config."""
    client = GoogleAdsClient.load_from_storage(GOOGLE_ADS_CONFIG_PATH)
    login_cid = client.login_customer_id
    if login_cid is None:
        raise ValueError("login_customer_id is not set in google-ads.yaml")
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

@app.route('/', methods=['GET'])
def index():
    return jsonify({
        "message": "Google Ads Backend API with Soft Cap Enforcement",
        "version": "2.0.0",
        "endpoints": {
            "POST /create-account": (
                "Create a new client account under the MCC (no automatic billing assignment). "
                "Body: {name, currency, timezone, email, [tracking_url], [final_url_suffix]}"
            ),
            "GET /list-linked-accounts": (
                "List all client accounts currently linked under the MCC."
            ),
            "POST /assign-billing-setup": (
                "Assign the MCC or child payments account as billing for an existing client account "
                "using Google Ads BillingSetupService. Body: {customer_id}"
            ),
            "POST /update-email": (
                "Update the dashboard/notification email stored for a given client account. "
                "Body: {customer_id, email}"
            ),
            "POST /approve-topup": (
                "Approve a topup and create or update an invoiced account budget (hard cap) for the client "
                "using AccountBudgetProposalService. Body: {customer_id, topup_amount}"
            ),
            "POST /check-and-pause-campaigns": (
                "Check current spend against the configured soft cap and pause all active campaigns "
                "for the client if the soft cap is reached/exceeded. Body: {customer_id}"
            ),
            "GET /client-spend-status": (
                "Get spend and balance status for a client account (based on Google Ads reporting "
                "and the last approved topup in our DB). "
                "Query: ?customer_id=XXX. "
                "Returns: {topup_amount, total_spend, remaining_balance, percentage_used}"
            ),
            "GET /list-payments-accounts": (
                "List payments accounts visible to a given customer (MCC or child) via PaymentsAccountService. "
                "Query: ?customer_id=XXX"
            ),
            "GET /debug-mcc-billing-setups": (
                "Debug endpoint that runs a billing_setup query at MCC level to show manager-level "
                "payments accounts and billing setups."
            ),
            "GET /debug-billing-status": (
                "Debug endpoint to list billing setups and their statuses for a specific customer. "
                "Query: ?customer_id=XXX"
            ),
            "POST /approve-topup-legacy": (
                "Legacy soft-cap-only topup handler (if still deployed). "
                "Body: {customer_id, topup_amount}"
            ),
            "POST /end-account-budget": (
                "End a single active account budget for a client using an END AccountBudgetProposal. "
                "Body: {customer_id}"
            ),
            "POST /end-all-budgets-if-suspended": (
                "If the customer is SUSPENDED, submit END proposals for all active account budgets. "
                "Body: {customer_id}"
            ),
            "GET /check-user-invite-status": (
                "Check whether a user invitation to a Google Ads account is still pending or already accepted. "
                "Query: ?customer_id=XXX&email=user@example.com"
            )
        }
    })


@app.route('/debug-mcc-billing-setups', methods=['GET'])
def debug_mcc_billing_setups():
    """
    GET /debug-mcc-billing-setups

    Runs Google's recommended query against the MCC login_customer_id
    (from google-ads.yaml) to check if any billing setups / payments
    accounts exist at manager level.
    """
    try:
        client, mcc_id = load_google_ads_client()  # login_customer_id should be 1331285009

        ga_service = client.get_service("GoogleAdsService")
        query = """
            SELECT
              billing_setup.payments_account,
              billing_setup.payments_account_info.payments_account_id,
              billing_setup.payments_account_info.payments_account_name,
              billing_setup.payments_account_info.payments_profile_name,
              billing_setup.payments_account_info.payments_profile_id,
              billing_setup.payments_account_info.secondary_payments_profile_id
            FROM billing_setup
        """

        rows = ga_service.search(customer_id=str(mcc_id), query=query)

        results = []
        for row in rows:
            bs = row.billing_setup
            info = bs.payments_account_info
            results.append({
                "billing_setup_resource": bs.resource_name,
                "payments_account": bs.payments_account,
                "payments_account_id": info.payments_account_id,
                "payments_account_name": info.payments_account_name,
                "payments_profile_name": info.payments_profile_name,
                "payments_profile_id": info.payments_profile_id,
                "secondary_payments_profile_id": info.secondary_payments_profile_id,
            })

        return jsonify({
            "success": True,
            "mcc_id": str(mcc_id),
            "count": len(results),
            "billing_setups": results,
        }), 200

    except GoogleAdsException as e:
        errs = []
        for err in e.failure.errors:
            errs.append({"code": str(err.error_code), "message": err.message})
        return jsonify({"success": False, "errors": errs}), 400

    except Exception as e:
        return jsonify({"success": False, "errors": [str(e)]}), 500

@app.route('/debug-billing-status', methods=['GET'])
def debug_billing_status():
    """GET /debug-billing-status?customer_id=XXXX"""
    customer_id = request.args.get('customer_id', '').strip()
    if not customer_id or not customer_id.isdigit():
        return jsonify({"success": False, "errors": ["Valid numeric customer_id required."]}), 400

    try:
        client, _ = load_google_ads_client()
        ga_service = client.get_service("GoogleAdsService")

        query = """
            SELECT
              billing_setup.id,
              billing_setup.resource_name,
              billing_setup.status
            FROM billing_setup
            ORDER BY billing_setup.id
        """

        rows = ga_service.search(customer_id=customer_id, query=query)
        setups = []
        for row in rows:
            setups.append({
                "id": row.billing_setup.id,
                "resource_name": row.billing_setup.resource_name,
                "status": row.billing_setup.status.name,
            })

        return jsonify({
            "success": True,
            "customer_id": customer_id,
            "billing_setups": setups
        }), 200

    except GoogleAdsException as e:
        errs = [{"code": str(err.error_code), "message": err.message} for err in e.failure.errors]
        return jsonify({"success": False, "errors": errs}), 400
    except Exception as e:
        return jsonify({"success": False, "errors": [str(e)]}), 500

from google.ads.googleads.errors import GoogleAdsException

def ensure_customer_active(client, customer_id: str):
    ga_service = client.get_service("GoogleAdsService")
    query = """
        SELECT
          customer.id,
          customer.descriptive_name,
          customer.status
        FROM customer
        LIMIT 1
    """
    rows = ga_service.search(customer_id=customer_id, query=query)
    for row in rows:
        status = row.customer.status.name
        name = row.customer.descriptive_name
        if status != "ENABLED":
            return False, status, name
        return True, status, name
    return False, None, None



def _get_customer_status(client, customer_id: str):
    ga_service = client.get_service("GoogleAdsService")
    query = """
        SELECT
          customer.id,
          customer.descriptive_name,
          customer.status
        FROM customer
        LIMIT 1
    """
    rows = ga_service.search(customer_id=customer_id, query=query)
    for row in rows:
        return row.customer.status.name, row.customer.descriptive_name
    return None, None


@app.route('/end-all-budgets', methods=['POST'])
def end_all_budgets():
    """
    POST /end-all-budgets

    Body:
    {
      "customer_id": "1234567890"
    }

    Flow:
    - Find all non-ended account_budgets.
    - Submit END proposals for each, regardless of suspension status.
    - Note: do NOT include proposed_notes for END proposal type (immutable field error).
    """
    data = request.json or {}
    customer_id = str(data.get('customer_id', '')).strip()

    if not customer_id or not customer_id.isdigit():
        return jsonify({"success": False, "errors": ["Valid numeric customer_id is required."]}), 400

    try:
        client, _ = load_google_ads_client()
        ga_service = client.get_service("GoogleAdsService")
        proposal_service = client.get_service("AccountBudgetProposalService")

        # 1) Block suspended / canceled / closed customers
        ok, status, name = ensure_customer_active(client, customer_id)
        if not ok:
            return jsonify({
                "success": False,
                "errors": [
                    f"Customer {customer_id} ({name}) has status {status}. "
                    "Cannot end budgets for non-ENABLED accounts."
                ],
                "customer_status": status,
            }), 400

        print("\n[END_BUDGETS] Starting...")
        print(f"[END_BUDGETS] Customer ID: {customer_id}")
        print(f"[END_BUDGETS] Customer Name: {name}")
        print(f"[END_BUDGETS] Customer Status: {status}")

        # 2) Query all account budgets
        budget_query = """
            SELECT
              account_budget.id,
              account_budget.resource_name,
              account_budget.status,
              account_budget.billing_setup,
              account_budget.approved_spending_limit_micros,
              account_budget.approved_start_date_time,
              account_budget.approved_end_date_time
            FROM account_budget
            ORDER BY account_budget.id
        """
        budgets = []
        all_budgets_found = []

        for row in ga_service.search(customer_id=customer_id, query=budget_query):
            b = row.account_budget
            all_budgets_found.append({
                "id": b.id,
                "resource_name": b.resource_name,
                "status": b.status.name,
                "billing_setup": b.billing_setup,
                "approved_spending_limit_micros": b.approved_spending_limit_micros,
            })
            print(
                f"[END_BUDGETS] Found budget: id={b.id}, status={b.status.name}, "
                f"billing_setup={b.billing_setup}"
            )

            # Consider everything except ENDED / CANCELLED as eligible to END
            if b.status.name not in ("ENDED", "CANCELLED"):
                budgets.append(b)

        if not budgets:
            return jsonify({
                "success": True,
                "customer_id": customer_id,
                "customer_name": name,
                "customer_status": status,
                "all_budgets_found": all_budgets_found,
                "ended_budgets": [],
                "message": f"No active account budgets to end. Total found: {len(all_budgets_found)}"
            }), 200

        # 3) Submit END proposals for each active budget
        ended = []
        failed = []

        for b in budgets:
            op = client.get_type("AccountBudgetProposalOperation")
            proposal = op.create
            proposal_type_enum = client.enums.AccountBudgetProposalTypeEnum

            proposal.proposal_type = proposal_type_enum.END
            proposal.account_budget = b.resource_name
            # NOTE: Do NOT set proposed_notes for END proposal type
            # It causes immutable_field error

            try:
                resp = proposal_service.mutate_account_budget_proposal(
                    customer_id=customer_id,
                    operation=op
                )
                proposal_resource = resp.result.resource_name
                proposal_id = proposal_resource.split("/")[-1]
                ended.append({
                    "account_budget_id": b.id,
                    "account_budget": b.resource_name,
                    "account_budget_status": b.status.name,
                    "billing_setup": b.billing_setup,
                    "end_proposal_resource": proposal_resource,
                    "end_proposal_id": proposal_id,
                })
                print(f"[END_BUDGETS] SUCCESS: Budget {b.id} ended. Proposal: {proposal_resource}")

            except GoogleAdsException as e:
                error_list = []
                for err in e.failure.errors:
                    error_list.append({
                        "error_code": str(err.error_code),
                        "message": err.message
                    })
                    print(f"[END_BUDGETS] Error on budget {b.id}: {err.message}")
                failed.append({
                    "account_budget_id": b.id,
                    "account_budget": b.resource_name,
                    "errors": error_list
                })

        return jsonify({
            "success": True,
            "customer_id": customer_id,
            "customer_name": name,
            "customer_status": status,
            "all_budgets_found": all_budgets_found,
            "ended_budgets": ended,
            "failed_to_end": failed,
            "message": (
                f"END proposals submitted for {len(ended)} active budgets. "
                f"{len(failed)} failed."
            ),
            "timestamp": datetime.utcnow().isoformat() + "Z"
        }), 200

    except GoogleAdsException as e:
        error_details = []
        for err in e.failure.errors:
            error_details.append({
                "error_code": str(err.error_code),
                "message": err.message
            })
        print(f"[END_BUDGETS] GoogleAdsException: {error_details}")
        return jsonify({"success": False, "errors": error_details}), 400

    except Exception as e:
        print(f"[END_BUDGETS] Exception: {str(e)}")
        return jsonify({"success": False, "errors": [str(e)]}), 500




@app.route('/list-payments-accounts', methods=['GET'])
def list_payments_accounts():
    """
    GET /list-payments-accounts?customer_id=XXXX

    Lists payments accounts visible to a *serving* customer
    from the current MCC's login_customer_id.
    
    Only serving (non-manager) accounts can call PaymentsAccountService.
    The payments account's paying_manager_customer field tells you
    if the account is under your manager hierarchy.
    """
    serving_cid = request.args.get('customer_id', '').strip()

    if not serving_cid or not serving_cid.isdigit():
        return jsonify({
            "success": False,
            "errors": ["Valid numeric customer_id (serving account) is required."],
        }), 400

    try:
        client, mcc_id = load_google_ads_client()

        service = client.get_service("PaymentsAccountService")
        request_proto = client.get_type("ListPaymentsAccountsRequest")
        request_proto.customer_id = serving_cid  # must be serving account, not manager

        response = service.list_payments_accounts(request=request_proto)

        results = []
        for pa in response.payments_accounts:
            results.append({
                "resource_name": pa.resource_name,
                "payments_account_id": pa.payments_account_id,
                "payments_profile_id": pa.payments_profile_id,
                "paying_manager_customer": pa.paying_manager_customer,
            })

        return jsonify({
            "success": True,
            "mcc_login_customer_id": mcc_id,
            "serving_customer_id": serving_cid,
            "count": len(results),
            "payments_accounts": results,
            "timestamp": datetime.utcnow().isoformat() + "Z"
        }), 200

    except GoogleAdsException as e:
        error_details = []
        for err in e.failure.errors:
            error_details.append({
                "error_code": str(err.error_code),
                "message": err.message
            })
        return jsonify({"success": False, "errors": error_details}), 400

    except Exception as e:
        return jsonify({"success": False, "errors": [str(e)]}), 500


from google.ads.googleads.errors import GoogleAdsException

@app.route('/check-user-invite-status', methods=['GET'])
def check_user_invite_status():
    """
    GET /check-user-invite-status?customer_id=XXX&email=someone@example.com

    Returns:
      - invitation_status: PENDING / NOT_FOUND
      - If NOT_FOUND, also tells you whether the user is already active on the account.
    """
    customer_id = (request.args.get('customer_id') or '').strip()
    email = (request.args.get('email') or '').strip()

    errors = []
    if not customer_id or not customer_id.isdigit():
        errors.append("Valid numeric customer_id required.")
    if not email:
        errors.append("email query parameter is required.")

    if errors:
        return jsonify({"success": False, "errors": errors}), 400

    try:
        client, _ = load_google_ads_client()
        ga_service = client.get_service("GoogleAdsService")

        # 1) Check pending invitations for this email
        invite_query = f"""
            SELECT
              customer_user_access_invitation.invitation_id,
              customer_user_access_invitation.email_address,
              customer_user_access_invitation.access_role,
              customer_user_access_invitation.invitation_status,
              customer_user_access_invitation.creation_date_time
            FROM
              customer_user_access_invitation
            WHERE
              customer_user_access_invitation.email_address = '{email}'
        """

        pending_invites = []
        for row in ga_service.search(customer_id=customer_id, query=invite_query):
            inv = row.customer_user_access_invitation
            pending_invites.append({
                "invitation_id": inv.invitation_id,
                "email": inv.email_address,
                "access_role": inv.access_role.name,
                "invitation_status": inv.invitation_status.name,
                "creation_date_time": inv.creation_date_time,
            })

        # 2) If at least one invitation is still PENDING, report that
        for inv in pending_invites:
            if inv["invitation_status"] == "PENDING":
                return jsonify({
                    "success": True,
                    "customer_id": customer_id,
                    "email": email,
                    "invitation_status": "PENDING",
                    "details": inv,
                    "message": "User invitation is still PENDING for this email."
                }), 200

        # 3) No pending invite; check if user is already active on the account
        access_query = f"""
            SELECT
              customer_user_access.user_id,
              customer_user_access.email_address,
              customer_user_access.access_role,
              customer_user_access.access_creation_date_time,
              customer_user_access.inviter_user_email_address
            FROM
              customer_user_access
            WHERE
              customer_user_access.email_address = '{email}'
        """

        active_user = None
        for row in ga_service.search(customer_id=customer_id, query=access_query):
            ua = row.customer_user_access
            active_user = {
                "user_id": ua.user_id,
                "email": ua.email_address,
                "access_role": ua.access_role.name,
                "access_creation_date_time": ua.access_creation_date_time,
                "inviter_email": ua.inviter_user_email_address,
            }
            break

        if active_user:
            return jsonify({
                "success": True,
                "customer_id": customer_id,
                "email": email,
                "invitation_status": "NOT_PENDING",
                "user_active": True,
                "active_user": active_user,
                "message": "No pending invitation. User is already active on this account."
            }), 200

        # 4) No pending invite and no active user
        return jsonify({
            "success": True,
            "customer_id": customer_id,
            "email": email,
            "invitation_status": "NOT_FOUND",
            "user_active": False,
            "message": "No pending invitation found and user is not active on this account."
        }), 200

    except GoogleAdsException as e:
        errs = [{"code": str(err.error_code), "message": err.message} for err in e.failure.errors]
        return jsonify({"success": False, "errors": errs}), 400
    except Exception as e:
        return jsonify({"success": False, "errors": [str(e)]}), 500


@app.route('/check-manager-billing-accounts', methods=['GET'])
def check_manager_billing_accounts():
    """
    GET /check-manager-billing-accounts?serving_customer_id=XXXX

    Checks if the MCC (login_customer_id) has any payments accounts
    that can be used for programmatic billing of child customers.
    
    A payments account is "usable" if its paying_manager_customer 
    matches the MCC's customer ID.
    """
    serving_cid = request.args.get('serving_customer_id', '').strip()

    if not serving_cid or not serving_cid.isdigit():
        return jsonify({
            "success": False,
            "can_do_programmatic_billing": False,
            "errors": ["Valid numeric serving_customer_id is required."],
            "message": "Cannot determine billing eligibility without a serving customer ID."
        }), 400

    try:
        client, mcc_id = load_google_ads_client()

        print(f"\n[CHECK-MANAGER-BILLING] Starting...")
        print(f"[CHECK-MANAGER-BILLING] MCC ID: {mcc_id}")
        print(f"[CHECK-MANAGER-BILLING] Serving Customer ID: {serving_cid}")

        # 1) List payments accounts visible to this serving customer
        service = client.get_service("PaymentsAccountService")
        request_proto = client.get_type("ListPaymentsAccountsRequest")
        request_proto.customer_id = serving_cid

        response = service.list_payments_accounts(request=request_proto)

        all_payments_accounts = []
        manager_payments_accounts = []

        for pa in response.payments_accounts:
            account = {
                "resource_name": pa.resource_name,
                "payments_account_id": pa.payments_account_id,
                "payments_profile_id": pa.payments_profile_id,
                "paying_manager_customer": pa.paying_manager_customer,
            }
            all_payments_accounts.append(account)

            # Extract numeric customer ID from resource name
            # paying_manager_customer format: "customers/1331285009"
            if pa.paying_manager_customer:
                manager_cid = pa.paying_manager_customer.split('/')[-1]
                print(f"[CHECK-MANAGER-BILLING] Checking payment account {pa.payments_account_id}:")
                print(f"  paying_manager_customer: {pa.paying_manager_customer}")
                print(f"  extracted manager_cid: {manager_cid}")
                print(f"  mcc_id: {mcc_id}")
                print(f"  match: {manager_cid == mcc_id}")
                
                if manager_cid == mcc_id:
                    manager_payments_accounts.append(account)
                    print(f"  ✓ ADDED to manager_payments_accounts")

        can_do_billing = len(manager_payments_accounts) > 0

        print(f"[CHECK-MANAGER-BILLING] Total payments accounts: {len(all_payments_accounts)}")
        print(f"[CHECK-MANAGER-BILLING] Manager-owned accounts: {len(manager_payments_accounts)}")
        print(f"[CHECK-MANAGER-BILLING] Can do programmatic billing: {can_do_billing}\n")

        return jsonify({
            "success": True,
            "can_do_programmatic_billing": can_do_billing,
            "mcc_login_customer_id": mcc_id,
            "serving_customer_id": serving_cid,
            "all_payments_accounts_count": len(all_payments_accounts),
            "all_payments_accounts": all_payments_accounts,
            "manager_payments_accounts_count": len(manager_payments_accounts),
            "manager_payments_accounts": manager_payments_accounts,
            "message": (
                f"Manager has {len(manager_payments_accounts)} usable payments account(s) "
                f"out of {len(all_payments_accounts)} total. "
                f"Programmatic billing is {'POSSIBLE' if can_do_billing else 'NOT POSSIBLE'}."
            ),
            "next_step": (
                "If can_do_programmatic_billing=true, you can call /assign-billing-setup with "
                "one of the manager_payments_accounts[].payments_account_id values. "
                "If false, use manual billing via Google Ads UI + logical soft caps."
            ),
            "timestamp": datetime.utcnow().isoformat() + "Z"
        }), 200

    except GoogleAdsException as e:
        error_details = []
        for err in e.failure.errors:
            error_details.append({
                "error_code": str(err.error_code),
                "message": err.message
            })
        return jsonify({
            "success": False,
            "can_do_programmatic_billing": False,
            "errors": error_details,
        }), 400

    except Exception as e:
        print(f"[CHECK-MANAGER-BILLING] EXCEPTION: {str(e)}")
        return jsonify({
            "success": False,
            "can_do_programmatic_billing": False,
            "errors": [str(e)],
        }), 500


# ============================================================================
# DEBUG ENDPOINT: GET PAYMENTS ACCOUNTS
# ============================================================================

@app.route('/debug-get-payments-accounts', methods=['GET'])
def debug_get_payments_accounts():
    """
    GET /debug-get-payments-accounts
    
    Query: ?customer_id=XXXX
    
    Retrieves all payments accounts linked to a customer (for debugging).
    """
    customer_id = request.args.get('customer_id', '').strip()
    
    if not customer_id or not customer_id.isdigit():
        return jsonify({"success": False, "errors": ["Valid numeric customer_id required."]}), 400
    
    try:
        client, mcc_id = load_google_ads_client()
        ga_service = client.get_service("GoogleAdsService")
        
        print(f"\n[DEBUG] Getting payments accounts for customer: {customer_id}")
        
        query = """
            SELECT
              billing_setup.payments_account,
              billing_setup.status,
              billing_setup.start_date_time,
              billing_setup.end_date_time
            FROM billing_setup
            ORDER BY billing_setup.creation_date_time DESC
        """
        
        print(f"[DEBUG] Query: {query}")
        response = ga_service.search(customer_id=customer_id, query=query)
        
        results = []
        for row in response:
            bs = row.billing_setup
            result = {
                "payments_account": bs.payments_account,
                "status": bs.status.name,
                "start_date": bs.start_date_time,
                "end_date": bs.end_date_time
            }
            results.append(result)
            print(f"[DEBUG] Found: {result}")
        
        print(f"[DEBUG] SUCCESS! Found {len(results)} billing setups\n")
        
        return jsonify({
            "success": True,
            "customer_id": customer_id,
            "billing_setups_count": len(results),
            "billing_setups": results,
            "timestamp": datetime.utcnow().isoformat() + "Z"
        }), 200
    
    except GoogleAdsException as e:
        error_details = [f"{err.error_code.name}: {err.message}" for err in e.failure.errors]
        print(f"[DEBUG] ERROR: {error_details}")
        return jsonify({"success": False, "errors": error_details}), 400
    
    except Exception as e:
        print(f"[DEBUG] EXCEPTION: {str(e)}")
        return jsonify({"success": False, "errors": [str(e)]}), 500
# ============================================================================
# ENDPOINT: CHECK BILLING ELIGIBILITY (DEBUG)
# ============================================================================


@app.route('/check-billing-eligibility', methods=['POST'])
def check_billing_eligibility():
    """
    POST /check-billing-eligibility

    Checks if a customer has any billing setups and returns their payments_account
    resource names (if present).

    Expected JSON:
    {
        "customer_id": "1234567890"
    }
    """
    data = request.json or {}
    customer_id = str(data.get('customer_id', '')).strip()

    if not customer_id or not customer_id.isdigit():
        return jsonify({"success": False, "errors": ["Valid numeric customer_id required."]}), 400

    try:
        client, mcc_id = load_google_ads_client()
        ga_service = client.get_service("GoogleAdsService")

        print(f"\n[CHECK-BILLING] Starting...")
        print(f"[CHECK-BILLING] Customer ID: {customer_id}")

        # Query 1: basic customer info (is_manager flag)
        query_manager = f"""
            SELECT
              customer.id,
              customer.manager,
              customer.test_account
            FROM customer
            WHERE customer.id = '{customer_id}'
        """

        print("[CHECK-BILLING] Query 1: Checking if customer is manager...")
        response_manager = ga_service.search(customer_id=customer_id, query=query_manager)

        is_manager = False
        for row in response_manager:
            is_manager = row.customer.manager
            print(f"[CHECK-BILLING] is_manager: {is_manager}")

        # Query 2: list billing setups and their payments_account
        query_billing = """
            SELECT
              billing_setup.resource_name,
              billing_setup.payments_account,
              billing_setup.status,
              billing_setup.start_date_time,
              billing_setup.end_date_time
            FROM billing_setup
        """

        print("[CHECK-BILLING] Query 2: Getting billing setups...")
        response_billing = ga_service.search(customer_id=customer_id, query=query_billing)

        billing_setups = []
        payments_accounts = set()

        for row in response_billing:
            bs = row.billing_setup
            setup = {
                "resource_name": bs.resource_name,
                "payments_account": bs.payments_account,
                "status": bs.status.name,
                "start_date": bs.start_date_time,
                "end_date": bs.end_date_time,
            }
            billing_setups.append(setup)
            if bs.payments_account:
                payments_accounts.add(bs.payments_account)
            print(f"[CHECK-BILLING] Billing Setup: {setup}")

        payments_accounts_list = list(payments_accounts)

        print("[CHECK-BILLING] SUCCESS!\n")

        return jsonify({
            "success": True,
            "customer_id": customer_id,
            "is_manager": is_manager,
            "billing_setups_count": len(billing_setups),
            "billing_setups": billing_setups,
            "payments_accounts": payments_accounts_list,
            "payments_accounts_count": len(payments_accounts_list),
            "message": f"Found {len(billing_setups)} billing setups and {len(payments_accounts_list)} distinct payments_account resource names.",
            "timestamp": datetime.utcnow().isoformat() + "Z"
        }), 200

    except GoogleAdsException as e:
        error_details = []
        for err in e.failure.errors:
            error_details.append({
                "error_code": str(err.error_code),
                "message": err.message
            })
        print(f"[CHECK-BILLING] ERROR: {error_details}")
        return jsonify({"success": False, "errors": error_details}), 400

    except Exception as e:
        print(f"[CHECK-BILLING] EXCEPTION: {str(e)}")
        return jsonify({"success": False, "errors": [str(e)]}), 500


@app.route('/create-account', methods=['POST'])
def create_account():
    """
    POST /create-account
    
    Creates new client account under MCC. NO auto-billing assignment.
    
    Expected JSON:
    {
        "name": "Account Name",
        "currency": "USD",
        "timezone": "Asia/Karachi",
        "email": "client@example.com",
        "tracking_url": "optional",
        "final_url_suffix": "optional"
    }
    """
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

            # Invite user to dashboard
            invitation_service = client.get_service("CustomerUserAccessInvitationService")
            invitation_operation = client.get_type("CustomerUserAccessInvitationOperation")
            invitation = invitation_operation.create
            invitation.email_address = email
            invitation.access_role = client.enums.AccessRoleEnum.STANDARD
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
                "role": "STANDARD",
                "message": f"Account {name} created. Customer ID: {customer_id}. Next: Call /assign-billing-setup",
                "accounts": []
            }), 200

        except Exception as e:
            if is_network_error(e):
                if attempt < 2:
                    time.sleep(5)
                    continue
                return jsonify({"success": False, "errors": ["Network error. Please try again.", str(e)], "accounts": []}), 500
            err_msg = str(e)
            user_msg = []
            if "currency_code" in err_msg:
                user_msg.append("Possible invalid currency code. Valid codes include USD, PKR, EUR, etc.")
            if "time_zone" in err_msg or "timezone" in err_msg:
                user_msg.append("Possible invalid time zone.")
            if "descriptive_name" in err_msg:
                user_msg.append("Problem with the account name.")
            if "email" in err_msg:
                user_msg.append("Problem with the provided email address.")
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


@app.route('/debug-account-health', methods=['GET'])
def debug_account_health():
    """
    GET /debug-account-health?customer_id=XXXX

    Returns a consolidated view of a customer's state:
    - Basic customer info (currency, manager flag, test account)
    - Billing setups and payments accounts
    - Account budgets (limits and status)
    - Current total spend (metrics.cost_micros)
    """
    customer_id = request.args.get('customer_id', '').strip()

    if not customer_id or not customer_id.isdigit():
        return jsonify({"success": False, "errors": ["Valid numeric customer_id required."]}), 400

    try:
        client, mcc_id = load_google_ads_client()
        ga_service = client.get_service("GoogleAdsService")

        print(f"\n[DEBUG-HEALTH] Starting for customer: {customer_id}")
        print(f"[DEBUG-HEALTH] MCC: {mcc_id}")

        # 1) Customer info
        customer_info = {}
        query_customer = f"""
            SELECT
              customer.id,
              customer.descriptive_name,
              customer.currency_code,
              customer.time_zone,
              customer.manager,
              customer.test_account
            FROM customer
            WHERE customer.id = '{customer_id}'
        """
        print("[DEBUG-HEALTH] Query customer info...")
        resp_customer = ga_service.search(customer_id=customer_id, query=query_customer)
        for row in resp_customer:
            c = row.customer
            customer_info = {
                "id": c.id,
                "name": c.descriptive_name,
                "currency_code": c.currency_code,
                "time_zone": c.time_zone,
                "is_manager": c.manager,
                "is_test_account": c.test_account,
            }
            break

        # 2) Billing setups
        billing_setups = []
        payments_accounts_set = set()
        query_billing = """
            SELECT
              billing_setup.resource_name,
              billing_setup.payments_account,
              billing_setup.status,
              billing_setup.start_date_time,
              billing_setup.end_date_time
            FROM billing_setup
        """
        print("[DEBUG-HEALTH] Query billing setups...")
        resp_billing = ga_service.search(customer_id=customer_id, query=query_billing)
        for row in resp_billing:
            bs = row.billing_setup
            setup = {
                "resource_name": bs.resource_name,
                "payments_account": bs.payments_account,
                "status": bs.status.name,
                "start_date": bs.start_date_time,
                "end_date": bs.end_date_time,
            }
            billing_setups.append(setup)
            if bs.payments_account:
                payments_accounts_set.add(bs.payments_account)

        # 3) Account budgets
        account_budgets = []
        query_budget = """
            SELECT
              account_budget.id,
              account_budget.resource_name,
              account_budget.status,
              account_budget.approved_spending_limit_micros,
              account_budget.proposed_spending_limit_micros,
              account_budget.approved_start_date_time,
              account_budget.approved_end_date_time
            FROM account_budget
            ORDER BY account_budget.id
        """
        print("[DEBUG-HEALTH] Query account budgets...")
        resp_budget = ga_service.search(customer_id=customer_id, query=query_budget)
        for row in resp_budget:
            ab = row.account_budget
            budget = {
                "id": ab.id,
                "resource_name": ab.resource_name,
                "status": ab.status.name,
                "approved_spending_limit_micros": ab.approved_spending_limit_micros,
                "proposed_spending_limit_micros": ab.proposed_spending_limit_micros,
                "approved_start_date_time": ab.approved_start_date_time,
                "approved_end_date_time": ab.approved_end_date_time,
            }
            account_budgets.append(budget)

        # 4) Current spend
        metrics_query = """
            SELECT
                customer.currency_code,
                metrics.cost_micros
            FROM customer
        """
        print("[DEBUG-HEALTH] Query current spend...")
        metrics_resp = ga_service.search(customer_id=customer_id, query=metrics_query)

        total_spend_micros = 0
        currency = customer_info.get("currency_code", "USD")
        for row in metrics_resp:
            total_spend_micros = row.metrics.cost_micros
            currency = row.customer.currency_code
            break

        print("[DEBUG-HEALTH] SUCCESS\n")

        return jsonify({
            "success": True,
            "customer_id": customer_id,
            "mcc_id": mcc_id,
            "customer_info": customer_info,
            "billing_setups_count": len(billing_setups),
            "billing_setups": billing_setups,
            "payments_accounts": list(payments_accounts_set),
            "payments_accounts_count": len(payments_accounts_set),
            "account_budgets_count": len(account_budgets),
            "account_budgets": account_budgets,
            "total_spend": total_spend_micros / 1e6,
            "total_spend_micros": total_spend_micros,
            "currency": currency,
            "timestamp": datetime.utcnow().isoformat() + "Z"
        }), 200

    except GoogleAdsException as e:
        error_details = []
        for err in e.failure.errors:
            error_details.append({
                "error_code": str(err.error_code),
                "message": err.message
            })
        print(f"[DEBUG-HEALTH] GoogleAdsException: {error_details}")
        return jsonify({"success": False, "errors": error_details}), 400

    except Exception as e:
        print(f"[DEBUG-HEALTH] EXCEPTION: {str(e)}")
        return jsonify({"success": False, "errors": [str(e)]}), 500


# ============================================================================
# ENDPOINT 4: ASSIGN BILLING SETUP
# ============================================================================

from datetime import datetime, timedelta

@app.route('/assign-billing-setup', methods=['POST'])
def assign_billing_setup():
    """
    POST /assign-billing-setup
    """
    data = request.json or {}
    customer_id = str(data.get('customer_id', '')).strip()

    if not customer_id or not customer_id.isdigit():
        return jsonify({"success": False, "errors": ["Valid numeric customer_id required."]}), 400

    # 1) Read envs
    mcc_payments_resource = os.getenv("MCC_PAYMENTS_ACCOUNT_RESOURCE", "").strip()
    child_payments_id = os.getenv("CHILD_PAYMENTS_ACCOUNT_ID", "").strip()

    if not mcc_payments_resource and not child_payments_id:
        return jsonify({
            "success": False,
            "errors": [
                "No payments account configured. "
                "Set MCC_PAYMENTS_ACCOUNT_RESOURCE or CHILD_PAYMENTS_ACCOUNT_ID in environment."
            ]
        }), 500

    try:
        client, mcc_customer_id = load_google_ads_client()
        billing_setup_service = client.get_service("BillingSetupService")
        ga_service = client.get_service("GoogleAdsService")

        # 1a) Block suspended / canceled / closed customers
        ok, status, name = ensure_customer_active(client, customer_id)
        if not ok:
            return jsonify({
                "success": False,
                "errors": [
                    f"Customer {customer_id} ({name}) has status {status}. "
                    "Billing setup is only allowed for ENABLED accounts."
                ],
                "customer_status": status,
            }), 400

        print("\n[ASSIGN_BILLING] Starting...")
        print(f"[ASSIGN_BILLING] MCC login_customer_id: {mcc_customer_id}")
        print(f"[ASSIGN_BILLING] Target child customer_id: {customer_id}")
        print(f"[ASSIGN_BILLING] MCC_PAYMENTS_ACCOUNT_RESOURCE: {mcc_payments_resource or 'NONE'}")
        print(f"[ASSIGN_BILLING] CHILD_PAYMENTS_ACCOUNT_ID: {child_payments_id or 'NONE'}")

        # 2) If MCC-level payments account is configured, prefer that
        if mcc_payments_resource:
            payments_account_resource = mcc_payments_resource
        else:
            payments_account_resource = (
                f"customers/{customer_id}/paymentsAccounts/{child_payments_id}"
            )

        print(f"[ASSIGN_BILLING] Using payments_account: {payments_account_resource}")

        # 3) Check if a billing setup already exists using this payments_account
        check_query = """
            SELECT
              billing_setup.resource_name,
              billing_setup.payments_account,
              billing_setup.status
            FROM billing_setup
        """
        existing = None
        for row in ga_service.search(customer_id=customer_id, query=check_query):
            bs = row.billing_setup
            if bs.payments_account == payments_account_resource:
                existing = bs
                break

        if existing:
            return jsonify({
                "success": True,
                "customer_id": customer_id,
                "mcc_id": str(mcc_customer_id),
                "payments_account": payments_account_resource,
                "billing_setup_resource": existing.resource_name,
                "status": existing.status.name,
                "message": "Billing setup already exists for this payments account.",
                "result": "ALREADY_ASSIGNED"
            }), 200

        # 4) Create new billing setup
        operation = client.get_type("BillingSetupOperation")
        billing_setup = operation.create
        billing_setup.payments_account = payments_account_resource
        billing_setup.start_time_type = client.enums.TimeTypeEnum.NOW

        print("[ASSIGN_BILLING] Calling mutate_billing_setup...")
        response = billing_setup_service.mutate_billing_setup(
            customer_id=customer_id,
            operation=operation
        )

        new_resource = response.result.resource_name
        print(f"[ASSIGN_BILLING] SUCCESS: {new_resource}")

        return jsonify({
            "success": True,
            "customer_id": customer_id,
            "mcc_id": str(mcc_customer_id),
            "payments_account": payments_account_resource,
            "billing_setup_resource": new_resource,
            "status": "PENDING",
            "message": "Billing setup created successfully. Status will be PENDING until Google approves.",
            "result": "CREATED"
        }), 200

    except GoogleAdsException as e:
        error_details = []
        for err in e.failure.errors:
            error_details.append({
                "error_code": str(err.error_code),
                "message": err.message
            })
        print(f"[ASSIGN_BILLING] GoogleAdsException: {error_details}")
        return jsonify({"success": False, "errors": error_details}), 400

    except Exception as e:
        print(f"[ASSIGN_BILLING] Exception: {str(e)}")
        return jsonify({"success": False, "errors": [str(e)]}), 500







@app.route('/update-email', methods=['POST'])
def update_email():
    """POST /update-email - Update dashboard access email."""
    data = request.json or {}
    customer_id = str(data.get('customer_id', '')).strip()
    email = data.get('email', '').strip()

    if not customer_id or not customer_id.isdigit():
        return jsonify({"success": False, "errors": ["Valid numeric customer_id is required."]}), 400
    if not email or not re.match(r"^[^@]+@[^@]+\.[^@]+$", email):
        return jsonify({"success": False, "errors": ["Valid email is required."]}), 400

    for attempt in range(3):
        try:
            client, _ = load_google_ads_client()
            ga_service = client.get_service("GoogleAdsService")

            query = """
                SELECT
                    customer_user_access.resource_name,
                    customer_user_access.email_address,
                    customer_user_access.access_role
                FROM customer_user_access
            """
            response = ga_service.search(customer_id=customer_id, query=query)

            found_access = None
            for row in response:
                if row.customer_user_access.access_role.name == "READ_ONLY":
                    found_access = row.customer_user_access
                    break

            if found_access:
                cua_service = client.get_service("CustomerUserAccessService")
                operation = client.get_type("CustomerUserAccessOperation")
                operation.remove = found_access.resource_name
                cua_service.mutate_customer_user_access(customer_id=customer_id, operation=operation)

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
                "customer_id": customer_id,
                "email": email,
                "message": f"Email updated to {email}. Invitation sent.",
                "timestamp": datetime.utcnow().isoformat() + "Z"
            }), 200

        except Exception as e:
            if is_network_error(e):
                if attempt < 2:
                    time.sleep(5)
                    continue
                return jsonify({"success": False, "errors": ["Network error. Please try again.", str(e)]}), 500
            return jsonify({"success": False, "errors": [str(e)]}), 400
    return jsonify({"success": False, "errors": ["Max retries reached."]}), 500

@app.route('/approve-topup', methods=['POST'])
def approve_topup():
    """
    POST /approve-topup
    """
    data = request.json or {}
    customer_id = str(data.get('customer_id', '')).strip()
    topup_amount = data.get('topup_amount')

    errors = []
    if not customer_id or not customer_id.isdigit():
        errors.append("Valid numeric customer_id is required.")
    if topup_amount is None:
        errors.append("topup_amount is required.")
    else:
        try:
            topup_amount = float(topup_amount)
            if topup_amount <= 0:
                errors.append("topup_amount must be greater than 0.")
        except (ValueError, TypeError):
            errors.append("topup_amount must be a valid number.")

    if errors:
        return jsonify({"success": False, "errors": errors}), 400

    topup_micros = int(topup_amount * 1_000_000)

    for attempt in range(3):
        try:
            client, _ = load_google_ads_client()
            ga_service = client.get_service("GoogleAdsService")
            proposal_service = client.get_service("AccountBudgetProposalService")

            # 0) Block suspended / canceled / closed customers
            ok, status, name = ensure_customer_active(client, customer_id)
            if not ok:
                return jsonify({
                    "success": False,
                    "errors": [
                        f"Customer {customer_id} ({name}) has status {status}. "
                        "Topups and account budgets are only allowed for ENABLED accounts."
                    ],
                    "customer_status": status,
                }), 400

            # 1) Get account currency
            customer_query = """
                SELECT
                  customer.currency_code
                FROM customer
                LIMIT 1
            """
            customer_response = ga_service.search(customer_id=customer_id, query=customer_query)
            customer_currency = None
            for row in customer_response:
                customer_currency = row.customer.currency_code
                break

            if not customer_currency:
                return jsonify({
                    "success": False,
                    "errors": ["Unable to determine account currency."]
                }), 400

            # 2) Find a usable billing setup (APPROVED_HELD / APPROVED / ACTIVE)
            billing_query = """
                SELECT
                  billing_setup.id,
                  billing_setup.resource_name,
                  billing_setup.status
                FROM billing_setup
                ORDER BY billing_setup.id
            """
            billing_setup_resource = None
            billing_status = None

            for row in ga_service.search(customer_id=customer_id, query=billing_query):
                status_name = row.billing_setup.status.name
                print(f"[TOPUP] Billing setup: id={row.billing_setup.id}, status={status_name}")

                if status_name in ("APPROVED_HELD", "APPROVED", "ACTIVE"):
                    billing_setup_resource = row.billing_setup.resource_name
                    billing_status = status_name
                    break

                if billing_status is None:
                    billing_status = status_name

            if not billing_setup_resource:
                msg = (
                    f"No usable billing setup found. Latest status: {billing_status or 'NONE'}. "
                    f"Billing setup must be APPROVED_HELD, APPROVED, or ACTIVE before approving topups."
                )
                return jsonify({
                    "success": False,
                    "errors": [msg]
                }), 400

            # 3) Check if an account_budget already exists
            budget_query = """
                SELECT
                  account_budget.id,
                  account_budget.resource_name,
                  account_budget.status,
                  account_budget.approved_spending_limit_micros,
                  account_budget.proposed_spending_limit_micros
                FROM account_budget
                ORDER BY account_budget.id
            """
            budget_response = ga_service.search(customer_id=customer_id, query=budget_query)
            existing_budget = None
            for row in budget_response:
                existing_budget = row.account_budget
                print(f"[TOPUP] Found existing account_budget: id={existing_budget.id}")
                break

            operation = client.get_type("AccountBudgetProposalOperation")
            proposal = operation.create
            proposal_type_enum = client.enums.AccountBudgetProposalTypeEnum
            time_type_enum = client.enums.TimeTypeEnum

            new_spending_limit_micros = None
            proposal_id = None
            account_budget_proposal_resource = None

            if existing_budget:
                current_limit = (
                    existing_budget.proposed_spending_limit_micros
                    or existing_budget.approved_spending_limit_micros
                )
                if current_limit is None or current_limit == 0:
                    new_spending_limit_micros = topup_micros
                else:
                    new_spending_limit_micros = current_limit + topup_micros

                proposal.proposal_type = proposal_type_enum.UPDATE
                proposal.account_budget = existing_budget.resource_name
                proposal.proposed_spending_limit_micros = new_spending_limit_micros
                proposal.proposed_notes = (
                    f"Updated via /approve-topup. "
                    f"Increment: {topup_amount} {customer_currency}. "
                    f"New limit: {new_spending_limit_micros / 1e6:.2f} {customer_currency}."
                )
                operation.update_mask.paths.append("proposed_spending_limit_micros")
                operation.update_mask.paths.append("proposed_notes")

            else:
                new_spending_limit_micros = topup_micros
                proposal.proposal_type = proposal_type_enum.CREATE
                proposal.billing_setup = billing_setup_resource
                proposal.proposed_spending_limit_micros = new_spending_limit_micros
                proposal.proposed_name = f"Top-up budget: {topup_amount} {customer_currency}"
                proposal.proposed_notes = (
                    f"Created via /approve-topup. "
                    f"Initial limit: {topup_amount} {customer_currency}."
                )
                proposal.proposed_start_time_type = time_type_enum.NOW
                proposal.proposed_end_time_type = time_type_enum.FOREVER

            # 4) Send AccountBudgetProposal
            try:
                response = proposal_service.mutate_account_budget_proposal(
                    customer_id=customer_id,
                    operation=operation
                )
                account_budget_proposal_resource = response.result.resource_name
                proposal_id = account_budget_proposal_resource.split("/")[-1]
                hard_cap_status = "PENDING"
            except GoogleAdsException as e:
                hard_cap_status = "FAILED"
                print("===== Hard cap failed =====")
                print("Customer ID:", customer_id)
                for error in e.failure.errors:
                    print("  Error:", error.message)

                return jsonify({
                    "success": False,
                    "errors": ["Failed to create/update AccountBudget via AccountBudgetProposal.", str(e)]
                }), 500

            return jsonify({
                "success": True,
                "customer_id": customer_id,
                "billing_setup_status": billing_status,
                "topup_amount": topup_amount,
                "currency": customer_currency,
                "topup_micros": topup_micros,
                "new_spending_limit_micros": new_spending_limit_micros,
                "new_spending_limit": (new_spending_limit_micros / 1e6) if new_spending_limit_micros else None,
                "hard_cap_status": hard_cap_status,
                "hard_cap_proposal_id": proposal_id,
                "account_budget_proposal_resource": account_budget_proposal_resource,
                "message": (
                    f"Topup of {topup_amount} {customer_currency} submitted as "
                    f"AccountBudgetProposal ({'CREATE' if not existing_budget else 'UPDATE'}). "
                    f"Status: {hard_cap_status}."
                ),
                "timestamp": datetime.utcnow().isoformat() + "Z"
            }), 200

        except Exception as e:
            if is_network_error(e):
                if attempt < 2:
                    time.sleep(5)
                    continue
                return jsonify({
                    "success": False,
                    "errors": ["Network error. Please try again later.", str(e)]
                }), 500
            return jsonify({"success": False, "errors": [f"Error: {str(e)}"]}), 500

    return jsonify({"success": False, "errors": ["Max retries reached."]}), 500



@app.route('/check-and-pause-campaigns', methods=['POST'])
def check_and_pause_campaigns():
    """POST /check-and-pause-campaigns - Enforce soft cap by pausing campaigns."""
    data = request.json or {}
    customer_id = str(data.get('customer_id', '')).strip()

    if not customer_id or not customer_id.isdigit():
        return jsonify({"success": False, "errors": ["Valid numeric customer_id is required."]}), 400

    for attempt in range(3):
        try:
            client, _ = load_google_ads_client()
            ga_service = client.get_service("GoogleAdsService")
            campaign_service = client.get_service("CampaignService")

            # Fetch spend metrics
            metrics_query = """
                SELECT
                    customer.currency_code,
                    metrics.cost_micros
                FROM customer
            """
            metrics_response = ga_service.search(customer_id=customer_id, query=metrics_query)
            
            total_spend_micros = 0
            for row in metrics_response:
                total_spend_micros = row.metrics.cost_micros
                break

            # TODO: Fetch stored soft cap from MongoDB
            stored_balance_micros = 10_000_000  # Placeholder: $10

            campaigns_paused = False
            if total_spend_micros >= stored_balance_micros:
                campaign_query = """
                    SELECT
                        campaign.id,
                        campaign.resource_name,
                        campaign.status,
                        campaign.name
                    FROM campaign
                    WHERE campaign.status = ENABLED
                """
                campaign_response = ga_service.search(customer_id=customer_id, query=campaign_query)

                for row in campaign_response:
                    campaign = row.campaign
                    operation = client.get_type("CampaignOperation")
                    operation.update = campaign
                    operation.update.status = client.enums.CampaignStatusEnum.PAUSED
                    operation.update_mask.paths.append("status")

                    campaign_service.mutate_campaigns(customer_id=customer_id, operations=[operation])
                    print(f"[DEBUG] Paused campaign {campaign.id}")
                    campaigns_paused = True

            return jsonify({
                "success": True,
                "customer_id": customer_id,
                "total_spend_micros": total_spend_micros,
                "stored_balance_micros": stored_balance_micros,
                "campaigns_paused": campaigns_paused,
                "message": f"Spend: ${total_spend_micros/1e6:.2f}. Balance: ${stored_balance_micros/1e6:.2f}.",
                "timestamp": datetime.utcnow().isoformat() + "Z"
            }), 200

        except Exception as e:
            if is_network_error(e):
                if attempt < 2:
                    time.sleep(5)
                    continue
                return jsonify({"success": False, "errors": ["Network error. Please try again.", str(e)]}), 500
            return jsonify({"success": False, "errors": [str(e)]}), 400
    return jsonify({"success": False, "errors": ["Max retries reached."]}), 500

@app.route('/client-spend-status', methods=['GET'])
def client_spend_status():
    """GET /client-spend-status?customer_id=XXXX - Return real-time spend and balance."""
    customer_id = request.args.get('customer_id', '').strip()

    if not customer_id or not customer_id.isdigit():
        return jsonify({"success": False, "errors": ["Valid numeric customer_id is required."]}), 400

    for attempt in range(3):
        try:
            client, _ = load_google_ads_client()
            ga_service = client.get_service("GoogleAdsService")

            # 1) Fetch spend metrics
            metrics_query = """
                SELECT
                    customer.currency_code,
                    metrics.cost_micros
                FROM customer
            """
            metrics_response = ga_service.search(customer_id=customer_id, query=metrics_query)

            total_spend_micros = 0
            currency = "USD"
            for row in metrics_response:
                total_spend_micros = row.metrics.cost_micros
                currency = row.customer.currency_code
                break

            # 2) Fetch current account budget limit (hard cap)
            budget_query = """
                SELECT
                    account_budget.approved_spending_limit_micros,
                    account_budget.proposed_spending_limit_micros
                FROM account_budget
                ORDER BY account_budget.id DESC
                LIMIT 1
            """
            topup_balance_micros = 0
            budget_response = ga_service.search(customer_id=customer_id, query=budget_query)
            for row in budget_response:
                approved = row.account_budget.approved_spending_limit_micros
                proposed = row.account_budget.proposed_spending_limit_micros
                topup_balance_micros = proposed or approved or 0
                break

            # If no budget found, treat as zero balance
            remaining_balance_micros = max(0, topup_balance_micros - total_spend_micros)
            percentage_used = (
                (total_spend_micros / topup_balance_micros * 100)
                if topup_balance_micros > 0 else 0
            )

            return jsonify({
                "success": True,
                "customer_id": customer_id,
                "currency": currency,
                "topup_amount": topup_balance_micros / 1e6,
                "topup_balance_micros": topup_balance_micros,
                "total_spend": total_spend_micros / 1e6,
                "total_spend_micros": total_spend_micros,
                "remaining_balance": remaining_balance_micros / 1e6,
                "remaining_balance_micros": remaining_balance_micros,
                "percentage_used": round(percentage_used, 2),
                "timestamp": datetime.utcnow().isoformat() + "Z"
            }), 200

        except Exception as e:
            if is_network_error(e):
                if attempt < 2:
                    time.sleep(5)
                    continue
                return jsonify({"success": False, "errors": ["Network error. Please try again.", str(e)]}), 500
            return jsonify({"success": False, "errors": [str(e)]}), 400

    return jsonify({"success": False, "errors": ["Max retries reached."]}), 500


if __name__ == '__main__':
    
    app.run(host='0.0.0.0', port=8080, debug=False)
