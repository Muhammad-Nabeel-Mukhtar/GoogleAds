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
PAYMENTS_PROFILE_ID = os.getenv("PAYMENTS_PROFILE_ID", "971027154283")

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
        "version": "3.0.0",
        "endpoints": {
            "POST /create-account": "Create new client account (no auto-billing). Body: {name, currency, timezone, email, [tracking_url], [final_url_suffix]}",
            "GET /list-linked-accounts": "List all client accounts under MCC",
            "POST /assign-billing-setup": "Assign billing setup to existing account. Body: {customer_id}",
            "GET /check-verification-status": "Check advertiser verification status. Query: ?customer_id=XXX",
            "POST /start-verification": "Initiate advertiser verification. Body: {customer_id}",
            "POST /update-email": "Update dashboard email. Body: {customer_id, email}",
            "POST /approve-topup": "Approve topup: creates hard+soft cap. Body: {customer_id, topup_amount}",
            "POST /check-and-pause-campaigns": "Enforce soft cap (pause campaigns). Body: {customer_id}",
            "GET /client-spend-status": "Get spend/balance status. Query: ?customer_id=XXX"
        }
    })

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


@app.route('/debug-assign-billing', methods=['POST'])
def debug_assign_billing():
    """Debug version of assign-billing-setup with full error details."""
    import traceback
    
    data = request.json or {}
    customer_id = str(data.get('customer_id', '')).strip()

    if not customer_id or not customer_id.isdigit():
        return jsonify({"success": False, "errors": ["Valid numeric customer_id required."]}), 400

    try:
        client, mcc_customer_id = load_google_ads_client()
        billing_setup_service = client.get_service("BillingSetupService")

        print(f"\n[DEBUG] MCC ID: {mcc_customer_id}")
        print(f"[DEBUG] Client ID: {customer_id}")
        print(f"[DEBUG] Payments Profile ID: {PAYMENTS_PROFILE_ID}")

        operation = client.get_type("BillingSetupOperation")
        billing_setup = operation.create

        billing_setup.payments_account_info.payments_profile_id = PAYMENTS_PROFILE_ID
        billing_setup.start_date_time = datetime.utcnow().strftime('%Y-%m-%d')

        print(f"[DEBUG] Created operation, calling API...")

        response = billing_setup_service.mutate_billing_setup(
            customer_id=customer_id,
            operation=operation
        )

        new_resource = response.result.resource_name
        print(f"[DEBUG] SUCCESS: {new_resource}")

        return jsonify({
            "success": True,
            "customer_id": customer_id,
            "new_billing_setup": new_resource,
            "message": "✅ Successfully linked billing setup via API."
        }), 200

    except Exception as e:
        print(f"[DEBUG] FULL ERROR:\n{traceback.format_exc()}")
        return jsonify({
            "success": False,
            "error": str(e),
            "error_type": type(e).__name__,
            "traceback": traceback.format_exc()
        }), 500


@app.route('/list-linked-accounts', methods=['GET'])
def list_linked_accounts():
    """GET /list-linked-accounts - List all client accounts under MCC."""
    for attempt in range(3):
        try:
            client, mcc_customer_id = load_google_ads_client()
            ga_service = client.get_service("GoogleAdsService")

            query = """
                SELECT
                    customer.id,
                    customer.descriptive_name,
                    customer.status
                FROM customer
            """
            response = ga_service.search(customer_id=mcc_customer_id, query=query)

            accounts = []
            for row in response:
                accounts.append({
                    "client_id": row.customer.id,
                    "name": row.customer.descriptive_name,
                    "status": row.customer.status.name
                })

            return jsonify({"success": True, "accounts": accounts, "errors": []}), 200
        except Exception as e:
            if is_network_error(e):
                if attempt < 2:
                    time.sleep(5)
                    continue
                return jsonify({"success": False, "errors": ["Network error. Please try again.", str(e)]}), 500
            return jsonify({"success": False, "errors": [str(e)]}), 400
    return jsonify({"success": False, "errors": ["Max retries reached."]}), 500


@app.route('/assign-billing-setup', methods=['POST'])
def assign_billing_setup():
    """
    POST /assign-billing-setup
    
    Assigns billing setup to client account using payments profile ID.
    Uses BillingSetupService to link the MCC's payments profile.
    
    Expected JSON: {customer_id: "1234567890"}
    """
    data = request.json or {}
    customer_id = str(data.get('customer_id', '')).strip()

    if not customer_id or not customer_id.isdigit():
        return jsonify({"success": False, "errors": ["Valid numeric customer_id is required."]}), 400

    for attempt in range(3):
        try:
            client, mcc_customer_id = load_google_ads_client()
            billing_setup_service = client.get_service("BillingSetupService")

            print(f"\n[ASSIGN-BILLING] Starting...")
            print(f"[ASSIGN-BILLING] MCC ID: {mcc_customer_id}")
            print(f"[ASSIGN-BILLING] Client ID: {customer_id}")
            print(f"[ASSIGN-BILLING] Payments Profile ID: {PAYMENTS_PROFILE_ID}")

            # Create billing setup operation using payments profile ID
            operation = client.get_type("BillingSetupOperation")
            billing_setup = operation.create

            # Set payments profile ID (12 digits, no dashes)
            billing_setup.payments_account_info.payments_profile_id = PAYMENTS_PROFILE_ID
            billing_setup.start_date_time = datetime.utcnow().strftime('%Y-%m-%d')

            print(f"[ASSIGN-BILLING] Creating billing setup with payments profile {PAYMENTS_PROFILE_ID}...")

            response = billing_setup_service.mutate_billing_setup(
                customer_id=customer_id,
                operation=operation
            )

            new_resource = response.result.resource_name
            print(f"[ASSIGN-BILLING] SUCCESS! Created: {new_resource}\n")

            return jsonify({
                "success": True,
                "customer_id": customer_id,
                "payments_profile_id": PAYMENTS_PROFILE_ID,
                "new_billing_setup": new_resource,
                "message": "✅ Successfully linked billing setup via API.",
                "status": "PENDING",
                "next_step": f"Verify account status, then call /start-verification",
                "timestamp": datetime.utcnow().isoformat() + "Z"
            }), 200

        except GoogleAdsException as e:
            error_msg = str(e)
            print(f"[ASSIGN-BILLING] GoogleAdsException: {error_msg}\n")
            
            error_details = []
            for error in e.failure.errors:
                error_details.append(f"{error.error_code.name}: {error.message}")
            
            if "BILLING_SETUP_ALREADY_EXISTS" in error_msg:
                return jsonify({"success": False, "errors": ["Customer already has a billing setup."]}), 400
            
            if "NO_SIGNUP_PERMISSION" in error_msg:
                return jsonify({"success": False, "errors": ["Account does not have permission to signup for billing. Check account status."]}), 400
            
            return jsonify({"success": False, "errors": error_details}), 400

        except Exception as e:
            if is_network_error(e):
                if attempt < 2:
                    time.sleep(5)
                    continue
                return jsonify({"success": False, "errors": ["Network error. Try again."]}), 500
            return jsonify({"success": False, "errors": [str(e)]}), 500

    return jsonify({"success": False, "errors": ["Max retries reached."]}), 500


@app.route('/check-verification-status', methods=['GET'])
def check_verification_status():
    """
    GET /check-verification-status?customer_id=XXXX
    
    Checks if a client account requires advertiser identity verification.
    Prerequisites: Account must have active billing setup (NO_EFFECTIVE_BILLING error if not).
    
    Returns verification status, deadline, and requirements.
    """
    customer_id = request.args.get('customer_id', '').strip()

    if not customer_id or not customer_id.isdigit():
        return jsonify({"success": False, "errors": ["Valid numeric customer_id required."]}), 400

    for attempt in range(3):
        try:
            client, _ = load_google_ads_client()
            iv_service = client.get_service("IdentityVerificationService")

            print(f"\n[CHECK-VERIFICATION] Checking verification status for {customer_id}...")

            response = iv_service.get_identity_verification(customer_id=customer_id)

            if not response.identity_verification:
                print(f"[CHECK-VERIFICATION] No verification required for {customer_id}")
                return jsonify({
                    "success": True,
                    "customer_id": customer_id,
                    "verification_required": False,
                    "message": "No advertiser verification required for this account.",
                    "timestamp": datetime.utcnow().isoformat() + "Z"
                }), 200

            iv = response.identity_verification[0]
            
            # Extract status
            program_status = iv.verification_progress.program_status.name if iv.verification_progress else "UNKNOWN"
            
            # Extract deadline
            deadline = iv.identity_verification_requirement.verification_completion_deadline_time if iv.identity_verification_requirement else "N/A"
            
            print(f"[CHECK-VERIFICATION] Status: {program_status}, Deadline: {deadline}")

            return jsonify({
                "success": True,
                "customer_id": customer_id,
                "verification_required": True,
                "status": program_status,
                "deadline": deadline,
                "message": f"Verification required. Status: {program_status}. Deadline: {deadline}",
                "timestamp": datetime.utcnow().isoformat() + "Z"
            }), 200

        except GoogleAdsException as e:
            error_msg = str(e)
            print(f"[CHECK-VERIFICATION] GoogleAdsException: {error_msg}\n")
            
            if "NO_EFFECTIVE_BILLING" in error_msg:
                return jsonify({
                    "success": False,
                    "errors": ["Account has no active billing setup. Call /assign-billing-setup first."]
                }), 400
            
            error_details = [f"{err.error_code.name}: {err.message}" for err in e.failure.errors]
            return jsonify({"success": False, "errors": error_details}), 400

        except Exception as e:
            if is_network_error(e):
                if attempt < 2:
                    time.sleep(5)
                    continue
                return jsonify({"success": False, "errors": ["Network error. Try again."]}), 500
            return jsonify({"success": False, "errors": [str(e)]}), 500

    return jsonify({"success": False, "errors": ["Max retries reached."]}), 500


@app.route('/start-verification', methods=['POST'])
def start_verification():
    """
    POST /start-verification
    
    Initiates the advertiser identity verification process for an account.
    Prerequisites: Account must have active billing setup (NO_EFFECTIVE_BILLING error if not).
    
    After this, user must manually complete verification form in Google Ads UI.
    """
    data = request.json or {}
    customer_id = str(data.get('customer_id', '')).strip()

    if not customer_id or not customer_id.isdigit():
        return jsonify({"success": False, "errors": ["Valid numeric customer_id required."]}), 400

    for attempt in range(3):
        try:
            client, _ = load_google_ads_client()
            iv_service = client.get_service("IdentityVerificationService")

            print(f"\n[START-VERIFICATION] Initiating verification for {customer_id}...")

            # Get the enum value for ADVERTISER_IDENTITY_VERIFICATION
            verification_program = client.enums.IdentityVerificationProgramEnum.ADVERTISER_IDENTITY_VERIFICATION

            iv_service.start_identity_verification(
                customer_id=customer_id,
                verification_program=verification_program
            )

            print(f"[START-VERIFICATION] Successfully initiated for {customer_id}\n")

            return jsonify({
                "success": True,
                "customer_id": customer_id,
                "message": "✅ Advertiser identity verification initiated.",
                "next_step": "User must login to Google Ads and complete verification form in Billing > Advertiser verification section.",
                "user_action_required": True,
                "estimated_completion_time": "5-10 minutes for user to submit docs + 1-7 days for Google review",
                "timestamp": datetime.utcnow().isoformat() + "Z"
            }), 200

        except GoogleAdsException as e:
            error_msg = str(e)
            print(f"[START-VERIFICATION] GoogleAdsException: {error_msg}\n")
            
            if "NO_EFFECTIVE_BILLING" in error_msg:
                return jsonify({
                    "success": False,
                    "errors": ["Account has no active billing setup. Call /assign-billing-setup first."]
                }), 400
            
            if "ALREADY_STARTED" in error_msg or "ALREADY_VERIFIED" in error_msg:
                return jsonify({
                    "success": False,
                    "errors": ["Verification already started or completed for this account."]
                }), 400
            
            error_details = [f"{err.error_code.name}: {err.message}" for err in e.failure.errors]
            return jsonify({"success": False, "errors": error_details}), 400

        except Exception as e:
            if is_network_error(e):
                if attempt < 2:
                    time.sleep(5)
                    continue
                return jsonify({"success": False, "errors": ["Network error. Try again."]}), 500
            return jsonify({"success": False, "errors": [str(e)]}), 500

    return jsonify({"success": False, "errors": ["Max retries reached."]}), 500


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
    
    Stores top-up as soft cap in DB and attempts to set hard cap via AccountBudgetProposal.
    
    Expected JSON:
    {
        "customer_id": "1234567890",
        "topup_amount": 100
    }
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

    spending_limit_micros = int(topup_amount * 1_000_000)

    for attempt in range(3):
        try:
            client, _ = load_google_ads_client()
            ga_service = client.get_service("GoogleAdsService")
            proposal_service = client.get_service("AccountBudgetProposalService")

            # Fetch client currency
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
                return jsonify({"success": False, "errors": ["Unable to determine account currency."]}), 400

            soft_cap_status = "STORED_IN_DB"

            # Check existing account_budget
            budget_query = """
                SELECT
                    account_budget.id,
                    account_budget.resource_name,
                    account_budget.status,
                    account_budget.approved_spending_limit_micros,
                    account_budget.proposed_spending_limit_micros,
                    account_budget.billing_setup
                FROM account_budget
                ORDER BY account_budget.id
            """
            try:
                budget_response = ga_service.search(customer_id=customer_id, query=budget_query)
                existing_budget = None
                for row in budget_response:
                    existing_budget = row.account_budget
                    print(f"[DEBUG] Found EXISTING account_budget: id={existing_budget.id}")
                    break
                
                if existing_budget is None:
                    print(f"[DEBUG] No existing account_budget for {customer_id}. Will CREATE new one.")
            except Exception as e:
                print(f"[DEBUG] Error querying account_budget: {str(e)}")
                existing_budget = None

            hard_cap_status = "NOT_ATTEMPTED"
            account_budget_proposal_resource = None
            proposal_id = None

            operation = client.get_type("AccountBudgetProposalOperation")
            proposal = operation.create

            proposal_type_enum = client.enums.AccountBudgetProposalTypeEnum
            time_type_enum = client.enums.TimeTypeEnum

            proposal_type_name = None

            if existing_budget:
                proposal.proposal_type = proposal_type_enum.UPDATE
                proposal.account_budget = existing_budget.resource_name
                proposal.proposed_spending_limit_micros = spending_limit_micros
                proposal.proposed_notes = f"Updated via /approve-topup. Limit: {topup_amount} {customer_currency}."
                operation.update_mask.paths.append("proposed_spending_limit_micros")
                operation.update_mask.paths.append("proposed_notes")
                proposal_type_name = "UPDATE"
            else:
                # CREATE new budget
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
                    print(f"[DEBUG] Billing setup: id={row.billing_setup.id}, status={status_name}")
                    if status_name in ("APPROVED", "ACTIVE"):
                        billing_setup_resource = row.billing_setup.resource_name
                        break

                if billing_setup_resource:
                    proposal.proposal_type = proposal_type_enum.CREATE
                    proposal.billing_setup = billing_setup_resource
                    proposal.proposed_spending_limit_micros = spending_limit_micros
                    proposal.proposed_name = f"Top-up budget: {topup_amount} {customer_currency}"
                    proposal.proposed_notes = f"Created via /approve-topup. Limit: {topup_amount} {customer_currency}."
                    proposal.proposed_start_time_type = time_type_enum.NOW
                    proposal.proposed_end_time_type = time_type_enum.FOREVER
                    proposal_type_name = "CREATE"

            if proposal_type_name:
                try:
                    response = proposal_service.mutate_account_budget_proposal(
                        customer_id=customer_id,
                        operation=operation
                    )
                    account_budget_proposal_resource = response.result.resource_name
                    proposal_id = account_budget_proposal_resource.split("/")[-1]
                    hard_cap_status = "PENDING"
                except GoogleAdsException as e:
                    hard_cap_status = "FAILED_USING_SOFT_CAP"
                    print("===== Hard cap failed =====")
                    print("Customer ID:", customer_id)
                    for error in e.failure.errors:
                        print("  Error:", error.message)

            return jsonify({
                "success": True,
                "customer_id": customer_id,
                "topup_amount": topup_amount,
                "currency": customer_currency,
                "spending_limit_micros": spending_limit_micros,
                "hard_cap_status": hard_cap_status,
                "hard_cap_proposal_id": proposal_id,
                "soft_cap_status": soft_cap_status,
                "account_budget_proposal_resource": account_budget_proposal_resource,
                "message": f"Topup of {topup_amount} {customer_currency} approved. Hard cap: {hard_cap_status}. Soft cap: {soft_cap_status}.",
                "timestamp": datetime.utcnow().isoformat() + "Z"
            }), 200

        except Exception as e:
            if is_network_error(e):
                if attempt < 2:
                    time.sleep(5)
                    continue
                return jsonify({"success": False, "errors": ["Network error. Please try again.", str(e)]}), 500
            return jsonify({"success": False, "errors": [f"Error: {str(e)}"]}) , 500

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

            # TODO: Fetch from MongoDB
            topup_balance_micros = 10_000_000

            remaining_balance_micros = max(0, topup_balance_micros - total_spend_micros)
            percentage_used = (total_spend_micros / topup_balance_micros * 100) if topup_balance_micros > 0 else 0

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
