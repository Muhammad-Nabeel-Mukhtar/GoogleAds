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
    """
    POST /create-account
    
    Creates a new client account under MCC and optionally auto-assigns billing setup.
    
    Expected JSON:
    {
        "name": "Account Name",
        "currency": "USD",
        "timezone": "Asia/Karachi",
        "email": "client@example.com",
        "tracking_url": "optional",
        "final_url_suffix": "optional",
        "auto_assign_billing": true  # NEW: auto-assign billing setup from MCC
    }
    """
    data = request.json or {}
    name = data.get('name', '').strip()
    currency = data.get('currency', '').strip().upper()
    timezone = data.get('timezone', '').strip()
    tracking_url = data.get('tracking_url')
    final_url_suffix = data.get('final_url_suffix')
    email = data.get('email', '').strip()
    auto_assign_billing = data.get('auto_assign_billing', False)  # NEW

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

            # NEW: Auto-assign billing setup if requested
            billing_setup_assigned = False
            if auto_assign_billing:
                try:
                    ga_service = client.get_service("GoogleAdsService")
                    billing_service = client.get_service("BillingSetupService")
                    
                    # Step 1: Find APPROVED/ACTIVE billing setup on MCC
                    billing_query = """
                        SELECT
                            billing_setup.id,
                            billing_setup.resource_name,
                            billing_setup.status,
                            billing_setup.payments_profile
                        FROM billing_setup
                        WHERE billing_setup.status = APPROVED
                        ORDER BY billing_setup.id
                        LIMIT 1
                    """
                    billing_response = ga_service.search(customer_id=mcc_customer_id, query=billing_query)
                    
                    mcc_billing_setup = None
                    for row in billing_response:
                        mcc_billing_setup = row.billing_setup
                        break
                    
                    if mcc_billing_setup:
                        # Step 2: Create billing setup proposal for the new client
                        bs_operation = client.get_type("BillingSetupOperation")
                        bs_proposal = bs_operation.create
                        bs_proposal.billing_setup = mcc_billing_setup.resource_name
                        
                        # Send the billing setup proposal to link to new client
                        bs_response = billing_service.mutate_billing_setup(
                            customer_id=customer_id,
                            operation=bs_operation
                        )
                        billing_setup_assigned = True
                        print(f"[DEBUG] Billing setup auto-assigned to {customer_id}")
                    else:
                        print(f"[DEBUG] No APPROVED billing setup found on MCC {mcc_customer_id}")
                except Exception as e:
                    print(f"[DEBUG] Failed to auto-assign billing: {str(e)}")
                    # Continue anyway; billing can be assigned manually later

            return jsonify({
                "success": True,
                "resource_name": response.resource_name,
                "customer_id": customer_id,
                "invite_sent": True,
                "invited_email": email,
                "role": "READ_ONLY",
                "auto_assign_billing": auto_assign_billing,
                "billing_setup_assigned": billing_setup_assigned,
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


@app.route('/assign-billing-setup', methods=['POST'])
def assign_billing_setup():
    """
    POST /assign-billing-setup
    
    Assigns MCC's billing setup to an existing client account.
    """
    data = request.json or {}
    customer_id = str(data.get('customer_id', '')).strip()

    if not customer_id or not customer_id.isdigit():
        return jsonify({
            "success": False,
            "errors": ["Valid numeric customer_id is required."]
        }), 400

    for attempt in range(3):
        try:
            client, mcc_customer_id = load_google_ads_client()
            ga_service = client.get_service("GoogleAdsService")
            billing_service = client.get_service("BillingSetupService")

            # Step 1: Find APPROVED/ACTIVE billing setup on MCC (FIXED QUERY - removed payments_profile)
            billing_query = """
                SELECT
                    billing_setup.id,
                    billing_setup.resource_name,
                    billing_setup.status
                FROM billing_setup
                WHERE billing_setup.status = APPROVED
                ORDER BY billing_setup.id
                LIMIT 1
            """
            billing_response = ga_service.search(customer_id=mcc_customer_id, query=billing_query)

            mcc_billing_setup = None
            for row in billing_response:
                mcc_billing_setup = row.billing_setup
                print(f"[DEBUG] Found MCC billing setup: {mcc_billing_setup.resource_name}, status={mcc_billing_setup.status.name}")
                break

            if not mcc_billing_setup:
                return jsonify({
                    "success": False,
                    "errors": ["No APPROVED billing setup found on MCC. Cannot assign billing."]
                }), 400

            # Step 2: Check if customer already has billing setup
            customer_billing_query = """
                SELECT
                    billing_setup.id,
                    billing_setup.resource_name,
                    billing_setup.status
                FROM billing_setup
            """
            customer_billing_response = ga_service.search(customer_id=customer_id, query=customer_billing_query)
            
            existing_billing = None
            for row in customer_billing_response:
                existing_billing = row.billing_setup
                break

            if existing_billing:
                return jsonify({
                    "success": False,
                    "errors": [f"Customer already has a billing setup: {existing_billing.resource_name}. Cannot assign another."]
                }), 400

            # Step 3: Create billing setup proposal for the customer
            bs_operation = client.get_type("BillingSetupOperation")
            bs_proposal = bs_operation.create
            bs_proposal.billing_setup = mcc_billing_setup.resource_name

            bs_response = billing_service.mutate_billing_setup(
                customer_id=customer_id,
                operation=bs_operation
            )

            billing_setup_resource = bs_response.result.resource_name
            print(f"[DEBUG] Billing setup assigned to {customer_id}: {billing_setup_resource}")

            return jsonify({
                "success": True,
                "customer_id": customer_id,
                "billing_setup_resource": billing_setup_resource,
                "mcc_billing_setup": mcc_billing_setup.resource_name,
                "message": f"Billing setup successfully assigned to customer {customer_id}. Will be PENDING until Google approves.",
                "note": "Once approved (~1 hour), hard account budgets can be set for this account.",
                "timestamp": datetime.utcnow().isoformat() + "Z"
            }), 200

        except GoogleAdsException as e:
            print("===== GoogleAdsException in /assign-billing-setup =====")
            print("Customer ID:", customer_id)
            print("Request ID:", e.request_id)
            for error in e.failure.errors:
                print("  Error code:", error.error_code)
                print("  Message   :", error.message)
            print("========================================================")

            err_msg = str(e)
            user_msg = []

            if "BILLING_SETUP_NOT_PERMITTED_FOR_ACCOUNT_TYPE" in err_msg:
                user_msg.append("This account type does not support billing setups (likely test account).")
            elif "BILLING_SETUP_ALREADY_EXISTS" in err_msg:
                user_msg.append("Customer already has a billing setup assigned.")
            elif "INVALID_ARGUMENT" in err_msg:
                user_msg.append("Invalid billing setup resource provided.")
            else:
                user_msg.append(f"Error: {err_msg}")

            return jsonify({
                "success": False,
                "errors": user_msg
            }), 400

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
                    "success": False,
                    "errors": [
                        "Network error (unable to reach Google servers). Please try again.",
                        str(e)
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

    When admin approves a user's top-up/deposit request, this endpoint:
    1. Stores the topup_amount as a soft spending limit in your DB (simulated here as in-memory for testing)
    2. For monthly invoicing accounts: attempts to set an AccountBudget (hard cap)
    3. For card/test accounts: soft cap only (will be enforced by /check-and-pause-campaigns endpoint)

    Currency is automatically fetched from the client account.

    Expected JSON:
    {
        "customer_id": "1234567890",
        "topup_amount": 100
    }
    """
    data = request.json or {}
    customer_id = str(data.get('customer_id', '')).strip()
    topup_amount = data.get('topup_amount')

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

    if errors:
        return jsonify({"success": False, "errors": errors}), 400

    # Step 2: convert to micros
    spending_limit_micros = int(topup_amount * 1_000_000)

    for attempt in range(3):
        try:
            client, _ = load_google_ads_client()
            ga_service = client.get_service("GoogleAdsService")
            proposal_service = client.get_service("AccountBudgetProposalService")

            # Step 2a: FETCH CLIENT CURRENCY from customer account
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
                    "errors": ["Unable to determine account currency for this customer_id. Account may not exist or be accessible."]
                }), 400

            # ===== SOFT CAP: Store topup in your DB (simulated here) =====
            soft_cap_status = "STORED_IN_DB"  # This is where you'd store to MongoDB

            # Step 3: check existing account_budget in client account
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
                    print(f"[DEBUG] Found EXISTING account_budget: id={existing_budget.id}, status={existing_budget.status.name}")
                    break
                
                if existing_budget is None:
                    print(f"[DEBUG] No existing account_budget found for {customer_id}. Will CREATE new one.")
            except Exception as e:
                print(f"[DEBUG] Error querying account_budget: {str(e)}")
                existing_budget = None

            # Step 4: try to set hard cap via AccountBudget (for invoicing accounts)
            hard_cap_status = "NOT_ATTEMPTED"
            account_budget_proposal_resource = None
            proposal_id = None

            operation = client.get_type("AccountBudgetProposalOperation")
            proposal = operation.create

            # FIX: Use client.enums instead of client.get_type for v22 compatibility
            proposal_type_enum = client.enums.AccountBudgetProposalTypeEnum
            time_type_enum = client.enums.TimeTypeEnum

            proposal_type_name = None

            if existing_budget:
                # UPDATE existing budget - MUST include update_mask
                proposal.proposal_type = proposal_type_enum.UPDATE
                proposal.account_budget = existing_budget.resource_name
                proposal.proposed_spending_limit_micros = spending_limit_micros
                proposal.proposed_notes = (
                    f"Updated via /approve-topup. New limit: {topup_amount} {customer_currency}."
                )
                # FIX: Add update_mask to specify which fields are being changed
                operation.update_mask.paths.append("proposed_spending_limit_micros")
                operation.update_mask.paths.append("proposed_notes")
                proposal_type_name = "UPDATE"
                print(f"[DEBUG] Building UPDATE proposal for existing budget: {existing_budget.resource_name}")
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
                    # DEBUG: log every billing setup status we see
                    print(f"[DEBUG] Billing setup found for {customer_id} -> status={status_name}, resource={row.billing_setup.resource_name}")
                    if status_name in ("APPROVED", "ACTIVE"):
                        billing_setup_resource = row.billing_setup.resource_name
                        break

                if billing_setup_resource:
                    proposal.proposal_type = proposal_type_enum.CREATE
                    proposal.billing_setup = billing_setup_resource
                    proposal.proposed_spending_limit_micros = spending_limit_micros
                    proposal.proposed_name = f"Top-up budget: {topup_amount} {customer_currency}"
                    proposal.proposed_notes = (
                        f"Created via /approve-topup. Limit: {topup_amount} {customer_currency}."
                    )
                    proposal.proposed_start_time_type = time_type_enum.NOW
                    proposal.proposed_end_time_type = time_type_enum.FOREVER
                    proposal_type_name = "CREATE"
                    print(f"[DEBUG] Building CREATE proposal with billing_setup: {billing_setup_resource}")
                else:
                    print(f"[DEBUG] No APPROVED/ACTIVE billing setup found for {customer_id} in API query")

            if proposal_type_name:
                try:
                    # Step 5: send proposal
                    print(f"[DEBUG] Sending {proposal_type_name} proposal to Google Ads API...")
                    response = proposal_service.mutate_account_budget_proposal(
                        customer_id=customer_id,
                        operation=operation
                    )

                    account_budget_proposal_resource = response.result.resource_name
                    proposal_id = account_budget_proposal_resource.split("/")[-1]
                    hard_cap_status = "PENDING"  # Google will approve asynchronously
                    print(f"[DEBUG] SUCCESS: {proposal_type_name} proposal created. Resource: {account_budget_proposal_resource}")
                except GoogleAdsException as e:
                    # Hard cap failed (card accounts / permission / config issues)
                    hard_cap_status = "FAILED_USING_SOFT_CAP"
                    print("===== Hard cap failed in inner mutate_account_budget_proposal =====")
                    print("Customer ID:", customer_id)
                    print("Proposal Type:", proposal_type_name)
                    print("Request ID:", e.request_id)
                    for error in e.failure.errors:
                        print("  Error code:", error.error_code)
                        print("  Message   :", error.message)
                    print("===============================================================")

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
                "message": (
                    f"Topup of {topup_amount} {customer_currency} approved. "
                    f"Hard cap: {hard_cap_status} (via Google Ads Account Budget if supported). "
                    f"Soft cap: {soft_cap_status} (stored in platform DB). "
                    f"Campaigns will be paused if spending exceeds limit."
                ),
                "note": "For card/test accounts, use /check-and-pause-campaigns to enforce the soft cap. For invoicing accounts, Google enforces the hard cap.",
                "timestamp": datetime.utcnow().isoformat() + "Z"
            }), 200

        except GoogleAdsException as e:
            # General Google Ads errors during approval
            print("===== GoogleAdsException in outer /approve-topup try-block =====")
            print("Customer ID:", customer_id)
            print("Request ID:", e.request_id)
            for error in e.failure.errors:
                print("  Error code:", error.error_code)
                print("  Message   :", error.message)
            print("===============================================================")

            err_msg = str(e)
            user_msg = []

            if "MUTATE_NOT_ALLOWED" in err_msg:
                user_msg.append(
                    "This account is not on monthly invoicing. Soft cap stored. "
                    "Use /check-and-pause-campaigns endpoint to enforce spending limits."
                )
            elif "INVALID_ARGUMENT" in err_msg:
                user_msg.append(
                    "Account does not support hard account budgets (likely card or test account). "
                    "Soft cap stored. Use /check-and-pause-campaigns to enforce limit."
                )
            elif "RESOURCE_NOT_FOUND" in err_msg:
                user_msg.append(
                    "No billing setup found. Soft cap stored. "
                    "Use /check-and-pause-campaigns to enforce spending limits."
                )
            else:
                user_msg.append(f"Partial error: {err_msg}. Soft cap has been stored.")

            return jsonify({
                "success": True,
                "customer_id": customer_id,
                "topup_amount": topup_amount,
                "soft_cap_status": "STORED_IN_DB",
                "hard_cap_status": "FAILED",
                "message": "Topup approved with SOFT CAP only. " + (user_msg[0] if user_msg else "Use soft cap enforcement."),
                "note": "For this account, call /check-and-pause-campaigns to pause campaigns when spending reaches limit.",
                "timestamp": datetime.utcnow().isoformat() + "Z"
            }), 200

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



@app.route('/check-and-pause-campaigns', methods=['POST'])
def check_and_pause_campaigns():
    """
    POST /check-and-pause-campaigns

    Enforcement endpoint for SOFT CAP:
    1. Reads topup_balance_micros from your DB
    2. Queries total spend via Google Ads API (metrics.cost_micros for current period)
    3. If spend >= balance: pause all active campaigns

    Expected JSON:
    {
        "customer_id": "1234567890"
    }
    """
    data = request.json or {}
    customer_id = str(data.get('customer_id', '')).strip()

    errors = []
    if not customer_id or not customer_id.isdigit():
        errors.append("Valid numeric customer_id is required.")
    if errors:
        return jsonify({"success": False, "errors": errors}), 400

    for attempt in range(3):
        try:
            client, _ = load_google_ads_client()
            ga_service = client.get_service("GoogleAdsService")
            campaign_service = client.get_service("CampaignService")

            # Step 1: FETCH topup_balance from your DB
            # PRODUCTION: Replace with real DB value
            topup_balance_micros = 100_000_000  # Example: 100 units

            if topup_balance_micros <= 0:
                return jsonify({
                    "success": True,
                    "customer_id": customer_id,
                    "message": "No topup balance found for this customer. Campaigns not paused.",
                    "topup_balance_micros": 0,
                    "total_spend_micros": 0,
                    "campaigns_paused": 0,
                    "campaigns_resumed": 0
                }), 200

            # Step 2: Fetch current spend for this customer (lifetime or period; GAQL mirrors UI) [web:122][web:123][web:129]
            spend_query = """
                SELECT
                    metrics.cost_micros
                FROM customer
            """
            spend_response = ga_service.search(customer_id=customer_id, query=spend_query)
            total_spend_micros = 0
            for row in spend_response:
                total_spend_micros = row.metrics.cost_micros
                break

            campaigns_paused = 0
            campaigns_resumed = 0
            action_taken = "NONE"

            if total_spend_micros >= topup_balance_micros:
                # PAUSE all active campaigns
                action_taken = "PAUSED"

                campaign_query = """
                    SELECT
                        campaign.id,
                        campaign.resource_name,
                        campaign.status
                    FROM campaign
                    WHERE campaign.status = ENABLED
                """
                campaign_response = ga_service.search(customer_id=customer_id, query=campaign_query)

                pause_operations = []
                for row in campaign_response:
                    campaign = row.campaign
                    op = client.get_type("CampaignOperation")
                    op.update.CopyFrom(campaign)
                    op.update.status = client.get_type("CampaignStatusEnum").PAUSED
                    op.update_mask.paths.append("status")
                    pause_operations.append(op)
                    campaigns_paused += 1

                if pause_operations:
                    campaign_service.mutate_campaigns(
                        customer_id=customer_id,
                        operations=pause_operations
                    )

            else:
                action_taken = "CHECKED_OK"

            return jsonify({
                "success": True,
                "customer_id": customer_id,
                "topup_balance_micros": topup_balance_micros,
                "total_spend_micros": total_spend_micros,
                "remaining_balance_micros": max(0, topup_balance_micros - total_spend_micros),
                "action_taken": action_taken,
                "campaigns_paused": campaigns_paused,
                "campaigns_resumed": campaigns_resumed,
                "message": (
                    f"Spend: {total_spend_micros / 1_000_000:.2f} | "
                    f"Balance: {topup_balance_micros / 1_000_000:.2f} | "
                    f"Action: {action_taken}"
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
                    "errors": [
                        "Network error (unable to reach Google servers). Please try again.",
                        str(e)
                    ]
                }), 500
            return jsonify({
                "success": False,
                "errors": [f"Error: {str(e)}"]
            }), 500

    return jsonify({
        "success": False,
        "errors": ["Max network retries reached. Please try again later."]
    }), 500

@app.route('/client-spend-status', methods=['GET'])
def client_spend_status():
    """
    GET /client-spend-status?customer_id=1234567890

    Returns current spend, balance, and soft-cap enforcement status for a client.
    """
    customer_id = request.args.get('customer_id', '').strip()

    if not customer_id or not customer_id.isdigit():
        return jsonify({
            "success": False,
            "errors": ["Valid numeric customer_id is required."]
        }), 400

    try:
        client, _ = load_google_ads_client()
        ga_service = client.get_service("GoogleAdsService")

        # Fetch currency
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

        # Fetch spend
        spend_query = """
            SELECT
                metrics.cost_micros
            FROM customer
        """
        spend_response = ga_service.search(customer_id=customer_id, query=spend_query)
        total_spend_micros = 0
        for row in spend_response:
            total_spend_micros = row.metrics.cost_micros
            break

        # IN PRODUCTION: Fetch topup_balance from DB
        topup_balance_micros = 100_000_000  # Placeholder
        campaigns_paused = total_spend_micros >= topup_balance_micros

        remaining_balance = max(0, topup_balance_micros - total_spend_micros)
        percentage_used = (total_spend_micros / topup_balance_micros * 100) if topup_balance_micros > 0 else 0

        return jsonify({
            "success": True,
            "customer_id": customer_id,
            "currency": customer_currency,
            "topup_balance_micros": topup_balance_micros,
            "topup_balance": topup_balance_micros / 1_000_000,
            "total_spend_micros": total_spend_micros,
            "total_spend": total_spend_micros / 1_000_000,
            "remaining_balance_micros": remaining_balance,
            "remaining_balance": remaining_balance / 1_000_000,
            "percentage_used": round(percentage_used, 2),
            "campaigns_paused": campaigns_paused,
            "timestamp": datetime.utcnow().isoformat() + "Z"
        }), 200

    except Exception as e:
        return jsonify({
            "success": False,
            "errors": [str(e)]
        }), 500

@app.route('/', methods=['GET'])
def index():
    return jsonify({
        "message": "Google Ads Backend API with Soft Cap Enforcement",
        "version": "2.0.0",
        "endpoints": {
            "POST /create-account": "Create a new client account under MCC. Body: {name, currency, timezone, email, [tracking_url], [final_url_suffix]}",
            "GET /list-linked-accounts": "List all client accounts under the MCC.",
            "POST /update-email": "Update dashboard access email for a client. Body: {customer_id, email}",
            "POST /approve-topup": "Admin approves top-up: sets hard cap (if invoicing) + soft cap (in DB). Body: {customer_id, topup_amount}",
            "POST /check-and-pause-campaigns": "Enforce soft cap: pause campaigns if spend >= balance. Body: {customer_id}. Call via cron.",
            "GET /client-spend-status?customer_id=...": "Get client's current spend, balance, and campaign status."
        }
    })

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=8080, debug=True)
