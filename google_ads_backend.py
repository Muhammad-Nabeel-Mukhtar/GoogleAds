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
            "POST /create-account": "Create new client account (no auto-billing). Body: {name, currency, timezone, email, [tracking_url], [final_url_suffix]}",
            "GET /list-linked-accounts": "List all client accounts under MCC",
            "POST /assign-billing-setup": "Assign billing setup to existing account. Body: {customer_id}",
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


# Add this debug endpoint temporarily
@app.route('/debug-mcc', methods=['GET'])
def debug_mcc():
    try:
        client, mcc_customer_id = load_google_ads_client()
        ga_service = client.get_service("GoogleAdsService")
        
        billing_query = "SELECT billing_setup.id, billing_setup.resource_name, billing_setup.status FROM billing_setup"
        response = ga_service.search(customer_id=mcc_customer_id, query=billing_query)
        
        setups = []
        for row in response:
            setups.append({
                "id": row.billing_setup.id,
                "resource": row.billing_setup.resource_name,
                "status": row.billing_setup.status.name
            })
        
        return jsonify({
            "mcc_customer_id": mcc_customer_id,
            "billing_setups": setups
        })
    except Exception as e:
        return jsonify({"error": str(e)})


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
    
    Assigns MCC's billing setup to an existing client account via Google Ads API.
    Uses BillingSetupService to create a billing setup link for the client.
    Billing setup ID is read from environment variable BILLING_SETUP_ID.
    
    Expected JSON:
    {
        "customer_id": "1234567890"
    }
    """
    data = request.json or {}
    customer_id = str(data.get('customer_id', '')).strip()

    if not customer_id or not customer_id.isdigit():
        return jsonify({
            "success": False,
            "errors": ["Valid numeric customer_id is required."]
        }), 400

    # Get billing setup ID from environment
    billing_setup_id = os.getenv('BILLING_SETUP_ID')
    if not billing_setup_id:
        return jsonify({
            "success": False,
            "errors": ["BILLING_SETUP_ID not configured in environment variables."]
        }), 500

    for attempt in range(3):
        try:
            client, mcc_customer_id = load_google_ads_client()
            billing_setup_service = client.get_service("BillingSetupService")

            print(f"[DEBUG] Using billing setup ID from environment: {billing_setup_id}")
            print(f"[DEBUG] MCC customer ID: {mcc_customer_id}")

            # Step 1: Check if customer already has a billing setup
            ga_service = client.get_service("GoogleAdsService")
            try:
                customer_billing_query = """
                    SELECT
                        billing_setup.id,
                        billing_setup.resource_name,
                        billing_setup.status
                    FROM billing_setup
                """
                customer_billing_response = ga_service.search(customer_id=customer_id, query=customer_billing_query)
                
                for row in customer_billing_response:
                    existing_bs_id = row.billing_setup.id
                    print(f"[DEBUG] Customer already has billing setup ID: {existing_bs_id}")
                    return jsonify({
                        "success": False,
                        "errors": [f"Customer {customer_id} already has a billing setup (ID: {existing_bs_id}). Cannot assign another."]
                    }), 400
            except Exception as e:
                # Expected for new accounts - they won't have any billing setup
                print(f"[DEBUG] Customer {customer_id} has no existing billing setup (expected for new accounts)")

            # Step 2: Build the MCC's billing setup resource name
            mcc_billing_setup_resource = f"customers/{mcc_customer_id}/billingSetups/{billing_setup_id}"
            print(f"[DEBUG] Linking billing setup: {mcc_billing_setup_resource}")

            # Step 3: Create billing setup operation
            operation = client.get_type("BillingSetupOperation")
            create_op = operation.create
            
            # Link the MCC's billing setup to this customer
            create_op.billing_setup = mcc_billing_setup_resource
            
            print(f"[DEBUG] Creating billing setup link for customer {customer_id}...")

            # Step 4: Execute the mutation
            response = billing_setup_service.mutate_billing_setup(
                customer_id=customer_id,
                operation=operation
            )

            if response.result and response.result.resource_name:
                new_billing_setup_resource = response.result.resource_name
                new_bs_id = new_billing_setup_resource.split("/")[-1]
                
                print(f"[DEBUG] SUCCESS: Billing setup created for customer {customer_id}")
                print(f"[DEBUG] New billing setup resource: {new_billing_setup_resource}")

                return jsonify({
                    "success": True,
                    "customer_id": customer_id,
                    "mcc_customer_id": mcc_customer_id,
                    "mcc_billing_setup_id": billing_setup_id,
                    "mcc_billing_setup_resource": mcc_billing_setup_resource,
                    "customer_billing_setup_resource": new_billing_setup_resource,
                    "customer_billing_setup_id": new_bs_id,
                    "message": f"Billing setup {billing_setup_id} successfully linked to customer {customer_id}.",
                    "note": "Status will be PENDING until Google approves (usually ~1 hour). Once ACTIVE, you can create account budgets.",
                    "next_step": f"Call /approve-topup with customer_id={customer_id} and topup_amount=XX to set hard spending cap",
                    "timestamp": datetime.utcnow().isoformat() + "Z"
                }), 200
            else:
                print(f"[DEBUG] Error: Response missing result")
                return jsonify({
                    "success": False,
                    "errors": ["Billing setup creation returned empty result."]
                }), 400

        except GoogleAdsException as e:
            error_code = None
            error_msg = str(e)
            
            if e.failure and e.failure.errors:
                error_code = e.failure.errors[0].error_code.name
                error_detail = e.failure.errors[0].message
            else:
                error_detail = error_msg

            print(f"[DEBUG] GoogleAdsException: Code={error_code}, Detail={error_detail}")
            
            user_msg = []

            if "BILLING_SETUP_ALREADY_EXISTS" in error_msg or error_code == "BILLING_SETUP_ALREADY_EXISTS":
                user_msg.append("Customer already has a billing setup assigned.")
            elif "BILLING_SETUP_NOT_PERMITTED_FOR_ACCOUNT_TYPE" in error_msg:
                user_msg.append("This account type does not support billing setups (e.g., test account).")
            elif "INVALID_ARGUMENT" in error_msg or error_code == "INVALID_ARGUMENT":
                user_msg.append(f"Invalid billing setup resource. Error: {error_detail}")
            elif "RESOURCE_NOT_FOUND" in error_msg or error_code == "RESOURCE_NOT_FOUND":
                user_msg.append("Billing setup resource not found. Check BILLING_SETUP_ID in environment.")
            elif "PERMISSION_DENIED" in error_msg:
                user_msg.append("Permission denied. Ensure MCC has proper access to this customer account.")
            else:
                user_msg.append(f"Google Ads API error: {error_detail}")

            return jsonify({
                "success": False,
                "errors": user_msg,
                "error_code": error_code
            }), 400

        except Exception as e:
            print(f"[DEBUG] Unexpected error: {str(e)}")
            if is_network_error(e):
                if attempt < 2:
                    print(f"[DEBUG] Network error, retrying (attempt {attempt + 1}/3)...")
                    time.sleep(5)
                    continue
                return jsonify({
                    "success": False,
                    "errors": ["Network error (unable to reach Google servers). Please try again.", str(e)]
                }), 500
            return jsonify({
                "success": False,
                "errors": [f"Unexpected error: {str(e)}"]
            }), 500

    return jsonify({
        "success": False,
        "errors": ["Max retries reached. Please try again later."]
    }), 500

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
