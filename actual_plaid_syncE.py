import os
import uuid
import json
import logging
import threading
import webbrowser
from datetime import datetime, date
import datetime as dt
import decimal

import tkinter as tk
from tkinter import ttk, messagebox
from tkinter.scrolledtext import ScrolledText

import requests  # Only used for optional checks or debugging
from dotenv import load_dotenv

from plaid import Configuration, Environment, ApiClient
from plaid.api import plaid_api
from plaid.model.transactions_sync_request import TransactionsSyncRequest
from plaid.exceptions import ApiException

# Import updated enums/models for v29.1.0
from plaid.model.link_token_create_request import LinkTokenCreateRequest
from plaid.model.link_token_create_request_user import LinkTokenCreateRequestUser
from plaid.model.products import Products
from plaid.model.country_code import CountryCode

# Flask for handling Plaid Link callback
from flask import Flask, request, jsonify, render_template_string

# actualpy for Actual Budget (v25.3.1)
from actual import Actual
from actual.queries import create_account, create_transaction, get_account

# ------------------------------------------------------------------------------
# 1. Load environment variables from .env file so they can populate the GUI
# ------------------------------------------------------------------------------
load_dotenv()

# -- Plaid credentials from environment --
PLAID_CLIENT_ID = os.getenv("PLAID_CLIENT_ID", "")
PLAID_SECRET = os.getenv("PLAID_SECRET", "")
PLAID_ACCESS_TOKEN = os.getenv("PLAID_ACCESS_TOKEN", "")
PLAID_ENV = os.getenv("PLAID_ENV", "sandbox")  # sandbox, development, production
PLAID_ACCOUNT_ID = os.getenv("PLAID_ACCOUNT_ID", "")  # New: required for transfers

# -- Actual credentials --
ACTUAL_PASSWORD = os.getenv("ACTUAL_PASSWORD", "")
ACTUAL_BUDGET_NAME = os.getenv("ACTUAL_BUDGET_NAME", "")
ACTUAL_ACCOUNT_NAME = os.getenv("ACTUAL_ACCOUNT_NAME", "")
ACTUAL_SERVER_URL = os.getenv("ACTUAL_SERVER_URL", "http://localhost:5006")

# Global variables for Plaid Link flow
global_access_token = None  # updated after exchanging public token
global_link_token = None    # generated for Plaid Link
flask_thread = None         # thread running our Flask app

# ------------------------------------------------------------------------------
# 2. Set up logging to both a file (sync.log) and the Tkinter GUI
# ------------------------------------------------------------------------------
logger = logging.getLogger("ActualPlaidSync")
logger.setLevel(logging.INFO)
formatter = logging.Formatter("%(asctime)s [%(levelname)s] %(message)s")

file_handler = logging.FileHandler("sync.log")
file_handler.setLevel(logging.INFO)
file_handler.setFormatter(formatter)
logger.addHandler(file_handler)

console_handler = logging.StreamHandler()
console_handler.setLevel(logging.INFO)
console_handler.setFormatter(formatter)
logger.addHandler(console_handler)

def append_log(message: str):
    """Append a log message to the ScrolledText widget."""
    log_text.config(state="normal")
    log_text.insert(tk.END, message + "\n")
    log_text.config(state="disabled")
    log_text.yview(tk.END)

class TextHandler(logging.Handler):
    """A logging handler that outputs logs to the Tkinter ScrolledText widget."""
    def emit(self, record):
        msg = self.format(record)
        append_log(msg)

text_handler = TextHandler()
text_handler.setLevel(logging.INFO)
text_handler.setFormatter(formatter)
logger.addHandler(text_handler)

# For transfer functions, we reuse logger.info via this helper.
def add_log_entry(message: str):
    logger.info(message)

# ------------------------------------------------------------------------------
# 3. Set up main Tkinter GUI
# ------------------------------------------------------------------------------
root = tk.Tk()
root.title("Actual Budget – Plaid Sync (actualpy)")

# Frames for configuration, Actual settings, controls, link, transfer, and log
config_frame = ttk.LabelFrame(root, text="Plaid API Credentials")
config_frame.pack(fill="x", padx=5, pady=5)

actual_frame = ttk.LabelFrame(root, text="Actual Budget Settings")
actual_frame.pack(fill="x", padx=5, pady=5)

control_frame = ttk.Frame(root)
control_frame.pack(fill="x", padx=5, pady=5)

link_frame = ttk.Frame(root)
link_frame.pack(fill="x", padx=5, pady=5)

transfer_frame = ttk.LabelFrame(root, text="Plaid Transfer Manager")
transfer_frame.pack(fill="x", padx=5, pady=5)

log_frame = ttk.LabelFrame(root, text="Log")
log_frame.pack(fill="both", expand=True, padx=5, pady=5)

# Plaid config fields
ttk.Label(config_frame, text="Client ID:").grid(row=0, column=0, sticky="e", padx=5, pady=2)
client_id_var = tk.StringVar(value=PLAID_CLIENT_ID)
ttk.Entry(config_frame, textvariable=client_id_var, width=40).grid(row=0, column=1, padx=5, pady=2)

ttk.Label(config_frame, text="Secret:").grid(row=1, column=0, sticky="e", padx=5, pady=2)
secret_var = tk.StringVar(value=PLAID_SECRET)
ttk.Entry(config_frame, textvariable=secret_var, width=40, show="*").grid(row=1, column=1, padx=5, pady=2)

ttk.Label(config_frame, text="Access Token:").grid(row=2, column=0, sticky="e", padx=5, pady=2)
token_var = tk.StringVar(value=PLAID_ACCESS_TOKEN)
ttk.Entry(config_frame, textvariable=token_var, width=40, show="*").grid(row=2, column=1, padx=5, pady=2)

ttk.Label(config_frame, text="Environment:").grid(row=3, column=0, sticky="e", padx=5, pady=2)
env_var = tk.StringVar(value=PLAID_ENV)
env_combo = ttk.Combobox(
    config_frame,
    textvariable=env_var,
    values=["sandbox", "development", "production"],
    state="readonly",
    width=37
)
env_combo.grid(row=3, column=1, padx=5, pady=2)

ttk.Label(config_frame, text="Account ID:").grid(row=4, column=0, sticky="e", padx=5, pady=2)
account_id_var = tk.StringVar(value=PLAID_ACCOUNT_ID)
ttk.Entry(config_frame, textvariable=account_id_var, width=40).grid(row=4, column=1, padx=5, pady=2)

# Actual config fields
ttk.Label(actual_frame, text="Actual Server URL:").grid(row=0, column=0, sticky="e", padx=5, pady=2)
actual_url_var = tk.StringVar(value=ACTUAL_SERVER_URL)
ttk.Entry(actual_frame, textvariable=actual_url_var, width=40).grid(row=0, column=1, padx=5, pady=2)

ttk.Label(actual_frame, text="Actual Password:").grid(row=1, column=0, sticky="e", padx=5, pady=2)
actual_pass_var = tk.StringVar(value=ACTUAL_PASSWORD)
ttk.Entry(actual_frame, textvariable=actual_pass_var, width=40, show="*").grid(row=1, column=1, padx=5, pady=2)

ttk.Label(actual_frame, text="Budget File Name/ID:").grid(row=2, column=0, sticky="e", padx=5, pady=2)
budget_var = tk.StringVar(value=ACTUAL_BUDGET_NAME)
ttk.Entry(actual_frame, textvariable=budget_var, width=40).grid(row=2, column=1, padx=5, pady=2)

ttk.Label(actual_frame, text="Account Name:").grid(row=3, column=0, sticky="e", padx=5, pady=2)
account_var = tk.StringVar(value=ACTUAL_ACCOUNT_NAME)
ttk.Entry(actual_frame, textvariable=account_var, width=40).grid(row=3, column=1, padx=5, pady=2)

ttk.Label(actual_frame, text="Sync Frequency (hours):").grid(row=4, column=0, sticky="e", padx=5, pady=2)
interval_var = tk.IntVar(value=24)
ttk.Spinbox(actual_frame, from_=1, to=168, textvariable=interval_var, width=5).grid(row=4, column=1, sticky="w", padx=5, pady=2)

# Transfer Manager fields
ttk.Label(transfer_frame, text="Transfer Amount (USD):").grid(row=0, column=0, sticky="e", padx=5, pady=2)
transfer_amount_var = tk.StringVar()
transfer_amount_entry = ttk.Entry(transfer_frame, textvariable=transfer_amount_var, width=20)
transfer_amount_entry.grid(row=0, column=1, padx=5, pady=2)

ttk.Label(transfer_frame, text="Recipient:").grid(row=1, column=0, sticky="e", padx=5, pady=2)
transfer_recipient_var = tk.StringVar()
transfer_recipient_entry = ttk.Entry(transfer_frame, textvariable=transfer_recipient_var, width=20)
transfer_recipient_entry.grid(row=1, column=1, padx=5, pady=2)

ttk.Label(transfer_frame, text="Reason:").grid(row=2, column=0, sticky="e", padx=5, pady=2)
transfer_reason_var = tk.StringVar()
transfer_reason_entry = ttk.Entry(transfer_frame, textvariable=transfer_reason_var, width=20)
transfer_reason_entry.grid(row=2, column=1, padx=5, pady=2)

request_transfer_btn = ttk.Button(transfer_frame, text="Request Transfer", command=lambda: request_transfer())
request_transfer_btn.grid(row=3, column=0, columnspan=2, pady=5)

# Control buttons
sync_now_btn = ttk.Button(control_frame, text="Sync Now")
start_btn = ttk.Button(control_frame, text="Start Sync")
launch_link_btn = ttk.Button(link_frame, text="Launch Plaid Link")

sync_now_btn.pack(side="left", padx=5, pady=5)
start_btn.pack(side="left", padx=5, pady=5)
launch_link_btn.pack(side="left", padx=5, pady=5)

log_text = ScrolledText(log_frame, height=15, state="disabled", font=("Courier", 9))
log_text.pack(fill="both", expand=True, padx=5, pady=5)

# ------------------------------------------------------------------------------
# 4. Plaid environment configuration and Link workflow
# ------------------------------------------------------------------------------
def get_plaid_configuration():
    client_id = client_id_var.get().strip()
    secret = secret_var.get().strip()
    env_selected = env_var.get().strip().lower()
    
    if env_selected == "sandbox":
        host = Environment.Sandbox
    elif env_selected == "development":
        logger.warning("The 'development' environment is deprecated. Using Production instead.")
        host = Environment.Production
    elif env_selected == "production":
        host = Environment.Production
    else:
        logger.warning(f"Unrecognized environment '{env_selected}'. Defaulting to Sandbox.")
        host = Environment.Sandbox

    configuration = Configuration(
        host=host,
        api_key={"clientId": client_id, "secret": secret}
    )
    return configuration

def get_plaid_base_url():
    env_selected = env_var.get().strip().lower()
    if env_selected == "production":
        return "https://production.plaid.com"
    else:
        return "https://sandbox.plaid.com"

def create_link_token(plaid_client):
    """Create a Plaid link token with transaction product access."""
    request_obj = LinkTokenCreateRequest(
        products=[Products("transactions")],
        client_name="Actual Plaid Bridge",
        country_codes=[CountryCode("US")],
        language="en",
        user=LinkTokenCreateRequestUser(client_user_id="unique_user_123")
    )
    response = plaid_client.link_token_create(request_obj)
    resp_dict = response.to_dict()
    return resp_dict.get("link_token")

# ------------------------------------------------------------------------------
# 5. Flask server for Plaid Link callback
# ------------------------------------------------------------------------------
flask_app = Flask(__name__)

PLAID_LINK_HTML = """
<!DOCTYPE html>
<html>
<head>
  <meta charset="utf-8">
  <title>Plaid Link</title>
  <script src="https://cdn.plaid.com/link/v2/stable/link-initialize.js"></script>
</head>
<body>
  <button id="link-button">Launch Plaid Link</button>
  <script>
    var linkHandler = Plaid.create({
      token: "{{ link_token }}",
      onSuccess: function(public_token, metadata) {
         fetch('/callback', {
           method: 'POST',
           headers: {'Content-Type': 'application/json'},
           body: JSON.stringify({public_token: public_token})
         }).then(response => response.json())
         .then(data => {
             document.body.innerHTML = "<p>Link success. You can now close this window.</p>";
         });
      },
      onExit: function(err, metadata) {
         if (err != null) {
           console.error(err);
         }
      }
    });
    document.getElementById('link-button').addEventListener('click', function() {
      linkHandler.open();
    });
  </script>
</body>
</html>
"""

@flask_app.route("/link", methods=["GET"])
def link():
    global global_link_token
    if not global_link_token:
        return "Error: Link token not set.", 400
    return render_template_string(PLAID_LINK_HTML, link_token=global_link_token)

@flask_app.route("/callback", methods=["POST"])
def callback():
    from plaid.model.item_public_token_exchange_request import ItemPublicTokenExchangeRequest
    data = request.get_json()
    public_token = data.get("public_token")
    if not public_token:
        return jsonify({"error": "Missing public token"}), 400
    try:
        configuration = get_plaid_configuration()
        api_client = ApiClient(configuration)
        plaid_client = plaid_api.PlaidApi(api_client)
        exchange_req = ItemPublicTokenExchangeRequest(public_token=public_token)
        exchange_response = plaid_client.item_public_token_exchange(exchange_req)
        ex_data = exchange_response.to_dict()
        access_token = ex_data.get("access_token")
        global global_access_token
        global_access_token = access_token
        token_var.set(access_token)
        logger.info("Plaid Link successful. Access token updated.")
        return jsonify({"status": "success"})
    except ApiException as e:
        logger.error(f"Error exchanging public token: {e}")
        return jsonify({"error": "Exchange failed"}), 500

def start_flask_server():
    flask_app.run(port=5001, threaded=True, use_reloader=False)

def launch_plaid_link():
    """Initiate Plaid Link by creating a link token and opening the local Flask server page."""
    global global_link_token, flask_thread
    configuration = get_plaid_configuration()
    api_client = ApiClient(configuration)
    plaid_client = plaid_api.PlaidApi(api_client)
    try:
        global_link_token = create_link_token(plaid_client)
        logger.info("Link token created successfully.")
    except ApiException as e:
        logger.error(f"Error creating link token: {e}")
        return
    if not flask_thread or not flask_thread.is_alive():
        flask_thread = threading.Thread(target=start_flask_server, daemon=True)
        flask_thread.start()
        logger.info("Flask server started on port 5001.")
    webbrowser.open("http://localhost:5001/link")

# ------------------------------------------------------------------------------
# 6. Actual import logic using actualpy
# ------------------------------------------------------------------------------
def import_to_actual(transactions):
    """
    Import transactions into Actual using actualpy.
    Each transaction should be a dict with at least:
      - date (string ISO-8601 or python date/datetime)
      - amount (float, from Plaid's transactions_sync)
      - name/payee
    This function flips the sign for outflows vs. inflows.
    """
    actual_url = actual_url_var.get().strip()
    actual_pass = actual_pass_var.get().strip()
    budget_name = budget_var.get().strip()
    account_name = account_var.get().strip()

    if not actual_url or not actual_pass or not budget_name or not account_name:
        raise Exception("Actual settings (URL, password, budget file, account) are incomplete.")

    logger.info("Importing %d transactions into Actual budget '%s'...", len(transactions), budget_name)
    with Actual(base_url=actual_url, password=actual_pass, file=budget_name) as act:
        session = act.session
        acct = get_account(session, account_name)
        if acct is None:
            logger.info(f"Account '{account_name}' not found; creating a new account in Actual.")
            acct = create_account(session, account_name)

        imported_count = 0
        for tr in transactions:
            date_val = tr.get("date")
            if isinstance(date_val, (dt.date, dt.datetime)):
                txn_date = date_val.date() if isinstance(date_val, dt.datetime) else date_val
            else:
                txn_date = dt.datetime.strptime(date_val, "%Y-%m-%d").date()
            amt = decimal.Decimal(str(tr.get("amount", 0.0)))
            actual_amount = amt.copy_negate()
            payee = tr.get("merchant_name") or tr.get("name") or "Unknown Payee"
            notes = tr.get("pending_transaction_id") or ""
            create_transaction(
                session,
                txn_date,
                acct,
                payee,
                notes=notes,
                amount=actual_amount
            )
            imported_count += 1

        act.commit()
    logger.info("Imported %d transactions into Actual successfully.", imported_count)

# ------------------------------------------------------------------------------
# 7. Sync process using Plaid's transactions_sync
# ------------------------------------------------------------------------------
def sync_transactions():
    """Fetch transactions from Plaid via transactions_sync, then import to Actual using actualpy."""
    token = token_var.get().strip()
    if not token:
        logger.error("No Plaid access token is set. Link an account or provide one in the Access Token field.")
        return

    logger.info("Starting sync cycle...")
    configuration = get_plaid_configuration()
    api_client = ApiClient(configuration)
    plaid_client = plaid_api.PlaidApi(api_client)

    state_file = "sync_state.json"
    cursor = None
    if os.path.exists(state_file):
        with open(state_file, "r") as f:
            state_data = json.load(f)
            cursor = state_data.get("last_cursor")

    all_added = []
    all_modified = []
    all_removed = []
    new_cursor = cursor
    has_more = True

    try:
        while has_more:
            if new_cursor:
                request_obj = TransactionsSyncRequest(
                    access_token=token,
                    cursor=new_cursor
                )
            else:
                request_obj = TransactionsSyncRequest(
                    access_token=token
                )
            logger.debug(f"Fetching transactions with cursor: {new_cursor}")
            response = plaid_client.transactions_sync(request_obj)
            res = response.to_dict()

            added = res.get("added", [])
            modified = res.get("modified", [])
            removed = res.get("removed", [])
            has_more = res.get("has_more", False)
            new_cursor = res.get("next_cursor")

            all_added.extend(added)
            all_modified.extend(modified)
            all_removed.extend(removed)

            logger.info(f"Fetched {len(added)} new, {len(modified)} modified, {len(removed)} removed (has_more={has_more})")

        logger.info(f"Fetched total {len(all_added)} new and {len(all_modified)} modified transactions from Plaid.")

        all_txns = all_added + all_modified
        formatted_txns = []
        for tr in all_txns:
            date_val = tr.get("date", "")
            if isinstance(date_val, (dt.date, dt.datetime)):
                date_str = date_val.isoformat()
            else:
                date_str = date_val
            amt = float(tr.get("amount", 0.0))
            payee_name = tr.get("merchant_name") or tr.get("name") or "Unknown"
            pending_id = tr.get("pending_transaction_id") or ""
            formatted_txns.append({
                "date": date_str,
                "amount": amt,
                "merchant_name": payee_name,
                "name": payee_name,
                "pending_transaction_id": pending_id,
            })

        if not formatted_txns:
            logger.info("No transactions to import or update in Actual.")
        else:
            import_to_actual(formatted_txns)

        with open(state_file, "w") as f:
            json.dump({"last_cursor": new_cursor}, f)

        logger.info("Sync cycle finished.")
    except Exception as e:
        logger.error(f"Error during sync: {e}", exc_info=True)

    interval_ms = interval_var.get() * 3600 * 1000
    root.after(interval_ms, sync_transactions)

# ------------------------------------------------------------------------------
# 8. Transfer Manager Functions
# ------------------------------------------------------------------------------
def request_transfer():
    """Initiate a transfer request by opening a manual approval dialog using transfer fields."""
    amount = transfer_amount_var.get().strip()
    recipient = transfer_recipient_var.get().strip()
    reason = transfer_reason_var.get().strip()
    if not amount or not recipient or not reason:
        messagebox.showerror("Input Error", "Please enter transfer amount, recipient, and reason.")
        return
    try:
        amt_val = float(amount)
        if amt_val <= 0:
            raise ValueError("Amount must be positive.")
    except Exception:
        messagebox.showerror("Input Error", "Please enter a valid positive number for transfer amount.")
        return

    approval_win = tk.Toplevel(root)
    approval_win.title("Approve Transfer")
    tk.Label(approval_win, text="Review Transfer Details", font=("Arial", 12, "bold")).pack(pady=10)
    details = f"Amount: ${amount}\nRecipient: {recipient}\nReason: {reason}"
    tk.Label(approval_win, text=details, justify="left").pack(padx=10, pady=5)
    btn_frame = tk.Frame(approval_win)
    btn_frame.pack(pady=10)
    def on_approve():
        approval_win.destroy()
        threading.Thread(target=proceed_transfer, args=(amount, recipient, reason), daemon=True).start()
    def on_reject():
        approval_win.destroy()
        add_log_entry(f"Transfer to '{recipient}' for ${amount} was rejected by user.")
        messagebox.showinfo("Transfer Cancelled", "The transfer request was cancelled.")
    tk.Button(btn_frame, text="Approve", command=on_approve, width=10).pack(side="left", padx=10)
    tk.Button(btn_frame, text="Reject", command=on_reject, width=10).pack(side="right", padx=10)

def proceed_transfer(amount: str, recipient: str, reason: str):
    """Execute the transfer via Plaid API calls after user approval."""
    if len(reason) > 15:
        reason = reason[:15]
    add_log_entry(f"Initiating transfer of ${amount} to '{recipient}' (Reason: {reason})")
    
    account_id = account_id_var.get().strip()
    if not account_id:
        add_log_entry("Error: Account ID is not set.")
        messagebox.showerror("Configuration Error", "Account ID is missing. Please set it in the configuration.")
        return

    base_url = get_plaid_base_url()
    auth_url = f"{base_url}/transfer/authorization/create"
    auth_request = {
        "client_id": client_id_var.get().strip(),
        "secret": secret_var.get().strip(),
        "access_token": token_var.get().strip(),
        "account_id": account_id,
        "type": "debit",
        "network": "ach",
        "amount": amount,
        "ach_class": "ppd",
        "user": {
            "legal_name": USER_NAME,
            "email_address": USER_EMAIL
        }
    }
    try:
        response = requests.post(auth_url, json=auth_request)
        auth_data = response.json()
    except Exception as e:
        add_log_entry("Error: Failed to authorize transfer (network error).")
        messagebox.showerror("Transfer Error", f"Transfer authorization failed: {e}")
        return

    if response.status_code != 200 or auth_data.get('error') or auth_data.get('error_code'):
        err_msg = auth_data.get('error_message', 'Unknown error')
        add_log_entry(f"Error: Transfer authorization failed - {err_msg}")
        messagebox.showerror("Transfer Error", f"Authorization failed: {err_msg}")
        return

    auth_decision = auth_data['transfer_authorization'].get('decision')
    if auth_decision != "approved":
        if auth_decision == "declined":
            reason_code = auth_data['transfer_authorization'].get('decision_rationale', {}).get('code')
            add_log_entry(f"Transfer not authorized (declined). Reason code: {reason_code}")
            messagebox.showwarning("Transfer Declined", "Plaid declined the transfer (risk checks failed).")
        elif auth_decision == "user_action_required":
            add_log_entry("Transfer not authorized: user action required to fix account issues.")
            messagebox.showwarning("Action Required", "Transfer cannot proceed until account link is updated.")
        else:
            add_log_entry(f"Transfer authorization returned '{auth_decision}', cannot proceed.")
            messagebox.showinfo("Transfer Not Authorized", f"Transfer not authorized (decision: {auth_decision}).")
        return

    auth_id = auth_data['transfer_authorization']['id']
    transfer_url = f"{base_url}/transfer/create"
    transfer_request = {
        "client_id": client_id_var.get().strip(),
        "secret": secret_var.get().strip(),
        "idempotency_key": str(uuid.uuid4()),
        "access_token": token_var.get().strip(),
        "account_id": account_id,
        "authorization_id": auth_id,
        "type": "debit",
        "network": "ach",
        "amount": amount,
        "description": reason,
        "ach_class": "ppd",
        "user": {
            "legal_name": USER_NAME,
            "email_address": USER_EMAIL
        }
    }
    try:
        response = requests.post(transfer_url, json=transfer_request)
        transfer_data = response.json()
    except Exception as e:
        add_log_entry("Error: Failed to create transfer (network error).")
        messagebox.showerror("Transfer Error", f"Transfer creation failed: {e}")
        return

    if response.status_code != 200 or transfer_data.get('error') or transfer_data.get('error_code'):
        err_msg = transfer_data.get('error_message', 'Unknown error')
        add_log_entry(f"Error: Transfer creation failed - {err_msg}")
        messagebox.showerror("Transfer Error", f"Transfer failed: {err_msg}")
        return

    transfer_id = transfer_data['transfer']['id']
    transfer_status = transfer_data['transfer'].get('status', 'pending')
    add_log_entry(f"Transfer {transfer_id} created successfully. Status: {transfer_status}")
    messagebox.showinfo("Transfer Created", f"Transfer has been created (Status: {transfer_status}).")

def poll_transfer_events():
    """Poll Plaid for new transfer events and update the log."""
    base_url = get_plaid_base_url()
    payload = {
        "client_id": client_id_var.get().strip(),
        "secret": secret_var.get().strip(),
        "count": 25
    }
    if poll_transfer_events.last_event_id is not None:
        payload["after_id"] = poll_transfer_events.last_event_id
    try:
        resp = requests.post(f"{base_url}/transfer/event/sync", json=payload)
        data = resp.json()
    except Exception:
        root.after(10000, poll_transfer_events)
        return
    if resp.status_code == 200 and data.get("transfer_events"):
        events = data["transfer_events"]
        if events:
            poll_transfer_events.last_event_id = events[-1]["event_id"]
            for event in events:
                status = event.get("event_type", "")
                transfer_id = event.get("transfer_id", "")
                status_name = status.split(".")[-1] if "." in status else status
                add_log_entry(f"Transfer {transfer_id} status updated to {status_name}")
                if status_name.lower() in ("posted", "settled", "completed", "failed", "returned"):
                    messagebox.showinfo("Transfer Update", f"Transfer {transfer_id} status: {status_name}")
    root.after(10000, poll_transfer_events)

poll_transfer_events.last_event_id = None

# ------------------------------------------------------------------------------
# 8. Handlers for GUI buttons
# ------------------------------------------------------------------------------
def on_start():
    for child in config_frame.winfo_children() + actual_frame.winfo_children():
        if isinstance(child, (tk.Entry, ttk.Combobox, tk.Spinbox)):
            child.config(state="disabled")
    start_btn.config(state="disabled")
    sync_now_btn.config(state="disabled")
    launch_link_btn.config(state="disabled")
    request_transfer_btn.config(state="disabled")
    # Start sync and transfer polling
    sync_transactions()
    root.after(10000, poll_transfer_events)

def on_sync_now():
    logger.info("Manual sync initiated.")
    root.after(100, sync_transactions)

launch_link_btn.config(command=launch_plaid_link)
start_btn.config(command=on_start)
sync_now_btn.config(command=on_sync_now)

# ------------------------------------------------------------------------------
# 9. Start the Tkinter main loop
# ------------------------------------------------------------------------------
root.mainloop()

