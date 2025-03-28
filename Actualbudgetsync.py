import os
import json
import logging
import threading
import webbrowser
import re # For parsing Plaid ID from notes
from datetime import datetime, date # Ensure date is imported
import datetime as dt
import decimal
import time # For retry delay

import tkinter as tk
from tkinter import ttk
from tkinter.scrolledtext import ScrolledText

# Removed unused requests import if only used for debugging before
# import requests
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

# actualpy for Actual Budget
from actual import Actual
# Corrected imports based on previous errors
from actual.queries import create_account, create_transaction, get_account, get_transactions

# ------------------------------------------------------------------------------
# 1. Load environment variables
# ------------------------------------------------------------------------------
load_dotenv()
PLAID_CLIENT_ID = os.getenv("PLAID_CLIENT_ID", "")
PLAID_SECRET = os.getenv("PLAID_SECRET", "")
PLAID_ACCESS_TOKEN = os.getenv("PLAID_ACCESS_TOKEN", "")
PLAID_ENV = os.getenv("PLAID_ENV", "sandbox")
ACTUAL_PASSWORD = os.getenv("ACTUAL_PASSWORD", "")
ACTUAL_BUDGET_NAME = os.getenv("ACTUAL_BUDGET_NAME", "")
ACTUAL_ACCOUNT_NAME = os.getenv("ACTUAL_ACCOUNT_NAME", "")
ACTUAL_SERVER_URL = os.getenv("ACTUAL_SERVER_URL", "http://localhost:5006")

# Global variables for Plaid Link flow
global_access_token = PLAID_ACCESS_TOKEN
global_link_token = None
flask_thread = None
sync_after_id = None

# Constants
PLAID_ID_NOTE_PREFIX = "plaid_id:"
STATE_FILE = "sync_state.json"
RETRY_DELAY_SECONDS = 10 # Delay before retrying Plaid pagination error

# ------------------------------------------------------------------------------
# 2. Set up logging
# ------------------------------------------------------------------------------
logger = logging.getLogger("ActualPlaidSync")
logger.setLevel(logging.INFO)
formatter = logging.Formatter("%(asctime)s [%(levelname)s] %(message)s")

# File Handler
try:
    log_filename = STATE_FILE.replace(".json", ".log")
    file_handler = logging.FileHandler(log_filename)
    file_handler.setLevel(logging.INFO)
    file_handler.setFormatter(formatter)
    logger.addHandler(file_handler)
except IOError as e:
    print(f"Warning: Could not open log file '{log_filename}'. Logging to console only. Error: {e}")

# Console Handler
console_handler = logging.StreamHandler()
console_handler.setLevel(logging.INFO)
console_handler.setFormatter(formatter)
logger.addHandler(console_handler)

# ------------------------------------------------------------------------------
# 3. Set up main Tkinter GUI
# ------------------------------------------------------------------------------
root = tk.Tk()
root.title("Actual Budget – Plaid Sync (actualpy) v2.7") # Version bump

# --- Frames ---
config_frame = ttk.LabelFrame(root, text="Plaid API Credentials")
config_frame.pack(fill="x", padx=5, pady=5)
actual_frame = ttk.LabelFrame(root, text="Actual Budget Settings")
actual_frame.pack(fill="x", padx=5, pady=5)
control_frame = ttk.Frame(root)
control_frame.pack(fill="x", padx=5, pady=5)
link_frame = ttk.Frame(root)
link_frame.pack(fill="x", padx=5, pady=5)
log_frame = ttk.LabelFrame(root, text="Log")
log_frame.pack(fill="both", expand=True, padx=5, pady=5)

# --- Plaid Config ---
ttk.Label(config_frame, text="Client ID:").grid(row=0, column=0, sticky="e", padx=5, pady=2)
client_id_var = tk.StringVar(value=PLAID_CLIENT_ID)
ttk.Entry(config_frame, textvariable=client_id_var, width=40).grid(row=0, column=1, padx=5, pady=2)

ttk.Label(config_frame, text="Secret:").grid(row=1, column=0, sticky="e", padx=5, pady=2)
secret_var = tk.StringVar(value=PLAID_SECRET)
ttk.Entry(config_frame, textvariable=secret_var, width=40, show="*").grid(row=1, column=1, padx=5, pady=2)

ttk.Label(config_frame, text="Access Token:").grid(row=2, column=0, sticky="e", padx=5, pady=2)
token_var = tk.StringVar(value=global_access_token)
ttk.Entry(config_frame, textvariable=token_var, width=40, show="*").grid(row=2, column=1, padx=5, pady=2)

ttk.Label(config_frame, text="Environment:").grid(row=3, column=0, sticky="e", padx=5, pady=2)
env_var = tk.StringVar(value=PLAID_ENV)
env_combo = ttk.Combobox(config_frame, textvariable=env_var, values=["sandbox", "development", "production"], state="readonly", width=37)
env_combo.grid(row=3, column=1, padx=5, pady=2)

# --- Actual Config ---
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

# --- Controls ---
sync_now_btn = ttk.Button(control_frame, text="Sync Now")
start_btn = ttk.Button(control_frame, text="Start Auto-Sync")
stop_btn = ttk.Button(control_frame, text="Stop Auto-Sync", state="disabled")
launch_link_btn = ttk.Button(link_frame, text="Launch Plaid Link to get/update Access Token")

sync_now_btn.pack(side="left", padx=5, pady=5)
start_btn.pack(side="left", padx=5, pady=5)
stop_btn.pack(side="left", padx=5, pady=5)
launch_link_btn.pack(side="left", padx=5, pady=5)

# --- Log Area ---
log_text = ScrolledText(log_frame, height=15, state="disabled", font=("Courier", 9))
log_text.pack(fill="both", expand=True, padx=5, pady=5)

def append_log(message: str):
    """Append a log message to the ScrolledText widget in a thread-safe way."""
    def _append():
        log_text.config(state="normal")
        log_text.insert(tk.END, message + "\n")
        log_text.config(state="disabled")
        log_text.yview(tk.END)
    if threading.current_thread() is threading.main_thread():
        _append()
    else:
        root.after(0, _append)

class TextHandler(logging.Handler):
    """A logging handler that outputs logs to the Tkinter ScrolledText widget."""
    def emit(self, record):
        msg = self.format(record)
        append_log(msg)

text_handler = TextHandler()
text_handler.setLevel(logging.INFO)
text_handler.setFormatter(formatter)
logger.addHandler(text_handler)

# ------------------------------------------------------------------------------
# 4. Plaid environment configuration and Link workflow
# ------------------------------------------------------------------------------
def get_plaid_configuration():
    client_id = client_id_var.get().strip()
    secret = secret_var.get().strip()
    env_selected = env_var.get().strip().lower()

    if not client_id or not secret:
         logger.error("Plaid Client ID and Secret cannot be empty.")
         raise ValueError("Plaid Client ID and Secret are required.")

    if env_selected == "sandbox":
        host = Environment.Sandbox
    elif env_selected == "development":
        logger.warning("Plaid 'development' environment is deprecated. Using 'production' host.")
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

def create_link_token(plaid_client):
    """Create a Plaid link token."""
    try:
        client_user_id = f"actual_sync_{budget_var.get().strip() or 'default'}"
        logger.info(f"Using client_user_id for Plaid Link: {client_user_id}")

        request = LinkTokenCreateRequest(
            products=[Products("transactions")],
            client_name="Actual Budget Plaid Sync Tool",
            country_codes=[CountryCode("US")],
            language="en",
            user=LinkTokenCreateRequestUser(client_user_id=client_user_id)
        )
        response = plaid_client.link_token_create(request)
        resp_dict = response.to_dict()
        return resp_dict.get("link_token")
    except ApiException as e:
        logger.error(f"Error creating Plaid link token: {e.body}")
        raise

# ------------------------------------------------------------------------------
# 5. Flask server for Plaid Link callback
# ------------------------------------------------------------------------------
flask_app = Flask(__name__)
log = logging.getLogger('werkzeug')
log.setLevel(logging.WARNING)

PLAID_LINK_HTML = """
<!DOCTYPE html><html><head><meta charset="utf-8"><title>Plaid Link</title>
<script src="https://cdn.plaid.com/link/v2/stable/link-initialize.js"></script>
</head><body><p>Click the button below to link your account via Plaid.</p>
<button id="link-button">Link Account</button><div id="status"></div>
<script>
  var handler = Plaid.create({
    token: "{{ link_token }}",
    onSuccess: function(public_token, metadata) {
      document.getElementById('status').innerText = 'Link successful, exchanging token...';
      fetch('/callback', {
        method: 'POST',
        headers: {'Content-Type': 'application/json'},
        body: JSON.stringify({public_token: public_token})
      }).then(response => response.json())
      .then(data => {
        if (data.status === 'success') {
            document.body.innerHTML = "<h1>Success!</h1><p>Access token received. You can close this window and check the application.</p>";
        } else {
            document.body.innerHTML = "<h1>Error</h1><p>Failed to exchange public token. Check application logs.</p><p>" + (data.error || '') + "</p>";
        }
      }).catch(error => {
         document.body.innerHTML = "<h1>Error</h1><p>Network error during callback. Check application logs and console.</p>";
         console.error('Callback fetch error:', error);
      });
    },
    onLoad: function() {},
    onExit: function(err, metadata) {
      if (err != null) {
        console.error('Plaid Link exit error:', err);
        document.getElementById('status').innerText = 'Plaid Link exited with error. See console.';
      } else {
        document.getElementById('status').innerText = 'Plaid Link exited.';
      }
    },
    onEvent: function(eventName, metadata) {}
  });
  document.getElementById('link-button').onclick = function() { handler.open(); };
</script></body></html>
"""

@flask_app.route("/link", methods=["GET"])
def link():
    global global_link_token
    if not global_link_token:
        logger.error("Flask /link endpoint called but global_link_token is not set.")
        return "Error: Link token not available. Try launching Plaid Link again from the application.", 400
    return render_template_string(PLAID_LINK_HTML, link_token=global_link_token)

@flask_app.route("/callback", methods=["POST"])
def callback():
    from plaid.model.item_public_token_exchange_request import ItemPublicTokenExchangeRequest
    global global_access_token
    data = request.get_json()
    public_token = data.get("public_token")

    if not public_token:
        logger.error("Plaid callback received without a public token.")
        return jsonify({"error": "Missing public_token"}), 400

    try:
        configuration = get_plaid_configuration()
        api_client = ApiClient(configuration)
        plaid_client = plaid_api.PlaidApi(api_client)

        exchange_req = ItemPublicTokenExchangeRequest(public_token=public_token)
        exchange_response = plaid_client.item_public_token_exchange(exchange_req)
        ex_data = exchange_response.to_dict()

        access_token = ex_data.get("access_token")
        item_id = ex_data.get("item_id")
        if not access_token:
            logger.error(f"Public token exchange response missing access_token. Item ID: {item_id}")
            return jsonify({"error": "Access token not received from Plaid"}), 500

        logger.info(f"Plaid Link successful. Received new access token for Item ID: {item_id}")
        global_access_token = access_token
        root.after(0, lambda: token_var.set(access_token))
        # root.after(1000, on_sync_now) # Optional: Trigger sync after link

        return jsonify({"status": "success", "item_id": item_id})

    except ApiException as e:
        logger.error(f"Plaid API error during public token exchange: {e.body}", exc_info=True)
        return jsonify({"error": f"Plaid API Error: {e.body}"}), 500
    except Exception as e:
        logger.error(f"Unexpected error during public token exchange: {e}", exc_info=True)
        return jsonify({"error": f"Server Error: {e}"}), 500

def start_flask_server():
    """Runs the Flask server in a daemon thread."""
    global flask_thread
    if not flask_thread or not flask_thread.is_alive():
        flask_thread = threading.Thread(target=lambda: flask_app.run(port=5001, host='localhost', threaded=True, use_reloader=False), daemon=True)
        flask_thread.start()
        logger.info("Flask server started on http://localhost:5001 for Plaid Link callback.")
    else:
        logger.info("Flask server already running.")

def launch_plaid_link():
    """Initiate Plaid Link: create token, start server, open browser."""
    global global_link_token
    try:
        configuration = get_plaid_configuration()
        api_client = ApiClient(configuration)
        plaid_client = plaid_api.PlaidApi(api_client)
        global_link_token = create_link_token(plaid_client)
        logger.info("Plaid Link token created successfully.")
        start_flask_server()
        webbrowser.open("http://localhost:5001/link")
        logger.info("Opened browser to Plaid Link URL.")
    except (ValueError, ApiException) as e:
        logger.error(f"Failed to launch Plaid Link: {e}")
    except Exception as e:
        logger.error(f"Unexpected error launching Plaid Link: {e}", exc_info=True)

# ------------------------------------------------------------------------------
# 6. Actual Budget Interaction Logic (Revised for Sync & Workarounds)
# ------------------------------------------------------------------------------

def parse_plaid_id_from_note(note):
    """Extracts Plaid transaction ID from the note string."""
    if not note:
        return None
    match = re.search(rf"{PLAID_ID_NOTE_PREFIX}(\S+)", note)
    return match.group(1) if match else None

def format_note_with_plaid_id(plaid_txn):
    """Creates the note string including the Plaid ID and other details."""
    plaid_id = plaid_txn.get('transaction_id')
    category = ", ".join(plaid_txn.get('category') or [])
    note_parts = []
    if plaid_id:
         note_parts.append(f"{PLAID_ID_NOTE_PREFIX}{plaid_id}")
    if category:
        note_parts.append(f"Plaid Cat: {category}")
    original_name = plaid_txn.get("name")
    merchant_name = plaid_txn.get("merchant_name")
    if merchant_name and original_name != merchant_name:
         note_parts.append(f"Orig: {original_name[:50]}")
    return " | ".join(note_parts)

# Expects account object
def get_actual_plaid_id_map(session, account):
    """
    Fetches transactions from Actual for the account and builds a map
    from Plaid ID (extracted from notes) to the Actual transaction object.
    Uses keyword arguments for dates and account object for get_transactions.
    """
    account_id = account.id # Get ID for logging
    logger.info(f"Fetching existing transactions from Actual account '{account.name}' (ID: {account_id}) to build Plaid ID map...")
    plaid_id_map = {}
    try:
        # --- Corrected call signature attempt ---
        # Explicitly provide date objects for start/end dates via keyword.
        # Pass account object via keyword 'account'.
        earliest_date = date(1970, 1, 1)
        latest_date = date(2099, 12, 31)
        logger.debug(f"Calling get_transactions for account '{account.name}' with date range: {earliest_date} to {latest_date}")
        # --------------------------------------------

        # *** Corrected Call: Use keywords for dates and account object ***
        actual_transactions = get_transactions(session,
                                             start_date=earliest_date,  # Keyword
                                             end_date=latest_date,    # Keyword
                                             account=account)         # Keyword

        count = 0
        for txn in actual_transactions:
            plaid_id = parse_plaid_id_from_note(txn.notes)
            if plaid_id:
                if plaid_id in plaid_id_map:
                    logger.warning(f"Duplicate Plaid ID '{plaid_id}' found in Actual notes for transactions "
                                   f"'{txn.id}' and '{plaid_id_map[plaid_id].id}'. Using the first found.")
                else:
                    plaid_id_map[plaid_id] = txn
            count += 1
        logger.info(f"Processed {count} Actual transactions, found {len(plaid_id_map)} with Plaid IDs in notes.")
        return plaid_id_map
    except Exception as e:
        logger.error(f"Failed during get_transactions call or processing results: {e}", exc_info=True)
        raise

def process_plaid_updates(session, account, added, modified, removed):
    """
    Processes transactions fetched from Plaid: deletes removed, updates modified, creates added.
    Uses Plaid ID stored in Actual notes for matching.
    """
    plaid_id_map = get_actual_plaid_id_map(session, account)

    # --- 1. Process Removed Transactions ---
    deleted_count = 0
    for removed_item in removed:
        plaid_id = removed_item.get('transaction_id')
        if not plaid_id:
            logger.warning("Found removed transaction item from Plaid with no transaction_id. Skipping.")
            continue
        actual_txn = plaid_id_map.pop(plaid_id, None)
        if actual_txn:
            try:
                logger.info(f"Deleting Actual transaction ID {actual_txn.id} (Plaid ID: {plaid_id}).")
                session.delete(actual_txn) # Assumes session object has a .delete method
                deleted_count += 1
            except AttributeError:
                 logger.error(f"Failed to delete Actual transaction ID {actual_txn.id} (Plaid ID: {plaid_id}): "
                              f"'session' object likely has no 'delete' method. Check actualpy documentation.", exc_info=True)
            except Exception as e:
                logger.error(f"Failed to delete Actual transaction ID {actual_txn.id} (Plaid ID: {plaid_id}): {e}", exc_info=True)
        else:
            logger.warning(f"Plaid indicated removal for transaction ID '{plaid_id}', but no matching transaction found in Actual notes.")

    # --- 2. Process Modified Transactions ---
    updated_count = 0
    for plaid_txn_dict in modified:
        plaid_id = plaid_txn_dict.get('transaction_id')
        if not plaid_id:
            logger.warning("Found modified transaction item from Plaid with no transaction_id. Skipping.")
            continue
        actual_txn = plaid_id_map.pop(plaid_id, None)
        if actual_txn:
            try:
                needs_update = False

                # --- Corrected Date Handling (Modified Loop) ---
                plaid_date_val = plaid_txn_dict.get("date") # Get the value (might be date obj or str)
                plaid_date = None # Initialize to None
                if isinstance(plaid_date_val, (dt.date, dt.datetime)):
                    plaid_date = plaid_date_val.date() if isinstance(plaid_date_val, dt.datetime) else plaid_date_val
                elif isinstance(plaid_date_val, str) and plaid_date_val:
                    try:
                        plaid_date = dt.datetime.strptime(plaid_date_val, "%Y-%m-%d").date()
                    except ValueError:
                        logger.warning(f"Plaid modified txn ID {plaid_id} had invalid date string '{plaid_date_val}'. Skipping date update.")
                else:
                     logger.warning(f"Plaid modified txn ID {plaid_id} had missing or unexpected date type '{type(plaid_date_val)}'. Skipping date update.")

                # Only proceed with comparison if we got a valid date from Plaid
                if plaid_date and actual_txn.date != plaid_date:
                    logger.info(f"Updating date for Actual Txn ID {actual_txn.id} (Plaid ID: {plaid_id}): {actual_txn.date} -> {plaid_date}")
                    actual_txn.date = plaid_date
                    needs_update = True
                # --- End Corrected Date Handling ---

                # Amount
                plaid_amt_str = str(plaid_txn_dict.get("amount", 0.0))
                if plaid_amt_str: # Check if not empty string after conversion
                    plaid_amt = decimal.Decimal(plaid_amt_str)
                    actual_expected_amount = plaid_amt.copy_negate()
                    # Use is_nan() check if necessary, but comparison should work
                    if actual_txn.amount != actual_expected_amount:
                         logger.info(f"Updating amount for Actual Txn ID {actual_txn.id} (Plaid ID: {plaid_id}): {actual_txn.amount} -> {actual_expected_amount}")
                         actual_txn.amount = actual_expected_amount
                         needs_update = True
                else:
                    logger.warning(f"Plaid modified transaction ID '{plaid_id}' has null/empty amount. Skipping amount update.")
                # Payee
                plaid_payee = plaid_txn_dict.get("merchant_name") or plaid_txn_dict.get("name") or "Unknown Payee"
                if actual_txn.payee != plaid_payee:
                    logger.info(f"Updating payee for Actual Txn ID {actual_txn.id} (Plaid ID: {plaid_id}): '{actual_txn.payee}' -> '{plaid_payee}'")
                    actual_txn.payee = plaid_payee
                    needs_update = True
                # Notes
                new_note = format_note_with_plaid_id(plaid_txn_dict)
                if actual_txn.notes != new_note:
                    logger.info(f"Updating notes for Actual Txn ID {actual_txn.id} (Plaid ID: {plaid_id})")
                    actual_txn.notes = new_note
                    needs_update = True
                # Cleared status could be added here

                if needs_update:
                    logger.info(f"Actual transaction ID {actual_txn.id} marked for update.")
                    updated_count += 1
                else:
                     logger.debug(f"Actual transaction ID {actual_txn.id} (Plaid ID: {plaid_id}) matches Plaid data. No update needed.")
            except Exception as e:
                logger.error(f"Failed to process update for Actual transaction ID {actual_txn.id} (Plaid ID: {plaid_id}): {e}", exc_info=True)
        else:
            logger.warning(f"Plaid modified transaction ID '{plaid_id}', but no matching transaction found in Actual notes. Will attempt to add it.")
            added.append(plaid_txn_dict)

    # --- 3. Process Added Transactions ---
    added_count = 0
    for plaid_txn_dict in added:
        plaid_id = plaid_txn_dict.get('transaction_id')
        if not plaid_id:
            logger.warning("Found added transaction item from Plaid with no transaction_id. Skipping.")
            continue
        if plaid_id in plaid_id_map:
            logger.warning(f"Plaid added transaction ID '{plaid_id}', but it already exists in Actual (Actual ID: {plaid_id_map[plaid_id].id}). Skipping add.")
            continue
        try:
            # --- Corrected Date Handling (Added Loop) ---
            date_val = plaid_txn_dict.get("date") # Get the value
            txn_date = None # Initialize
            if isinstance(date_val, (dt.date, dt.datetime)):
                txn_date = date_val.date() if isinstance(date_val, dt.datetime) else date_val
            elif isinstance(date_val, str) and date_val: # Check if it's a non-empty string
                 try:
                     txn_date = dt.datetime.strptime(date_val, "%Y-%m-%d").date()
                 except ValueError:
                      logger.error(f"Plaid added txn ID {plaid_id} had invalid date string '{date_val}'. Using today's date.")
                      txn_date = date.today() # Fallback
            else:
                 logger.warning(f"Plaid added txn ID {plaid_id} had missing or unexpected date type '{type(date_val)}'. Using today's date.")
                 txn_date = date.today() # Fallback if missing or wrong type
            # --- End Corrected Date Handling ---

            # Amount
            amount_str = str(plaid_txn_dict.get("amount", "0.0")) # Default to "0.0" string
            if not amount_str:
                 logger.warning(f"Plaid added transaction ID '{plaid_id}' has null/empty amount. Skipping add.")
                 continue
            amount_decimal = decimal.Decimal(amount_str)
            actual_amount = amount_decimal.copy_negate()
            # Payee
            payee = plaid_txn_dict.get("merchant_name") or plaid_txn_dict.get("name") or "Unknown Payee"
            # Notes
            notes = format_note_with_plaid_id(plaid_txn_dict)

            logger.info(f"Creating new Actual transaction for Plaid ID: {plaid_id} (Date: {txn_date}, Payee: '{payee}', Amount: {actual_amount})")
            # Create
            create_transaction(session, date=txn_date, account=account, payee=payee, notes=notes, amount=actual_amount)
            added_count += 1
        except Exception as e:
            logger.error(f"Failed to create Actual transaction for Plaid ID {plaid_id}: {e}", exc_info=True)

    logger.info(f"Processing summary: {deleted_count} deleted, {updated_count} updated (marked for update), {added_count} added.")
    return deleted_count, updated_count, added_count

# ------------------------------------------------------------------------------
# 7. Sync process using Plaid transactions_sync
# ------------------------------------------------------------------------------
def sync_transactions(is_manual_run=False, retry_count=0): # Add retry_count
    """
    Fetch transactions from Plaid via transactions_sync, process updates in Actual,
    and schedule the next run if auto-sync is enabled. Handles Plaid pagination errors.
    """
    global global_access_token, sync_after_id
    MAX_RETRIES = 1

    if retry_count == 0:
        logger.info("Starting sync cycle...")

    current_access_token = token_var.get().strip()
    if not current_access_token:
        logger.error("Plaid Access Token is missing. Cannot sync.")
        if sync_after_id: on_stop()
        if is_manual_run: sync_now_btn.config(state="normal")
        return
    global_access_token = current_access_token

    # --- Load Cursor ---
    cursor = None
    try:
        if os.path.exists(STATE_FILE):
            with open(STATE_FILE, "r") as f:
                state_data = json.load(f)
                cursor = state_data.get("last_cursor")
                if retry_count == 0:
                     logger.info(f"Loaded previous cursor: {cursor}")
    except (IOError, json.JSONDecodeError) as e:
        logger.warning(f"Could not load cursor from state file '{STATE_FILE}': {e}. Starting sync from beginning (if no cursor).")

    # --- Fetch from Plaid ---
    all_added, all_modified, all_removed = [], [], []
    new_cursor = cursor
    has_more = True
    plaid_fetch_success = False
    plaid_pagination_error = False

    try:
        configuration = get_plaid_configuration()
        api_client = ApiClient(configuration)
        plaid_client = plaid_api.PlaidApi(api_client)
        while has_more:
            request_obj = TransactionsSyncRequest(access_token=global_access_token)
            if new_cursor: request_obj.cursor = new_cursor
            logger.debug(f"Fetching Plaid transactions with cursor: {new_cursor}")
            response = plaid_client.transactions_sync(request_obj)
            res = response.to_dict()
            added = res.get("added", [])
            modified = res.get("modified", [])
            removed = res.get("removed", [])
            has_more = res.get("has_more", False)
            next_cursor_from_plaid = res.get("next_cursor")
            all_added.extend(added)
            all_modified.extend(modified)
            all_removed.extend(removed)
            new_cursor = next_cursor_from_plaid
            logger.info(f"Fetched page: {len(added)} new, {len(modified)} modified, {len(removed)} removed. Has More: {has_more}")

        logger.info(f"Plaid fetch complete. Total: {len(all_added)} added, {len(all_modified)} modified, {len(all_removed)} removed.")
        plaid_fetch_success = True

    except ApiException as e:
        error_body_dict = {}
        error_body_str = str(e.body)
        try:
             error_body_dict = json.loads(e.body) if isinstance(e.body, (str, bytes)) else e.body if isinstance(e.body, dict) else {}
             error_body_str = json.dumps(error_body_dict)
        except json.JSONDecodeError:
             logger.debug(f"Plaid API error body was not valid JSON: {e.body}")
             error_body_dict = {}

        error_code = error_body_dict.get("error_code")
        logger.error(f"Plaid API error during transaction sync: {error_body_str}", exc_info=False) # exc_info=False for Plaid API errors unless debugging

        if error_code == "TRANSACTIONS_SYNC_MUTATION_DURING_PAGINATION":
            plaid_pagination_error = True
            if retry_count < MAX_RETRIES:
                logger.warning(f"Plaid pagination error detected. Retrying sync after {RETRY_DELAY_SECONDS} seconds (Attempt {retry_count + 1}/{MAX_RETRIES})...")
                root.after(RETRY_DELAY_SECONDS * 1000, lambda: sync_transactions(is_manual_run=is_manual_run, retry_count=retry_count + 1))
                return
            else:
                logger.error(f"Plaid pagination error persisted after {MAX_RETRIES} retries. Aborting sync cycle.")
        elif "ITEM_LOGIN_REQUIRED" in error_body_str:
             logger.error("Plaid item requires login. Please re-link the account using Plaid Link.")
             if sync_after_id: on_stop()

    except ValueError as e:
         logger.error(f"Configuration error: {e}")
    except Exception as e:
        logger.error(f"Unexpected error during Plaid transaction sync: {e}", exc_info=True)

    # --- Save Cursor ---
    if plaid_fetch_success:
        try:
            with open(STATE_FILE, "w") as f:
                json.dump({"last_cursor": new_cursor}, f)
                logger.info(f"Successfully saved new cursor to {STATE_FILE}: {new_cursor}")
        except IOError as e:
            logger.error(f"CRITICAL: Failed to save cursor to state file '{STATE_FILE}': {e}. Risk of duplicates!")
            plaid_fetch_success = False

    # --- Process Updates in Actual Budget ---
    actual_update_success = False
    needs_actual_update = plaid_fetch_success and (all_added or all_modified or all_removed or cursor is None)

    if needs_actual_update:
        logger.info("Connecting to Actual Budget to process updates...")
        actual_url = actual_url_var.get().strip()
        actual_pass = actual_pass_var.get().strip()
        budget_name = budget_var.get().strip()
        account_name = account_var.get().strip()
        if not all([actual_url, actual_pass, budget_name, account_name]):
            logger.error("Actual Budget settings are incomplete. Cannot process updates.")
        else:
            try:
                with Actual(base_url=actual_url, password=actual_pass, file=budget_name) as act:
                    session = act.session
                    logger.info(f"Connected to Actual Budget file '{budget_name}'.")
                    acct = get_account(session, account_name)
                    if acct is None:
                        logger.info(f"Account '{account_name}' not found; creating it now.")
                        acct = create_account(session, name=account_name)
                        if acct is None:
                             raise Exception(f"Failed to create Actual account '{account_name}'. Check actualpy documentation.")
                        logger.info(f"Created Actual account '{account_name}' with ID {acct.id}. You may need to set the account type manually in Actual Budget.")

                    d_count, u_count, a_count = process_plaid_updates(session, acct, all_added, all_modified, all_removed)
                    if d_count > 0 or u_count > 0 or a_count > 0:
                         logger.info("Committing changes to Actual Budget...")
                         act.commit()
                         logger.info("Actual Budget changes committed successfully.")
                    else:
                         logger.info("No changes needed to be committed to Actual Budget.")
                    actual_update_success = True

            except ImportError:
                 logger.error("Failed to import 'actualpy'. Is it installed correctly?")
            except Exception as e:
                logger.error(f"Error during Actual Budget update: {e}", exc_info=True)

    elif plaid_fetch_success:
        logger.info("No new, modified, or removed transactions fetched from Plaid.")
        actual_update_success = True

    # --- Reschedule ---
    overall_success = plaid_fetch_success and actual_update_success

    if not plaid_pagination_error or retry_count >= MAX_RETRIES:
        if sync_after_id: # Auto-sync mode
            if overall_success:
                interval_hours = max(1, interval_var.get())
                interval_ms = interval_hours * 3600 * 1000
                logger.info(f"Scheduling next sync in {interval_hours} hours.")
                sync_after_id = root.after(interval_ms, lambda: sync_transactions(is_manual_run=False, retry_count=0))
            else:
                logger.error("Sync cycle failed. Stopping auto-sync.")
                on_stop()
        else: # Manual sync mode
             if is_manual_run:
                  logger.info("Manual sync finished.")
                  if not sync_after_id:
                       sync_now_btn.config(state="normal")

        if not (is_manual_run and overall_success):
             logger.info("Sync cycle finished.")


# ------------------------------------------------------------------------------
# 8. Handlers for GUI buttons
# ------------------------------------------------------------------------------

def set_config_state(state):
    """Enable/disable configuration widgets."""
    for child in config_frame.winfo_children() + actual_frame.winfo_children():
        widget_type = child.winfo_class()
        if widget_type in ('TEntry', 'TCombobox', 'TSpinbox'):
            try: child.config(state=state)
            except tk.TclError: pass

def on_start():
    """Start automatic background synchronization."""
    global sync_after_id
    if sync_after_id:
        logger.warning("Auto-sync is already running.")
        return
    logger.info("Starting automatic synchronization...")
    set_config_state("disabled")
    start_btn.config(state="disabled")
    stop_btn.config(state="normal")
    sync_now_btn.config(state="disabled")
    launch_link_btn.config(state="disabled")
    sync_after_id = root.after(100, lambda: sync_transactions(is_manual_run=False, retry_count=0))

def on_stop():
    """Stop automatic background synchronization."""
    global sync_after_id
    if sync_after_id:
        logger.info("Stopping automatic synchronization...")
        try: root.after_cancel(sync_after_id)
        except ValueError: logger.debug("No active sync task found to cancel.")
        sync_after_id = None
        set_config_state("normal")
        start_btn.config(state="normal")
        stop_btn.config(state="disabled")
        sync_now_btn.config(state="normal")
        launch_link_btn.config(state="normal")
        logger.info("Auto-sync stopped.")
    else:
        logger.warning("Auto-sync is not currently running.")

def on_sync_now():
    """Manually trigger a one-time sync cycle."""
    if sync_after_id:
        logger.warning("Cannot run manual sync while auto-sync is active. Stop auto-sync first.")
        return
    logger.info("Manual sync requested.")
    sync_now_btn.config(state="disabled")
    root.after(100, lambda: sync_transactions(is_manual_run=True, retry_count=0))

launch_link_btn.config(command=launch_plaid_link)
start_btn.config(command=on_start)
stop_btn.config(command=on_stop)
sync_now_btn.config(command=on_sync_now)

# ------------------------------------------------------------------------------
# 9. Graceful Exit & Start Main Loop
# ------------------------------------------------------------------------------

def on_closing():
    """Handle window closing event."""
    logger.info("Close requested. Stopping sync if running...")
    if sync_after_id: on_stop()
    logger.info("Exiting application.")
    root.destroy()

root.protocol("WM_DELETE_WINDOW", on_closing)

try:
    current_time_str = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    logger.info(f"Application started ({current_time_str}). Fill details, use Link if needed, then Start/Sync.")
    root.mainloop()
except KeyboardInterrupt:
    logger.info("Keyboard interrupt received. Exiting.")
    on_closing()
