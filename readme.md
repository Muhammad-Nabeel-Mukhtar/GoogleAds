# Google Ads Backend API

This backend provides API endpoints for:
- Creating new Google Ads client accounts under your Manager (MCC) account
- Listing all linked (client) accounts under any specified Manager account

## Setup Instructions

1. **Clone or download this repository**
2. **Install dependencies**  
pip install -r requirements.txt

3. **Add your `google-ads.yaml` config**  
- Place it in `config/` or project root as needed  
- Never share this file publicly; it contains sensitive credentials

4. **Run the backend server**  
python -m app.main

or, if using a single top-level file:
python google_ads_backend.py



## API Endpoints

### **1. Create a Google Ads Account**
- **POST** `/create-account`
- **Body (JSON):**
 ```
 {
   "name": "Example Account Name",
   "currency": "USD",
   "timezone": "America/New_York",
   "tracking_url": "{lpurl}?device={device}",        // optional
   "final_url_suffix": "keyword={keyword}"           // optional
 }
 ```
- **Response Example (success):**
 ```
 {
   "success": true,
   "resource_name": "customers/XXXXXXXXXX",
   "customer_id": "XXXXXXXXXX"
 }
 ```
- **Response Example (error):**
 ```
 {
   "success": false,
   "errors": ["Error message details..."]
 }
 ```

### **2. List Linked Accounts (for any MCC)**
- **GET** `/list-linked-accounts?mcc_id=YOUR_MCC_ID`
- **Response:**
 ```
 {
   "success": true,
   "accounts": [
     {"client_id": "XXXXXXXXXX", "name": "Account Name", "status": "ENABLED"}
   ]
 }
 ```

## Configuration

- **Manager account ID** (`MCC_CUSTOMER_ID`) for account creation is set in `app/google_ads_service.py`.

## Notes

- This backend should be run on a secure server and not exposed publicly without proper authentication controls.
- The Google Ads API credentials and `google-ads.yaml` must be kept private.
- Customize and expand the endpoints/modules as needed for your business logic.

