import sys
import http.client
import urllib
import json
import hashlib
import hmac
import time


class ExmoAPI:
    def __init__(self, API_KEY, API_SECRET, API_URL="api.exmo.com", API_VERSION="v1"):
        self.API_URL = API_URL
        self.API_VERSION = API_VERSION
        self.API_KEY = API_KEY
        self.API_SECRET = bytes(API_SECRET, encoding="utf-8")

    def sha512(self, data):
        H = hmac.new(key=self.API_SECRET, digestmod=hashlib.sha512)
        H.update(data.encode("utf-8"))
        return H.hexdigest()

    def api_query(self, api_method, params={}):
        params["nonce"] = int(round(time.time() * 1000))
        params = urllib.parse.urlencode(params)

        sign = self.sha512(params)
        headers = {
            "Content-type": "application/x-www-form-urlencoded",
            "Key": self.API_KEY,
            "Sign": sign,
        }
        conn = http.client.HTTPSConnection(self.API_URL)
        conn.request("POST", "/" + self.API_VERSION + "/" + api_method, params, headers)
        response = conn.getresponse().read()

        conn.close()

        try:
            obj = json.loads(response.decode("utf-8"))
            return obj
        except json.decoder.JSONDecodeError:
            print("Error while parsing response:", response)


API_KEY = "K-5c48ca01887ddf50ea7094e021b1f37c37ced971"
SECRET_KEY = "S-127383e2a3cc853a0be497520de08029d1016b9f"
exmo = ExmoAPI(API_KEY, SECRET_KEY)

order = {
    "pair": "BTC_USDT",
    "quantity": 0.0001,
    "type": "market_buy",
    "price": 0,
}

balance_before = exmo.api_query("user_info")
print(f"Balance before: {balance_before}")

created_order = exmo.api_query("order_create", order)
print(f"Created order: {created_order}")

fetched_order = exmo.api_query(
    "order_trades",
    {"order_id": created_order["order_id"]},
)
print(f"Fetched order: {fetched_order}")

balance_after = exmo.api_query("user_info")["balances"]["BTC"]
print(f"Balance after: {balance_after}")

user_trades = exmo.api_query("user_trades")
print(f"User trades: {user_trades}")
