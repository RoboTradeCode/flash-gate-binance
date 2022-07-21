import sentry_sdk
from .configurator import Configurator
from .gate import Gate

SENTRY_PUBLIC_KEY = "f993873cec4947ba985cad95ec73ea5b"
SENTRY_DSN = f"https://{SENTRY_PUBLIC_KEY}@o1326766.ingest.sentry.io/6587314"

# Set traces_sample_rate to 1.0 to capture 100%
# of transactions for performance monitoring.
# We recommend adjusting this value in production.
sentry_sdk.init(dsn=SENTRY_DSN, traces_sample_rate=1.0, send_default_pii=True)
