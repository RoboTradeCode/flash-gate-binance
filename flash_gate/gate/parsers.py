class ConfigParser:
    """
    Класс для получения необходимых шлюзу данных из конфигурации
    """

    def __init__(self, config: dict):
        self.config = config

    @property
    def _gate_config(self) -> dict:
        gate_config = self.config["data"]["configs"]["gate_config"]
        return gate_config

    @property
    def _rate_limits(self) -> dict:
        rate_limits = self._gate_config["rate_limits"]
        return rate_limits

    @property
    def exchange_id(self) -> str:
        exchange_id = self._gate_config["exchange"]["exchange_id"]
        return exchange_id

    @property
    def exchange_config(self) -> dict:
        gate_config = self._gate_config
        exchange_config = {
            "apiKey": gate_config["exchange"]["credentials"]["api_key"],
            "secret": gate_config["exchange"]["credentials"]["secret_key"],
            "password": gate_config["exchange"]["credentials"]["password"],
            "enableRateLimit": self._rate_limits["enable_ccxt_rate_limiter"],
        }
        return exchange_config

    @property
    def aeron_config(self) -> dict:
        aeron_config: dict = self._gate_config["aeron"]
        # После добавления поля решили, что отсутствие подписчика не является ошибкой
        # Следовательно логирование этой ситуации остаётся на усмотрение разработчика
        aeron_config.pop("no_subscriber_log_delay")
        return aeron_config

    @property
    def data_collection_method(self) -> dict:
        data_collection_method = self._gate_config["data_collection_method"]
        return data_collection_method

    @property
    def subscribe_delay(self) -> int:
        subscribe_delay = self._rate_limits["subscribe_timeout"]
        return subscribe_delay

    @property
    def fetch_delays(self) -> dict:
        fetch_delays = self._rate_limits["api_request_timeouts"]
        return fetch_delays

    @property
    def tickers(self) -> list[str]:
        markets = self.config["data"]["markets"]
        tickers = [market["common_symbol"] for market in markets]
        return tickers

    @property
    def order_book_limit(self) -> int:
        order_book_limit = self._gate_config["gate"]["order_book_depth"]
        return order_book_limit

    @property
    def assets(self) -> list[str]:
        assets_labels = self.config["data"]["assets_labels"]
        assets = [asset_label["common"] for asset_label in assets_labels]
        return assets
