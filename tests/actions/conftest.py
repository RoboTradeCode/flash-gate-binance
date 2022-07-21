import pytest
from flash_gate import Gate, Configurator

CONFIGURATOR_SOURCE = "https://configurator.robotrade.io/exmo/test?only_new=false"


@pytest.fixture(scope="session")
async def config() -> dict:
    configurator = Configurator("api", CONFIGURATOR_SOURCE)
    config = await configurator.get_config()
    return config


@pytest.fixture(scope="module")
def gate(config: dict) -> Gate:
    with Gate(config) as gate:
        yield gate


def core_publisher(scope="session")