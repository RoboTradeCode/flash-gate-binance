from enum import Enum


class DriverType(str, Enum):
    """
    Тип драйвера
    """

    FILE = "file"
    HTTP = "api"
