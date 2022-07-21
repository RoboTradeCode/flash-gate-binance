<a href="https://www.okx.com">
    <img alt="OKX" src="https://user-images.githubusercontent.com/44947427/175785831-05075124-3cb8-4166-ae16-031c0c68dcd9.png">
</a>
<a href="https://kuna.io">
    <img alt="Kuna" height="48" src="https://user-images.githubusercontent.com/44947427/175785516-e1bbd230-535a-4f69-8f9f-5cd1b034ab8b.svg">
</a>
<a href="https://exmo.me">
    <img alt="EXMO" height="48" src="https://user-images.githubusercontent.com/44947427/175785584-6c573ba3-98f2-4942-9c2b-ebb03c963ca3.svg">
</a>

# Flash Gate

[![Python](https://img.shields.io/badge/python-3.10-blue)](https://www.python.org)
[![CPython](https://img.shields.io/badge/implementation-cpython-blue)](https://github.com/python/cpython)
[![Linux](https://img.shields.io/badge/platform-linux-lightgrey)](https://ru.wikipedia.org/wiki/Linux)
[![Code style: black](https://img.shields.io/badge/code%20style-black-000000.svg)](https://github.com/psf/black)

## Установка

### Предварительные требования

Перед установкой и использованием данного гейта, у вас должен быть установлен Aeron, Python 3.10 и Pipenv. Вы можете
воспользоваться статьями в Wiki для установки всего необходимого:

- [Установка Aeron](https://github.com/RoboTradeCode/gate-okx-python/wiki/Установка-Aeron)
- [Установка Python](https://github.com/RoboTradeCode/gate-okx-python/wiki/Установка-Python)
- [Установка Pipenv](https://github.com/RoboTradeCode/gate-okx-python/wiki/Установка-Pipenv)

### Установка зависимостей

```shell
pipenv install
```

> При установке зависимостей используется подключение с помощью SSH. Подробнее о нём вы можете прочитать в
> руководстве ["Connecting to GitHub with SSH"](https://docs.github.com/en/authentication/connecting-to-github-with-ssh)

### Конфигурация

Описание начальной и типовой конфигураций приведено
в [одноимённом разделе Wiki](https://github.com/RoboTradeCode/gate-okx-python/wiki/Конфигурация)

## Использование

```shell
pipenv run python main.py
```

> Перед запуском скрипта, у вас должен быть запущен медиа-драйвер Aeron. Его можно запустить командой `aeronmd`

### Логирование

В своей работе гейт отправляет следующие логи:

- Сообщения от ядра
- Изменение баланса
- Изменение ордеров (создание / отмена / статус)
- Периодическая метрика (ping)

> Гейт не хранит историю изменений баланса. Поэтому под изменением баланса подразумевается ретрансляция с
> соответствующего сокета биржы

### Rate Limiter

В гейте выключен контроль скорости отправки сообщений. Ядро должно следить за тем, чтобы
ограничения для конкретной биржы не были превышены
