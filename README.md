[![OKX](https://user-images.githubusercontent.com/44947427/166470904-b9810b07-520b-421f-b180-1d33fed8cd6a.png)](https://www.okx.com)

# gate-okx-python

[![Python](https://img.shields.io/badge/python-3.10-blue)](https://www.python.org/downloads/)
[![CPython](https://img.shields.io/badge/implementation-cpython-blue)](https://github.com/python/cpython)
[![Linux](https://img.shields.io/badge/platform-linux-lightgrey)](https://ru.wikipedia.org/wiki/Linux)
[![Code style: black](https://img.shields.io/badge/code%20style-black-000000.svg)](https://github.com/psf/black)

Гейт [OKX](https://www.okx.com).

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

## Использование

```shell
pipenv run python main.py
```

> Перед запуском скрипта, у вас должен быть запущен медиа-драйвер Aeron. Его можно запустить командой `aeronmd`
