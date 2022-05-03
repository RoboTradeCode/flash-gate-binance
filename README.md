[![OKX](https://user-images.githubusercontent.com/44947427/166470904-b9810b07-520b-421f-b180-1d33fed8cd6a.png)](https://www.okx.com)

# gate-okx-python

[![Python](https://img.shields.io/badge/python-3.10-blue)](https://www.python.org/downloads/)
[![CPython](https://img.shields.io/badge/implementation-cpython-blue)](https://github.com/python/cpython)
[![Linux](https://img.shields.io/badge/platform-linux-lightgrey)](https://ru.wikipedia.org/wiki/Linux)
[![Code style: black](https://img.shields.io/badge/code%20style-black-000000.svg)](https://github.com/psf/black)

Гейт [OKX](https://www.okx.com).

## Установка

### Предварительные требования

Перед установкой и использованием данного гейта, у вас должен быть установлен Aeron. Если это не так, то установите его,
воспользовавшись [официальным руководством](https://github.com/real-logic/aeron#c-build):

1. Установите зависимости сборки:

```shell
sudo apt update
sudo apt install --assume-yes git cmake g++ default-jdk libbsd-dev uuid-dev
git clone --branch 1.38.2 --depth 1 https://github.com/real-logic/aeron.git
```

2. Соберите и протестируйте код:

```shell
cd aeron
mkdir --parents cppbuild/Debug
cd cppbuild/Debug
cmake -DCMAKE_BUILD_TYPE=Debug ../..
cmake --build . --clean-first
ctest
```

> Вы можете ускорить сборку, указав команде `cmake --build` максимальное количество параллельных процессов. За это
> отвечает параметр [`--parallel`](https://cmake.org/cmake/help/latest/manual/cmake.1.html#build-a-project)

3. Установите библиотеку в систему

```shell
sudo cmake --install .
```

> По умолчанию CMake установит библиотеку в `/usr/local`. Вы можете изменить директорию установки с помощью
> параметра [`--prefix`](https://cmake.org/cmake/help/latest/variable/CMAKE_INSTALL_PREFIX.html#variable:CMAKE_INSTALL_PREFIX)

### Сборка и установка зависимостей

```shell
pipenv install
```

> При установке зависимостей используется подключение с помощью SSH. Подробнее о нём вы можете прочитать в
> руководстве ["Connecting to GitHub with SSH"](https://docs.github.com/en/authentication/connecting-to-github-with-ssh)

## Использование

```shell
pipenv run python main.py
```
