def main():
    s = {1, 2, 3, 4, 5, 6, 7, 8, 9, 10}

    for x in s:
        print(x)
        if x % 2 == 0:
            s.discard(x)

    print(s)


if __name__ == "__main__":
    main()
