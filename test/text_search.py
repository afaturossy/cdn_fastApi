import re

if __name__ == "__main__":
    just = 'Standard Price:20.000'

    price = ''.join(filter(str.isdecimal, just))
    print(price)

    stringToSearch = 'chapter 1'
    searchPattern = re.compile(".*(-?[0-9]\.[0-9]).*")
    searchMatch = searchPattern.search(stringToSearch)

    if searchMatch:
        floatValue = searchMatch.group(1)
        print(floatValue)
    else:
        raise Exception('float not found')