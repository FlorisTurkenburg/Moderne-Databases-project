def reduce(key, value):
    # reduce the values of the key
    return key, sum(value)