def map(key, value):
    # For every product, emit the name as key, and value 1
    for product in value:
        emit(product, 1)


def reduce(key, value):
    return key, sum(value)