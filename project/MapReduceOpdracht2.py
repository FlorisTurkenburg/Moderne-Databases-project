def map(key, value):
    # Extract the vendor from the product strings and emit it as key, with value
    # 1 for every product
    for product in value:
        # print(product)
        vendor = product.split(':')[2]
        # print(vendor)
        emit(vendor, 1)


def reduce(key, value):
    return key, sum(value)