def map(doc_key, doc_value):
    # do something with it and emit
    emit("length", len(doc_value)


def reduce(key, value):
    # reduce the values of the key
    return [key, sum(value)]