import btree
import astevalscript
import os

def emit(key, value):
    if temp_tree[key] != None:
        temp_tree[key] = temp_tree[key] + [value]

    else:
        temp_tree[key] = [value]

    # temp_tree._commit()

f = open("temp_map_store", "w")
f.close()
temp_tree = btree.start_up(filename="temp_map_store", max_size=4)

def main():
    script = astevalscript.Script()
    script.symtable["emit"] = emit
    # The user defined map and reduce functions
    script.add_file("map.py")
    script.add_file("reduce.py")

    document_store = btree.start_up(filename="data", max_size=4)

    for key in document_store._get_documents():
        script.invoke("map", doc_key=key, doc_value=document_store[key])

    temp_tree._commit()


    for key in temp_tree._get_documents():
        print(script.invoke("reduce", key=key, value=temp_tree[key]))


    # delete the temporary document store
    os.remove("temp_map_store")


if __name__ == '__main__':
        main()    


