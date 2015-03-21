from tornado.ioloop import IOLoop
from tornado.web import RequestHandler, Application, url, RedirectHandler
from tornado.escape import json_decode
import json
import btree
import astevalscript
import os


class HelloHandler(RequestHandler):
    def get(self):
        self.write("Hello, world")

class MainHandler(RequestHandler):
    def get(self):
        self.write('<a href="%s">link to documents</a>' %
                   self.reverse_url("documents"))

class DocumentsHandler(RequestHandler):
    def initialize(self, db):
        self.db = db

    def prepare(self):
        if "Content-Type" in self.request.headers:
            if self.request.headers["Content-Type"].startswith("application/json"):
                self.json_args = json.loads(self.request.body.decode("utf-8"))
            else:
                self.json_args = None

    def get(self):
        request_header = self.request.headers.get('User-Agent')

        # plain object with curl
        if request_header.startswith('curl/'):
            self.write(dict(self.db))
        # render html in browser
        else:
            self.render("doc_list.html", title="All Documents", items=self.db._get_documents())

    def post(self):
        if self.json_args != None:
            print(self.json_args)
            key = self.json_args["docKey"]
            value = self.json_args["docContent"]
        else:
            key = self.get_body_argument("docKey")
            value = self.get_body_argument("docContent")
        
        self.db[key] = value
        self.db._commit()

        self.set_header("Content-Type", "text/plain")
        message = "You inserted key=" + repr(key) + " with value=" + repr(value) + " into the database."
        self.write(message)



class DocumentHandler(RequestHandler):
    def initialize(self, db):
        self.db = db

    def prepare(self):
        if "Content-Type" in self.request.headers:
            if self.request.headers["Content-Type"].startswith("application/json"):
                self.json_args = json.loads(self.request.body.decode("utf-8"))
            else:
                self.json_args = None

    def get(self, doc_key):
        request_header = self.request.headers.get('User-Agent')

        # plain object with curl
        if request_header.startswith('curl/'):
            self.write({doc_key : self.db[doc_key]})
        # render html in browser
        else:
            self.write("Content of document " + str(doc_key) + " :<br>" + str(self.db[doc_key]))

    def put(self, doc_key):
        if self.db[doc_key] != None:
            print('found key ', doc_key)
            self.db[doc_key] = self.json_args["docContent"]
            self.db._commit()
        else:
            self.write('Document key not present in database')

class InsertDocHandler(RequestHandler):
    def initialize(self, db):
        self.db = db

    def get(self):
        self.write('<html><body><form action="/documents/" method="POST">'
                    'Key:<br>'
                    '<input type="text" name="docKey" required><br>'
                    'Document content:<br>'
                    '<input type="text" name="docContent" required><br>'
                    '<input type="submit" value="Insert Document">'
                    '</form></body></html>')

    
class CompactionHandler(RequestHandler):
    def initialize(self, db):
        self.db = db

    def get(self):
        self.db.compaction()
        self.write('Database is compacted')
        self.write('<a href="/documents/">Click here to go back to the document list</a>')


# global temp_tree
class MapReduce(RequestHandler):
    def initialize(self, db):
        self.db = db

    def prepare(self):
        if "Content-Type" in self.request.headers:
            if self.request.headers["Content-Type"].startswith("application/json"):
                self.json_args = json.loads(self.request.body.decode("utf-8"))
            else:
                self.json_args = None

    def post(self):
        script = astevalscript.Script()
        script.symtable["emit"] = emit
        
        # Load the user defined map and reduce functions
        if self.json_args != None:
            if "mapreduce" in self.json_args:
                script.add_file(self.json_args["mapreduce"])
            else:
                script.add_file(self.json_args["map"])
                script.add_file(self.json_args["reduce"])
        else:
            script.add_file("map.py")
            script.add_file("reduce.py")
            

        f = open("temp_map_store", "w")
        f.close()
        global temp_tree
        temp_tree = btree.start_up(filename="temp_map_store", max_size=4)
        # document_store = btree.start_up(filename="data", max_size=4)

        for key in self.db._get_documents():
            script.invoke("map", doc_key=key, doc_value=self.db[key])

        temp_tree._commit()

        self.write('The result of the MapReduce is:<br>')
        for key in temp_tree._get_documents():
            
            red_value = script.invoke("reduce", key=key, value=temp_tree[key])
            print(red_value)
            self.write(str(red_value) + '<br>')


        # delete the temporary document store
        os.remove("temp_map_store")


    def get(self):
        script = astevalscript.Script()
        script.symtable["emit"] = emit
        # The user defined map and reduce functions
        script.add_file("map.py")
        script.add_file("reduce.py")

        f = open("temp_map_store", "w")
        f.close()
        global temp_tree
        temp_tree = btree.start_up(filename="temp_map_store", max_size=4)
        # document_store = btree.start_up(filename="data", max_size=4)

        for key in self.db._get_documents():
            script.invoke("map", doc_key=key, doc_value=self.db[key])

        temp_tree._commit()

        self.write('The result of the MapReduce is:<br>')
        for key in temp_tree._get_documents():
            
            red_value = script.invoke("reduce", key=key, value=temp_tree[key])
            print(red_value)
            self.write(str(red_value) + '<br>')


        # delete the temporary document store
        os.remove("temp_map_store")


def emit(key, value):
    if temp_tree[key] != None:
        temp_tree[key] = temp_tree[key] + [value]

    else:
        temp_tree[key] = [value]


def make_app():
    tree = btree.start_up(filename="data", max_size=4)
    return Application([
        url(r"/", RedirectHandler, dict(url=r"/documents/")),
        url(r"/compact/?", CompactionHandler, dict(db=tree), name="compaction"),
        url(r"/documents/?", DocumentsHandler, dict(db=tree), name="documents"),
        url(r"/document/([a-zA-Z0-9_]+)", DocumentHandler, dict(db=tree), name="document"),
        url(r"/insertDoc/?", InsertDocHandler, dict(db=tree), name="insertdocument"),
        url(r"/mapreduce/?", MapReduce, dict(db=tree), name="MapReduce")
    ])

def main():
    app = make_app()
    app.listen(8888)
    IOLoop.current().start()

if __name__ == '__main__':
    main()


# curl -X POST localhost:8888/mapreduce/ -H "Content-Type:application/json" -d '{"mapreduce":"mapAndReduce.py"}'
# curl -X POST localhost:8888/mapreduce/ -H "Content-Type:application/json" -d '{"map":"map.py", "reduce":"reduce.py"}'
# curl -X POST localhost:8888/documents/ -H "Content-Type:application/json" -d '{"docKey":"jsontest", "docContent":"This is the content of a document inserted with json"}'
# curl -X POST localhost:8888/documents/ -d 'docKey=curltest&docContent=somecurlcontent'
