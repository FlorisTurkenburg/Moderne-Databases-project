from tornado.ioloop import IOLoop
from tornado.web import RequestHandler, Application, url, RedirectHandler
import btree


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

    def get(self):
        # self.write("All documents:</br>")
        
        # for doc in self.db._get_documents():
        #     self.write('<a href="%s">link to document: %s</a></br>' % 
        #         (self.reverse_url("document", doc), doc))

        # self.write('<br><a href="/insertDoc/">Click here to insert a document.</a>')
        self.render("doc_list.html", title="All Documents", items=self.db._get_documents())

    def post(self):
        self.set_header("Content-Type", "text/plain")
        key = self.get_body_argument("docKey")
        value = self.get_body_argument("docContent")
        self.db[key] = value
        self.db._commit("data")

        message = "You inserted key=" + repr(key) + " with value=" + repr(value) + " into the database."
        self.write('alert("%s")' % message)



class DocumentHandler(RequestHandler):
    def initialize(self, db):
        self.db = db

    def get(self, doc_key):
        self.write("Content of document " + str(doc_key) + " :<br>" + str(self.db[doc_key]))


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

    



def make_app():
    tree = btree.start_up(4)
    return Application([
        url(r"/", MainHandler),
        url(r"/documents", RedirectHandler, dict(url=r"/documents/") ),
        url(r"/documents/", DocumentsHandler, dict(db=tree), name="documents"),
        url(r"/document/([a-zA-Z0-9_]+)", DocumentHandler, dict(db=tree), name="document"),
        url(r"/insertDoc/", InsertDocHandler, dict(db=tree), name="insertdocument")
    ])

def main():
    app = make_app()
    app.listen(8888)
    IOLoop.current().start()

if __name__ == '__main__':
    main()