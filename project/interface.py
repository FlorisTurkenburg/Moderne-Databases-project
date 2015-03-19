from tornado.ioloop import IOLoop
from tornado.web import RequestHandler, Application, url
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
        self.write("All documents:</br>")
        for doc in self.db._get_documents():
            self.write('<a href="%s">link to document: %s</a></br>' % 
                (self.reverse_url("document", doc), doc))

class DocumentHandler(RequestHandler):
    def initialize(self, db):
        self.db = db

    def get(self, doc_key):
        self.write("Content of document " + str(doc_key) + " :\n" + str(self.db[doc_key]))


def make_app():
    tree = btree.start_up(4)
    return Application([
        url(r"/", MainHandler),
        url(r"/documents/", DocumentsHandler, dict(db=tree), name="documents"),
        url(r"/documents/([a-z]+)", DocumentHandler, dict(db=tree), name="document")
    ])

def main():
    app = make_app()
    app.listen(8888)
    IOLoop.current().start()

if __name__ == '__main__':
    main()