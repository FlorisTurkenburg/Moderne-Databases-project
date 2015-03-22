import xml.sax as sax
import btree


class NVDContentHandler(sax.ContentHandler):
    def __init__(self):
        self.CurrentTag = ""
        self.id = ""
        self.products = []
        self.db = btree.start_up(filename="nvd_database", max_size=100)

        sax.ContentHandler.__init__(self)

    def startElement(self, name, attrs):
        # print("startElement '", name, "'")
        self.CurrentTag = name
        if name == "entry":
            # print("\t attribute id='" + attrs.getValue("id") + "'")
            self.id = attrs.getValue("id")


    def endElement(self, name):
        # print("endElement '", name, "'")
        self.CurrentTag = ""
        if name == "entry":
            # print("Entry:", self.id, "\n\t Products:", self.products,"\n")
            self.db[self.id] = self.products
            self.products = []

        elif name == "nvd":
            self.db._commit()



    def characters(self, content):
        # print("characters '", content, "'")
        if self.CurrentTag == "vuln:product":
            self.products += [content] 


def main():
    source = open("nvdcve-2.0-2014.xml")
    sax.parse(source, NVDContentHandler())


if __name__ == '__main__':
    main()