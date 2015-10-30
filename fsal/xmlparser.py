from xml.sax import make_parser
from xml.sax.handler import ContentHandler
try:
    from StringIO import StringIO
except ImportError:
    from io import StringIO


class Node(object):

    def __init__(self, tag):
        self.tag = tag
        self.children = []
        self.is_root = False
        self.data = ''

    def add_child(self, element):
        self.children.append(element)

    def add_data(self, data):
        self.data += data

    def __getattr__(self, key):
        matching_children = [x for x in self.children if x.tag == key]
        if matching_children:
            if len(matching_children) == 1:
                return matching_children[0]
            else:
                return matching_children
        else:
            raise AttributeError(
                "'%s' has no attribute '%s'" % (self.tag, key)
            )


class SaxHandler(ContentHandler):

    def __init__(self):
        self.root = Node(None)
        self.root.is_root = True
        self.node_stack = []

    def startElement(self, name, attrs):
        name = SaxHandler.clean_name(name)
        node = Node(name)
        if len(self.node_stack) > 0:
            self.node_stack[-1].add_child(node)
        else:
            self.root.add_child(node)
        self.node_stack.append(node)

    def endElement(self, name):
        self.node_stack.pop()

    def characters(self, cdata):
        self.node_stack[-1].add_data(cdata)

    @staticmethod
    def clean_name(name):
        name = name.replace('-', '_')
        return name


def parsestring(xml_str):
    """
    Returns python object which represent the input xml
    """
    sax_parser = make_parser()
    sax_handler = SaxHandler()
    sax_parser.setContentHandler(sax_handler)
    sax_parser.parse(StringIO(xml_str))
    return sax_handler.root
