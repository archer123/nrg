import requests

class Dptr:

    def __init__(self):
        self.proto = 0
        self.url = ''

    def unregister_node(self, ID):
        response = requests.delete(self.url + 'son/nodes/' + hex(ID))
        return response