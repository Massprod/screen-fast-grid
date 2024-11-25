

class GridObject:

    def __init__(
            self,
            whitespace: bool = False,
            identifier: bool = False,
            wheelstack: bool = False,
            identifier_string: str = ''):
        self.whitespace: bool = whitespace
        self.identifier: bool = identifier
        self.wheelstack: bool = wheelstack
        self.identifier_string: str = identifier_string

    def reset_object(self):
        self.whitespace = False
        self.identifier = False
        self.wheelstack = False
        self.identifier_string = ''
    
    def get_dict(self):
        return {
            'wheelStack': self.wheelstack,
            'whitespace': self.whitespace,
            'identifier': self.identifier,
            'identifierString': self.identifier_string,
        }
