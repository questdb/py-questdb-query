import re

class Endpoint:
    """
    HTTP connection parameters into QuestDB
    """
    def __init__(
                self,
                host='127.0.0.1',
                port=None,
                https=False,
                username=None,
                password=None,
                token=None):
        self.host = host
        self.port = port or (443 if https else 9000)
        self.https = https
        self.username = username
        self.password = password
        self.token = token
        if ((self.username or self.password) and \
            not (self.username and self.password)):
            raise ValueError('Must provide both username and password or neither')
        if self.token and self.username:
            raise ValueError('Cannot use token with username and password')
        if token and not re.match(r'^[A-Za-z0-9-._~+/]+=*$', token):
            # https://datatracker.ietf.org/doc/html/rfc6750#section-2.1
            raise ValueError("Invalid characters in token")

    @property
    def url(self):
        protocol = 'https' if self.https else 'http'
        return f'{protocol}://{self.host}:{self.port}'
