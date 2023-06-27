class Endpoint:
    """
    HTTP connection parameters into QuestDB
    """
    def __init__(self, host='127.0.0.1', port=None, https=True, username=None, password=None):
        self.host = host
        self.port = port or (443 if https else 9000)
        self.https = https
        self.username = username
        self.password = password

    @property
    def url(self):
        protocol = 'https' if self.https else 'http'
        return f'{protocol}://{self.host}:{self.port}'
