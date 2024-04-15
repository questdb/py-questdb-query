from typing import Optional


class QueryError(Exception):
    def __init__(self, message: str, query: str, position: Optional[int] = None):
        super().__init__(message)
        self.query = query
        self.position = position

    @classmethod
    def from_json(cls, json: dict):
        message = json.get('error')
        if not message:
            message = json.get('message')
        return cls(
            message=message,
            query=json.get('query'),
            position=json.get('position'))

