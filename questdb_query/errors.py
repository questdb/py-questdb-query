from typing import Optional


class QueryError(Exception):
    def __init__(self, message: str, query: str, position: Optional[int] = None):
        super().__init__(message)
        self.query = query
        self.position = position

    @classmethod
    def from_json(cls, json: dict):
        return cls(
            message=json['error'],
            query=json['query'],
            position=json.get('position'))
