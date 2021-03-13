from typing import List


class Course:
    def __init__(self, id_: str, tokens: List[str] = [], query_tokens: List[str] = []):
        self.id = id_
        self.tokens = tokens
        self.query_tokens = query_tokens
