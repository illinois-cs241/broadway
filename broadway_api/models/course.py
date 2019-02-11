from typing import List


class Course:
    def __init__(self, id: str, tokens: List[str] = []):
        self.id = id
        self.tokens = tokens
