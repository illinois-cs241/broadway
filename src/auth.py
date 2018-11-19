from src.settings import UNAUTHORIZED_REQUEST_CODE


def authenticate(func):
    def wrapper(*args, **kwargs):
        self = args[0]
        token = self.settings.get("token")
        request_token = self.request.headers.get("Authorization")

        if (request_token is None) or (token != request_token):
            self.set_status(UNAUTHORIZED_REQUEST_CODE)
            self.fail({"message": "Not authorized"})
        else:
            return func(*args, **kwargs)

    return wrapper
