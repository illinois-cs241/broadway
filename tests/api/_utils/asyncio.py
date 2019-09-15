import asyncio


def to_sync(awaitable):
    return asyncio.get_event_loop().run_until_complete(awaitable)
