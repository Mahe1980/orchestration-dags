import re

ALPHANUMERIC_PATTERN = re.compile(r'\W+')


def encode_id(original):
    return ALPHANUMERIC_PATTERN.sub("_", original).lower()


def pubsub_pull_messages(subscription, hook, max_messages=20):
    project, subscription = subscription.split('.')
    while True:
        messages = hook.pull(project, subscription, max_messages, return_immediately=True)
        if messages:
            yield from iter(messages)
        else:
            raise StopIteration
