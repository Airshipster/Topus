"""Cooperative boundary between durable publication batches."""
import time

CONTINUE_QUEUE = 76
PUBLICATION_BATCH_SECONDS = 600


class PublicationBudget:
    def __init__(self, clock=time.monotonic, seconds=PUBLICATION_BATCH_SECONDS):
        self.clock = clock
        self.deadline = clock() + seconds
        self.attempts = 0

    def should_yield(self):
        # Even a slow startup must be able to make progress on the queue.
        return self.attempts > 0 and self.clock() >= self.deadline

    def attempted(self):
        self.attempts += 1
