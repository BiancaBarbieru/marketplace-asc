"""
This module represents the Producer.

Computer Systems Architecture Course
Assignment 1
March 2021
"""

from threading import Thread
from time import sleep
import logging


class Producer(Thread):
    """
    Class that represents a producer.
    """

    def __init__(self, products, marketplace, republish_wait_time, **kwargs):
        """
        Constructor.

        @type products: List()
        @param products: a list of products that the producer will produce

        @type marketplace: Marketplace
        @param marketplace: a reference to the marketplace

        @type republish_wait_time: Time
        @param republish_wait_time: the number of seconds that a producer must
        wait until the marketplace becomes available

        @type kwargs:
        @param kwargs: other arguments that are passed to the Thread's __init__()
        """
        Thread.__init__(self, **kwargs)
        self.products = products
        self.marketplace = marketplace
        self.republish_wait_time = republish_wait_time
        self.producer_id = self.marketplace.register_producer()

    def run(self):
        # While there are products to be published the producer will try to post
        # one at every wait_time seconds, if the product couldn't be published
        # the producer will wait republish_wait_time seconds and will try again
        while True:
            for prod_name, no_products, wait_time in self.products:
                while no_products:
                    published = self.marketplace.publish(self.producer_id, prod_name)
                    if published:
                        no_products -= 1
                        sleep(wait_time)
                    else:
                        sleep(self.republish_wait_time)
