"""
This module represents the Marketplace.

Computer Systems Architecture Course
Assignment 1
March 2021
"""
from logging.handlers import RotatingFileHandler
from threading import Lock, current_thread
from .product import Product
import unittest
import logging

log = logging.getLogger()
log.setLevel(logging.INFO)
handler = RotatingFileHandler('marketplace.log', mode='a', maxBytes=3000, backupCount=7)
log.addHandler(handler)
    
class TestMarketplace(unittest.TestCase):
    
    def setUp(self):
        self.queue_size_per_producer = 5
        self.marketplace = Marketplace(self.queue_size_per_producer)
        self.prod1 = Product("prod1", 10)
        self.prod2 = Product("prod2", 10)
        
    def test_register(self):
        self.assertEqual(self.marketplace.register_producer(), 0)
        self.assertEqual(self.marketplace.register_producer(), 1)
    
    def test_publish(self):
        self.assertEqual(self.marketplace.register_producer(), 0)
        self.assertEqual(self.marketplace.register_producer(), 1)
        self.assertTrue(self.marketplace.publish(0, self.prod1))
        self.assertTrue(self.marketplace.publish(1, self.prod2))
        
    def test_cart(self):
        self.assertEqual(self.marketplace.new_cart(), 0)
        self.assertEqual(self.marketplace.new_cart(), 1)
        
    def test_add(self):
        self.assertEqual(self.marketplace.register_producer(), 0)
        self.assertTrue(self.marketplace.publish(0, self.prod1))
        self.assertEqual(self.marketplace.new_cart(), 0)
        self.assertTrue(self.marketplace.add_to_cart(0, self.prod1))
    
    def test_remove(self):
        self.assertEqual(self.marketplace.register_producer(), 0)
        self.assertTrue(self.marketplace.publish(0, self.prod1))
        self.assertEqual(self.marketplace.new_cart(), 0)
        self.assertTrue(self.marketplace.add_to_cart(0, self.prod1))
        self.assertFalse(self.marketplace.remove_from_cart(0, self.prod1))
        
    def test_order(self):
        self.assertEqual(self.marketplace.register_producer(), 0)
        self.assertTrue(self.marketplace.publish(0, self.prod1))
        self.assertEqual(self.marketplace.new_cart(), 0)
        self.assertTrue(self.marketplace.add_to_cart(0, self.prod1))
        products = self.marketplace.place_order(0)
        self.assertEqual(products, [self.prod1])
         

class Marketplace:
    """
    Class that represents the Marketplace. It's the central part of the implementation.
    The producers and consumers use its methods concurrently.
    """
    def __init__(self, queue_size_per_producer):
        """
        Constructor

        :type queue_size_per_producer: Int
        :param queue_size_per_producer: the maximum size of a queue associated with each producer
        """
        self.queue_size_per_producer = queue_size_per_producer
        
        self.register_lock = Lock()
        self.publish_lock = Lock()
        self.new_cart_lock = Lock() 
        self.place_order_lock = Lock()
          
        self.cart_items = {}     
        self.producers_dict = {}
        self.carts_dict = {}
        
        self.no_producers = 0
        self.no_carts = 0

    # For any registered producer the no_producers will be incremented.
    # Using lock I make sure that it is thread_safe.
    def register_producer(self):
        """
        Returns an id for the producer that calls this.
        """
        with self.register_lock:
            producer_id = self.no_producers
            self.no_producers += 1
            self.producers_dict[producer_id] = []
            self.cart_items[producer_id] = []
            log.info("A producer has been registered")
        return producer_id

    # If the limit of the queue_size_per_producer hasn't been touched the producer will try 
    # to publish a product. Using lock I make sure that it is thread_safe.
    def publish(self, producer_id, product): 
        """
        Adds the product provided by the producer to the marketplace

        :type producer_id: String
        :param producer_id: producer id

        :type product: Product
        :param product: the Product that will be published in the Marketplace

        :returns True or False. If the caller receives False, it should wait and then try again.
        """
        with self.publish_lock:
            if len(self.producers_dict[producer_id]) < self.queue_size_per_producer:
                self.producers_dict[producer_id].append(product)
                log.info("Producer with id: %s published a product ", producer_id)
                return True
            log.info("Producer with id: %s couldn't publish product", producer_id,)
        return False

    # For any new cart the no_carts will be incremented.
    # Using lock I make sure that it is thread_safe.
    def new_cart(self):
        """
        Creates a new cart for the consumer

        :returns an int representing the cart_id
        """
        with self.new_cart_lock:
            cart_id = self.no_carts
            self.no_carts += 1
            self.carts_dict[cart_id] = []
            log.info("A new cart has been created")
        return cart_id

    # The consumer will look for the product he needs through all the products published
    # in the publishers lists. If the product is found then the product is added to the 
    # cart and removed from the publisher's list of products
    def add_to_cart(self, cart_id, product):
        """
        Adds a product to the given cart. The method returns

        :type cart_id: Int
        :param cart_id: id cart

        :type product: Product
        :param product: the product to add to cart

        :returns True or False. If the caller receives False, it should wait and then try again
        """
        for k in self.producers_dict:
            if product in self.producers_dict[k]:
                self.carts_dict[cart_id].append(product)
                self.cart_items[k].append(product)
                self.producers_dict[k].remove(product) 
                log.info("A product has been added in cart with id: %d", cart_id)      
                return True
        log.info("The product hasn't been founded in cart with id: %d", cart_id)      
        return False
    
    # The consumer will look for the product that needs to be removed in his cart, the 
    # product will be sent back in his publisher's list and will be removed from the cart
    def remove_from_cart(self, cart_id, product):
        """
        Removes a product from cart.

        :type cart_id: Int
        :param cart_id: id cart

        :type product: Product
        :param product: the product to remove from cart
        """
        for k in self.producers_dict:
            if product in self.producers_dict[k]:
                self.producers_dict[k].append(product)
                self.carts_dict[cart_id].remove(product)
                log.info("A product has been removed from cart with id: %d", cart_id)
                return True
        return False
        
    # For each product in the consumer's cart will print the products that has been bought
    def place_order(self, cart_id):
        """
        Return a list with all the products in the cart.

        :type cart_id: Int
        :param cart_id: id cart
        """
        with self.place_order_lock:
            aux_cart = self.carts_dict.get(cart_id)
            for product in aux_cart:
                print(f"{current_thread().name} bought {product}")
            log.info("The order with id: %d had been placed", cart_id)
            return aux_cart

if __name__ == '__main__':
    unittest.main()