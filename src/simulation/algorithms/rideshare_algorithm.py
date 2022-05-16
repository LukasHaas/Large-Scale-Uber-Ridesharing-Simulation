from abc import ABC, abstractmethod

class RideShareMatchingAlgorithm(ABC):
    """Abstract class for a ridesharing matching algorithm.
    """
    @staticmethod
    def is_match_possible(requests, drivers):
        if len(requests) == 0 or len(drivers) == 0:
            return False
        
        return True
     
    @abstractmethod
    def create_matches(self, time, new_item, requests, drivers):
        # Should return a 3-tuple consisting of:
        #    - a list of tuples of matches
        #    - a list of requests which were umatched or should remain in the queue
        #    - a list of drivers which were unmatched or should remain in the queue
        pass