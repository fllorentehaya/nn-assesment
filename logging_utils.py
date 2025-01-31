import logging


logger= logging.getLogger(__name__)
logging.basicConfig(format='%(asctime)s %(message)s',level=logging.DEBUG)

def logging_enrich(func):
        def wrapper(*args, **kwargs):
            logger.info(f"{func.__name__}  started")
            logger.info(f"{func.__name__}  params {args} {kwargs}")
            
            result=func(*args,**kwargs)
            logging.info(f"{func.__name__}  result {result}")
            logging.info(func.__name__ + " finished")
            return result
        return wrapper

