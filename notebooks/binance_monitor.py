from unicorn_binance_websocket_api.manager import BinanceWebSocketApiManager
import threading
import time
import json
from abc import ABC, abstractmethod

class UserEventHandler(ABC): 
    """ Clase abstracta a extender por usuario
    """
    @abstractmethod
    def price_changed_event(self, symbol, timestamp, price):
        """ Evento de cambio del precio de un símbolo.
        param symbol: nombre del símbolo, por ejemplo BTCUSDT
        param timestamp: estampa de tiempo en milisegundos UTC (formato Binance)
        param price: precio, por ejemplo, U$S para BTCUSDT.
        """
        pass    

def _is_empty_message(message):
    if message is False:
        return True
    if '"result":null' in message:
        return True
    if '"result":None' in message:
        return True
    return False



class BinanceAssetPriceMonitor:
    """
    """
    
    def __init__(self, api_key,secret_key, symbols,user_event_handler: UserEventHandler):
        """
        
        """
        self.base_endpoint = 'https://api.binance.us'  #  NOTE: This may be different for your exchange. Check the docs
        self.api_key = api_key
        self.secret_key = secret_key
        self.channels = {'trade', }
        self.binance_us_websocket_api_manager = BinanceWebSocketApiManager(exchange="binance.us")
        self.lc_symbols = []        
        for symbol in symbols:
            self.lc_symbols.append(symbol.lower())
        self.user_event_handler = user_event_handler
        
    def start(self):
        """
        """
        self.curr_stream_id = self.binance_us_websocket_api_manager.create_stream(
            self.channels, 
            markets=self.lc_symbols, 
            api_key=self.api_key, api_secret=self.secret_key)

        # Start a worker process to move the received stream_data from the stream_buffer to a print function
        self.worker_thread = threading.Thread(
            target=self.process_stream_data)
        self.worker_thread.start()
        
    def stop(self):
        """
        """
        self.binance_us_websocket_api_manager.stop_manager_with_all_streams()
        self.worker_thread.join()       
                                                                  
    def process_stream_data(self):
        """
        """
        self.keep_running = True
        while self.keep_running:
            if self.binance_us_websocket_api_manager.is_manager_stopping():
                self.keep_running = False
                continue
        
            oldest_data = self.binance_us_websocket_api_manager.pop_stream_data_from_stream_buffer()
            is_empty = _is_empty_message(oldest_data)
            if is_empty:
                time.sleep(0.01)
            else:
                oldest_data_dict = json.loads(oldest_data)
                data = oldest_data_dict['data']

                #  Handle price change
                self.user_event_handler.price_changed_event(symbol=data['s'], timestamp=data['T'], price=float(data['p']))