import pandas as pd
import heapq
from enum import Enum

BuySellEnum = Enum('BuySellEnum', ['BUY', 'SELL'])


class LimitOrderBook:
    """
    Consists of 2 BST trees, one for bidside and one for askside. Keeps track of best 3 ask and bid orders.
    Each node of these trees are doubly linked 

    """
    def __init__(self):
        self.bids = LimitLevelRoot()
        self.asks = LimitLevelRoot()
        self.best_3_bid = None
        self.best_3_ask = None
        self._volume = {} #maps (int price, Enum side) to int qty, allows finding volume in O(1) 
        self._orders = {} #maps int order_id to Order object, allows O(1) cancellation of order in queue
        self._queues = {} #maps (int price, Enum side) to queue, allows O(logn) deletion of queues
        self._data = pd.read_csv('AKBNK.E.csv')

    def price_match(order1, order2):
        if order1.side == BuySellEnum.BUY:
            return order1.price >= order2.price
        else:
            return order1.price <= order2.price
        
    def place_order(self, order):
        opposite_side = self.asks if order.side == BuySellEnum.BUY else self.bids
        same_side = self.bids if order.side == BuySellEnum.BUY else self.asks
        self._orders[order.order_id] = order

        while order.qty > 0 and not opposite_side.is_empty and self.price_match(opposite_side.peek().peek(), order): #fix
            opposite_order = opposite_side.peek().peek()
            trade_price = opposite_order.price
            trade_volume = min(opposite_order.qty, order.qty)

            opposite_order.qty -= trade_volume
            order.qty -= trade_volume
    
            if opposite_order.qty == 0:
                self.cancel_order(opposite_order)
        if order.qty > 0:
            self.add_order(order, same_side)
    
    def add_order(self, order):
        if not (order.price, order.side) in self._queues:
            self._queues[(order.price, order.side)] = self.create_dll(order) #fix
            self._volume[(order.price, order.side)] = order.qty
        else:
            self._queues[(order.price, order.side)].push(order) #fix
            self._volume[(order.price, order.side)] += order.qty

    def cancel_order(self, id): #implement
        if id in self._orders:
            order = self._orders[id]
            queue = self._queues[(order.price, order.side)]
            queue.remove(order)
            if queue.len == 0:
                same_side = self.bids if order.side == BuySellEnum.BUY else self.asks
                same_side.remove(queue) #fix
            self._volume[(order.price, order.side)] -= order.qty
            del self._orders(order.order_id)



        

    ##GetVolumeAtPrice


class OrderDLL:
def __init__(self, parent_limit):
        self.head = None
        self.tail = None
        self.count = 0
        self.parent_limit = parent_limit

    def __len__(self):
        return self.count

    def append(self, order):
        """Appends an order to this List.

        Same as LimitLevel append, except it automatically updates head and tail
        if it's the first order in this list.

        :param order:
        :return:
        """
        if not self.tail:
            order.root = self
            self.tail = order
            self.head = order
            self.count += 1
        else:
            self.tail.append(order)
class Order:
    def __init__(self, network_time, msg_type, side, price, qty, order_id, lob):
        self._network_time = network_time
        self._msg_type = msg_type
        self._side = side
        self._price = price
        self._qty = qty
        self._order_id = order_id
        self._lob = lob

    
    def __repr__(self):
        return f"Order(_qty={self._qty}, _price={self._price}, _network_time='{self._network_time}', type='{self._side}')" #fix





# Function to process a group of rows
def process_group(group, network_time, lob):
    for index, row in group.iterrows():
        # Access specific column values
        price = row['price']
        msg_type = row['msg_type']
        side = row['side']  
        qty = row['qty']
        order_id = row['order_id'] 

        # Create an Order object
        order = Order(network_time, msg_type, side, price, qty, order_id,)
        lob.place_order(order)
        

def main():
    lob = LimitOrderBook()

    # Group the DataFrame by the 'network_time' column
    grouped = lob._data.groupby('network_time')

    # Iterate over each group
    for nwork_time, group in grouped:
        process_group(group, nwork_time, lob)

if __name__ == "__main__":
    main()