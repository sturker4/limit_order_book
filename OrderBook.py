import pandas as pd
import heapq
from enum import Enum

BuySellEnum = Enum('BuySellEnum', ['BUY', 'SELL'])

class LimitOrderBook:
    """
    Consists of 2 AVL trees, one for bidside and one for askside. Keeps track of best 3 ask and bid orders.
    Each node of these trees is a doubly linked list.
    """
    def __init__(self):
        self.bids = LimitLevelHeap()
        self.asks = LimitLevelTree()
        self.best_bid = None
        self.best_ask = None
        self._price_levels = {}  # maps (price, side) to LimitLevel
        self._orders = {}  # maps order_id to Order object, allows O(1) cancellation of order in queue
        self._data = pd.read_csv('AKBNK.E.csv')

    def price_match(self, order1, order2):
        if order1.side == BuySellEnum.BUY:
            return order1.price >= order2.price
        else:
            return order1.price <= order2.price
        
    def place_order(self, order):
        opposite_side = self.asks if order.side == BuySellEnum.BUY else self.bids
        same_side = self.bids if order.side == BuySellEnum.BUY else self.asks
        self._orders[order.order_id] = order

        while order.qty > 0 and not opposite_side.is_empty and self.price_match(opposite_side.peek().peek(), order):
            opposite_order = opposite_side.peek().peek()
            trade_price = opposite_order.price
            trade_volume = min(opposite_order.qty, order.qty)

            opposite_order.qty -= trade_volume
            order.qty -= trade_volume
    
            if opposite_order.qty == 0:
                self.cancel_order(opposite_order)
        if order.qty > 0:
            self.add_order(order, same_side)
    
    def add_order(self, order, same_side):
        if (order.price, order.side) not in self._price_levels:
            limit_level = LimitLevel(order)
            self._price_levels[(order.price, order.side)] = limit_level
            same_side.insert(limit_level)
            if order.side == BuySellEnum.BUY:
                if self.best_bid is None or limit_level.price > self.best_bid.price:
                    self.best_bid = limit_level
            else:
                if self.best_ask is None or limit_level.price < self.best_ask.price:
                    self.best_ask = limit_level
        else:
            self._price_levels[(order.price, order.side)].append(order)
    
    def cancel_order(self, order):
        if order.order_id in self._orders:
            order = self._orders[order.order_id]
            price_level = self._price_levels[(order.price, order.side)]
            price_level.orders.remove(order)
            if len(price_level.orders) == 0:
                same_side = self.bids if order.side == BuySellEnum.BUY else self.asks
                price_level.remove()
                del self._price_levels[(order.price, order.side)]
            del self._orders[order.order_id]

class LimitLevel:
    """AVL BST node.

    This Binary Tree implementation balances on each insert.

    Attributes:
        parent: Parent node of this Node
        is_root: Boolean, to determine if this Node is root
        left_child: Left child of this Node; Values smaller than price
        right_child: Right child of this Node; Values greater than price
    """
    __slots__ = ['price', 'size', 'parent', 'left_child', 'right_child', 'orders']

    def __init__(self, order):
        """Initialize a Node() instance.

        :param order:
        """
        # Data Values
        self.price = order.price
        self.size = order.qty

        # BST Attributes
        self.parent = None
        self.left_child = None
        self.right_child = None

        # Doubly-Linked-list attributes
        self.orders = OrderList(self)
        self.append(order)

    @property
    def is_root(self):
        return isinstance(self.parent, LimitLevelTree)

    @property
    def balance_factor(self):
        """Calculate and return the balance of this Node.

        :return:
        """
        right_height = self.right_child.height if self.right_child else 0
        left_height = self.left_child.height if self.left_child else 0
        return right_height - left_height

    @property
    def grandpa(self):
        try:
            if self.parent:
                return self.parent.parent
            else:
                return None
        except AttributeError:
            return None

    @property
    def height(self):
        """Calculates the height of the tree up to this Node.

        :return: int, max height among children.
        """
        left_height = self.left_child.height if self.left_child else 0
        right_height = self.right_child.height if self.right_child else 0
        if left_height > right_height:
            return left_height + 1
        else:
            return right_height + 1

    @property
    def min(self):
        """Returns the smallest node under this node.

        :return:
        """
        minimum = self
        while minimum.left_child:
            minimum = minimum.left_child
        return minimum

    def append(self, order):
        """Wrapper function to make appending to Order List simpler.

        :param order: Order() Instance
        :return:
        """
        return self.orders.append(order)

    def _replace_node_in_parent(self, new_value=None):
        """Replaces Node in parent on a delete() call.

        :param new_value: LimitLevel() instance
        :return:
        """
        if not self.is_root:
            if self == self.parent.left_child:
                self.parent.left_child = new_value
            else:
                self.parent.right_child = new_value
        if new_value:
            new_value.parent = self.parent

    def remove(self):
        """Deletes this limit level.

        :return:
        """

        if self.left_child and self.right_child:
            # We have two kids
            succ = self.right_child.min

            # Swap Successor and current node
            self.left_child, succ.left_child = succ.left_child, self.left_child
            self.right_child, succ.right_child = succ.right_child, self.right_child
            self.parent, succ.parent = succ.parent, self.parent
            self.remove()
            self.balance_grandpa()
        elif self.left_child:
            # Only left child
            self._replace_node_in_parent(self.left_child)
        elif self.right_child:
            # Only right child
            self._replace_node_in_parent(self.right_child)
        else:
            # No children
            self._replace_node_in_parent(None)

    def balance_grandpa(self):
        """Checks if our grandparent needs rebalancing.

        :return:
        """
        if self.grandpa and self.grandpa.is_root:
            pass
        elif self.grandpa and not self.grandpa.is_root:
            self.grandpa.balance()
        elif self.grandpa is None:
            pass

    def balance(self):
        if self.balance_factor > 1:
            # right is heavier
            if self.right_child.balance_factor < 0:
                # right_child.left is heavier, RL case
                self._rl_case()
            elif self.right_child.balance_factor > 0:
                # right_child.right is heavier, RR case
                self.right_right()
        elif self.balance_factor < -1:
            # left is heavier
            if self.left_child.balance_factor < 0:
                # left_child.left is heavier, LL case
                self.left_left()
            elif self.left_child.balance_factor > 0:
                # left_child.right is heavier, LR case
                self.left_right()
        else:
            # Everything's fine.
            pass

        # Now check upwards
        if not self.is_root and not self.parent.is_root:
            self.parent.balance()

    def left_left(self):
        """
        Left-left rotation
        """
        child = self.left_child

        if self.parent.is_root or self.price > self.parent.price:
            self.parent.right_child = child
        else:
            self.parent.left_child = child

        child.parent, self.parent = self.parent, child
        child.right_child, self.left_child = self, child.right_child

    def right_right(self):
        """
        Right-right rotation
        """
        child = self.right_child

        if self.parent.is_root or self.price > self.parent.price:
            self.parent.right_child = child
        else:
            self.parent.left_child = child

        child.parent, self.parent = self.parent, child
        child.left_child, self.right_child = self, child.left_child

    def left_right(self):
        """
        Left-right rotation
        """
        child, grand_child = self.left_child, self.left_child.right_child
        child.parent, grand_child.parent = grand_child, self
        child.right_child = grand_child.left_child
        self.left_child, grand_child.left_child = grand_child, child
        self.left_left()

    def right_right(self):
        """
        Right-left rotation
        """
        child, grand_child = self.right_child, self.right_child.left_child
        child.parent, grand_child.parent = grand_child, self
        child.left_child = grand_child.right_child
        self.right_child, grand_child.right_child = grand_child, child
        self.right_right()