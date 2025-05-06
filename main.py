import pandas as pd

def load_akbnk_data() -> pd.DataFrame:
    return pd.read_csv("AKBNK.E.csv")



def process_side(side):
    if side == 'S':
        # Min heap processing
        if price in min_price_to_queue:
            # Add the order to the existing queue
            min_price_to_queue[price].append(order)
        else:
            # Create a new queue and add the order
            new_queue = [order]
            min_price_to_queue[price] = new_queue
            # Add the new queue to the min heap
            heapq.heappush(min_price_heap, (price, new_queue))
    
    elif side == 'B':
        # Max heap processing
        if price in max_price_to_queue:
            # Add the order to the existing queue
            max_price_to_queue[price].append(order)
        else:
            # Create a new queue and add the order
            new_queue = [order]
            max_price_to_queue[price] = new_queue
            # Add the new queue to the max heap (using negative _price to simulate max heap)
            heapq.heappush(max_price_heap, (price, new_queue))
