import numpy as np

quantity, price, total = np.nan, 10, 100
quantity = total / price
price = total / quantity
total = price * quantity
print(quantity, price, total)









