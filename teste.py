min_qty = 1e-05
if isinstance(min_qty, float):
    decimal_places = abs(int(round(min_qty, 8)).as_integer_ratio()[1].bit_length() - 1)
else:
    decimal_places = len(str(min_qty).split('.')[1]) if '.' in str(min_qty) else 0

print(decimal_places)

