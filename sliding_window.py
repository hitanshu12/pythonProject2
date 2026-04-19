# Sliding Window 3 sum

seq2 = [1, 2, 3, 4, 5, 6, 6, 7, 1]

# number of fixed item in the array/string
w = 3

# Step 1: find the sum of first {w} array num
sum_window = sum(seq2[:w])
max_sum = sum_window

# step 2 ittreate the element of array
for i in range(w, len(seq2)-1):
    sum_window += seq2[i] - seq2[i -w]
    max_sum = max(max_sum, sum_window)
    print(f'sum_window: {max_sum}')


print()
# silding window 4 element

print("silding window 4 element")
seq2 = [1, 2, 3, 4, 5, 6, 6, 7, 1]

k = 3

sum_w = sum(seq2[:k])
max_w = sum_w
print(max_sum)

for i in range(k, len(seq2)-1):
    sum_w += seq2[i] - seq2[i - k]
    max_w = max(max_w, sum_w)
    print(f'sum_window: {max_w}')

