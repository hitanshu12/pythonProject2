
# Two Pointer sum
arr = [1,2,3,4,5,6,7]
target = 8

left = 0
right = len(arr) - 1

while left < right:
    current_sum = arr[left] + arr[right]
    if current_sum == target:
        print(arr[left], arr[right])
        left += 1
        right -= 1
    elif current_sum < target:
        left = left + 1
    else:
        right = right -1


# Maximum sum sub array Problem

a = [3, 8, 2, 5, 7, 6, 12]
sub_arr = 4

# Step 1: calculate first window sum
current_sum = sum(a[:sub_arr])
print(f'current sum: {current_sum}')

maxx = current_sum

# Step 2: slide the window
for i in range(1, len(a) - sub_arr + 1):
    current_sum = current_sum - a[i - 1] + a[i + sub_arr - 1]

    if current_sum > maxx:
        maxx = current_sum

    print(f'loop Maxx: {maxx}')

print(f'Maxx: {maxx}')

# K largest Number
num = [32, 45, 67, 34, 87, 23]
print(sorted(num, reverse=True))
print(sorted(num, reverse=True)[:1])


# two sum even numbers from array
seq = [1, 2, 3, 4, 5, 6, 6, 7, 1]

for i in range(0, len(seq)-1):

    l = i
    r = i + 1

    twoSum = seq[l] + seq[r]
    if twoSum % 2 == 0:
        print(seq[l], seq[r])


# three sum from array
seq1 = [1, 2, 3, 4, 5, 6, 6, 7, 1]
nl = []
for i in range(len(seq1)-1):

    l = i
    r = i + 2

    threeSum = seq1[l] + seq1[r]
    print(f'threeSum: {threeSum}')
    nl.append(threeSum)

print(nl)












