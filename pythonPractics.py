# Write a program to swap two numbers without using a third variable.

a = 4
b = 6

a, b = b, a

print(a,b)

# with third variable
c = 4
d = 6
temp = c
c = d
d = temp

print(a, b)

# Convert a temperature from Celsius to Fahrenheit.

#c = int(input("Enter celsius: "))

def convertFehrenheight(celsius):
    f = (celsius * (9/5)) + 32
    print(f)

convertFehrenheight(10)

# Count how many vowels are present in a given string.

string = "Hello World Python Engineering"
vowels = "aeiouAEIOU"

count = 0

for i in string:
    if i in vowels:
        count = count + 1
    else:
        pass

print(count)

# Check if a string is a palindrome

s = "madam"

if s == s[::-1]:
    print("palindrome")
else:
    print("palindrome")


# Find the largest and smallest number in a list without using max() or min().

numbers = [12, 5, 23, 1, 89, 34]

smallest = numbers[0]
largest = numbers[0]

for i in numbers:
    if i < smallest:
        smallest = i
    if i > largest:
        largest = i

print("Smallest number:", smallest)
print("Largest number:", largest)


# Remove duplicates from a list.

lsi1 = [1,2,3,4,2,3,1]

lisSet = set(lsi1)

print(lisSet)

lis2 = list(lisSet)
print(lis2)

# method 2

lsi3 = [1,2,3,4,2,3,1]

lisE = []

for i in lsi3:
    if i not in lisE:
        lisE.append(i)

print(lisE)

# Print a pattern like:


for i in range(4):
    for j in range(i):
        print("*", end="")
    print()


# count how many times names appear in the list
from collections import Counter
list1 = ['John','Kelly', 'Peter', 'Moses', 'Peter', 'Moses', 'John','Kelly']

nameList = []

count_dict = Counter(list1)
print(count_dict)


# Count one specific name

count = 0
for c in list1:
    if c == "Kelly":
        count += 1

print(count)


listl = ['John','Kelly', 'Peter', 'Moses', 'Peter', 'Moses', 'John','Kelly']

counts = {}

for item in listl:
    if item in counts:
        counts[item] += 1
    else:
        counts[item] = 1

print(counts)

#================================================================================

import sys
import keyword
import operator
from datetime import datetime
import os

# Square of each numbers

numbers = [1, 2, 3, 4, 5]
numList = []
for i in numbers:
    sqr = i ** 2
    print("Square of:", i, "is:", sqr)
    numList.append(sqr)

print(numList)

# Calculate the average of list of numbers

numbers = [10, 20, 30, 40, 50]

sum = 0

for i in numbers:
    sum = sum + i

avgNum = sum/len(numbers)

print(avgNum)

#  Print all even and odd numbers
even =[]
odd = []

for i in range(1,11):
    if i % 2 == 0:
        even.append(i)
    else:
        odd.append(i)

print(f"Even number list is {even}")
print(f"Odd number list is {odd}")

print("Reverse numbers using for loop")

for i in range (5, -1, -1):
    print(i)


# Reverse a list using a loop
print("Reverse a list using a loop")
numbers = [1, 2, 3, 4]
for i in numbers[::-1]:
    print(i)

"""
Nested for loop to print the following pattern
*
* *
* * *
* * * *
* * * * *
"""

for i in range(5):
    for j in range(1, i+1):
        print("*", end=" ")
    print(' ')


# Printing the elements of the list with its index number using the range() function4

numbers = [10, 20, 30, 40, 50]

size = len(numbers)

for i in range(size):
    print(f"Index is {i} and Value is {numbers[i]}")



# Remove sequential duplicate from list
Lst = [1,2,3,3,3,3,4,5,3,5,6,7,6,8]
seqList = [Lst[0]]

for i in range(1, len(Lst)):
    if Lst[i] != Lst[i - 1]:
        print("prev")
        seqList.append(Lst[i])

print(seqList)


# Find largest and smallest digit in a number
num1 = 9876543210

numstr = str(num1)
largest_digit = int(numstr[0])
smallest_digit = int(numstr[0])

for i in numstr[1:]:
    str_i = int(i)
    if str_i > largest_digit:
        largest_digit = str_i
    if str_i < smallest_digit:
        smallest_digit = str_i


print(f"Largets Digit number is {largest_digit}")
print(f"Smallest Digit number is {smallest_digit}")


# Flatten a nested list using loops
print("Flatten a nested list using loops")
nested_list = [1, [2, 3], [4, 5, 6], 7, [8, 9]]
flat_lis = []
for i in nested_list:
    if isinstance(i, list): # check if the i is the list
        for item in i:
            flat_lis.append(item)
    else:
        flat_lis.append(i)

print(flat_lis)

"""
Print the alternate numbers pattern
1  
2 3  
4 5 6  
7 8 9 10  
11 12 13 14 15
"""
num = 1
for i in range(1,6):
    if i % 2 != 0:
        for j in range(num, num + i):
            print(j, end=" ")
        print(" ")
    else:
        for k in range(num + i - 1, num -1, -1):
            print(k, end=" ")
        print(" ")
    num += 1




































