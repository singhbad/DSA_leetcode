def binary_search(nums: list[int], low, high, target: int) -> int:
    if low > high: 
        return -1
    
    mid  = (low + high) // 2
    if nums[mid] == target:
        return mid

    elif nums[mid] < target:
        return binary_search(nums, mid + 1, high, target)

    else:
        return binary_search(nums, low, mid - 1, target)


nums = [2,4,5,7,8,9,11,23,45,76]
n = len(nums)
target = 8
low = 0
high = n-1

result = binary_search(nums, low, high, target)
print(result)