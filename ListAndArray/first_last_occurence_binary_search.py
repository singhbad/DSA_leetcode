def lower_bound(nums: list[int], target) -> list[int]:
    n = len(nums)
    lb = -1
    low = 0
    high = n-1

    while low <= high:
        mid = ( low + high ) // 2
        if nums[mid] >= target:
            lb = mid
            high = mid - 1

        else:
            low = mid + 1

    return lb

def upper_bound(nums: list[int], target) -> int:
    n =len(nums)
    ub = -1
    low = 0
    high = n-1

    while low <= high:
        mid = ( low + high ) // 2
        if nums[mid] > target:
            ub = mid
            high = mid - 1

        else:
            low = mid + 1

    return ub



nums = [1,2,3,3,3,3,3,5,6,8,9,9,10]

target = 3

first = lower_bound(nums, target)
last = upper_bound(nums, target) - 1

print([first, last])