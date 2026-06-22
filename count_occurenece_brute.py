def countOccu(nums: list[int], target: int) -> int:
    n = len(nums)
    count = 0

    for i in range(n):
        if nums[i] == target:
            count += 1

    return count

nums = [1,2,3,3,3,3,3,5,6,8,9,9,10]

target = 3

occuurence = countOccu(nums, target)

print(occuurence)