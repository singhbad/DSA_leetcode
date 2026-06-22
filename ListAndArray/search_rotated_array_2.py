def searchRotatedArray2(nums: list[int], target: int) -> bool:
    n = len(nums)
    low = 0
    high = n - 1

    while low <= high:
        mid = ( low + high ) // 2
        if nums[mid] == target:
            return True

        if nums[mid] == nums[high]:
            high -= 1

        elif nums[mid] < nums[high]:
            if nums[mid] <= target <= nums[high]:
                low = mid + 1
            else:
                high = mid - 1

        else:
            if nums[low] <= target <= nums[mid]:
                high = mid - 1
            else:
                low = mid + 1

    return False


nums = [1,1,1,1,1,1,1,1,1,13,1,1,1,1,1,1,1,1,1,1,1,1]
target = 13

choice = searchRotatedArray2(nums, target)

print(choice)