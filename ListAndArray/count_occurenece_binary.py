class Solution:

    def lower_bound(self, nums, target):
        n = len(nums)

        lb = n
        low = 0
        high = n - 1

        while low <= high:
            mid = (low + high) // 2

            if nums[mid] >= target:
                lb = mid
                high = mid - 1
            else:
                low = mid + 1

        return lb

    def upper_bound(self, nums, target):
        n = len(nums)

        ub = n
        low = 0
        high = n - 1

        while low <= high:
            mid = (low + high) // 2

            if nums[mid] > target:
                ub = mid
                high = mid - 1
            else:
                low = mid + 1

        return ub

    def count_occurrences(self, nums, target):

        first = self.lower_bound(nums, target)

        # target not present
        if first == len(nums) or nums[first] != target:
            return 0

        last = self.upper_bound(nums, target)

        return last - first


nums = [1,2,3,3,3,3,3,5,6,8,9,9,10,12,12,12]

target = 12

obj = Solution()

print(obj.count_occurrences(nums, target))