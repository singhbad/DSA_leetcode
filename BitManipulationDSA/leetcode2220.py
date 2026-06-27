class Solution:
    def minBitFlips(self, start: int, goal: int) -> int:
        ans = start ^ goal
        count = 0

        while ans:
            ans &= (ans - 1)
            count += 1

        return count