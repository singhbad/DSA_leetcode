def exists_subsequence_sum_k(arr, k):

    def dfs(index, current_sum):

        # Base case
        if index == len(arr):
            return current_sum == k

        # Take
        if dfs(index + 1, current_sum + arr[index]):
            return True

        # Don't take
        if dfs(index + 1, current_sum):
            return True

        return False

    return dfs(0, 0)


arr = [1, 2, 1]
k = 2

print(exists_subsequence_sum_k(arr, k))