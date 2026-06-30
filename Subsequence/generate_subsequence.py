def generate_subsequences(arr):
    n = len(arr)
    current = []

    def dfs(index):

        if index == n:
            print(current)
            return

        # Take
        current.append(arr[index])
        dfs(index + 1)

        # Undo
        current.pop()

        # Don't Take
        dfs(index + 1)

    dfs(3)


arr = [1, 2, 3]
generate_subsequences(arr)