#Brute Force Solution

def rotate_array(matrix: list[int]):
    n = len(matrix)

    result = [[0 for _ in range(n)] for _ in range(n)]

    for i in range(n):
        for j in range(n):

            result[j][ (n-1) - i ] = matrix[i][j]

    return result


matrix = [[1,2,3,4],
          [5,6,7,8],
          [9,10,11,12],
          [13,14,15,16]
          ]

result = rotate_array(matrix)

for row in result:
    print(row)