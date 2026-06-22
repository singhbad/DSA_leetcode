#Brute Force Solution

def rotate_array(matrix: list[int]):
    n = len(matrix)

    for i in range(n-1):
        for j in range(i+1, n):
            matrix[i][j],matrix[j][i] = matrix[j][i],matrix[i][j]

    for i in range(n):
        matrix[i].reverse()

    for row in matrix:
        print(row)


matrix = [[1,2,3,4],
          [5,6,7,8],
          [9,10,11,12],
          [13,14,15,16]
          ]

result = rotate_array(matrix)

