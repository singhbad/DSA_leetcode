def set_zeroes(matrix):
    r = len(matrix)
    c = len(matrix[0])

    row_track = [ 0 for _ in range(r)]
    col_track = [ 0 for _ in range(c)]

    for i in range(r):
        for j in range(c):
            if matrix[i][j] == 0:
                row_track[i] = -1
                col_track[j] = -1

    for i in range(r):
        for j in range(c):
            if row_track[i] == -1 or col_track[j] == -1:
                matrix[i][j] = 0

    

matrix = [[7,10,29,3], 
          [1,20,0,4],
          [19,0,6,11],
          [4,27,14,7]]

set_zeroes(matrix)

for row in matrix:
    print(row)