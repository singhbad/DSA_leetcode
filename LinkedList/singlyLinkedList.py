class Node:

    def __init__(self, val):
        self.val = val
        self.next = None

class SinglyLinkedList:

    def __init__(self):
        self.head = None

    def append(self, val):
        new_node = Node(val)

        if self.head == None:
            self.head = new_node

        else:
            curr = self.head
            while curr.next is not None:
                curr = curr.next
            curr.next = new_node

    def traverse(self):

        if self.head is None:
            print("SLL is Empty")

        else:
            curr = self.head
            while curr is not None:
                print(curr.val, end=" ")
                curr = curr.next
            print()

    def insert(self, val, pos): 
        new_node = Node(val)

        if pos == 0:
            new_node.next = self.head
            self.head = new_node
            return
        
        curr = self.head
        count = 0

        while curr is not None and count < pos - 1:
            curr = curr.next
            count += 1

        if curr is None:
            print("Invalid Position")
            return

        new_node.next = curr.next
        curr.next = new_node


    def delete(self, pos):
        
        if self.head is None:
            print("SLL is Empty")
            return
        #position
        
        if pos == 0:
            self.head = self.next.next
            return

        curr = self.head
        count = 0

        while curr is not None and count < pos - 1:
            curr = curr.next
            count += 1
    
        if curr is None or curr.next is None:
            print("Invalid Position")
            return 

        curr.next = curr.next.next


sll = SinglyLinkedList()

sll.append(10)
sll.append(20)
sll.append(30)
sll.append(40)
sll.append(1)
sll.insert(25,3)
sll.delete(4)
sll.traverse()