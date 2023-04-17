import time
from maxpar import Task, TaskSystem

X: int = 0
Y: int = 0
Z: int = 0


def run_t1():
    global X
    time.sleep(5)
    X = 5


def run_t2():
    global Y
    time.sleep(5)
    Y = 3


def run_t3():
    global X, Y
    time.sleep(3)
    X *= X
    Y *= Y


def run_t4():
    global Z
    time.sleep(10)
    Z = 10


def run_t5():
    global X, Y, Z
    time.sleep(2)
    Z += X + Y


def run_t6():
    global Z
    time.sleep(2)
    Z += 5
    print("Result =", Z)


if __name__ == '__main__':
    T1 = Task(name='T1', reads=[], writes=['X'], run=run_t1)
    T2 = Task(name='T2', reads=[], writes=['Y'], run=run_t2)
    T3 = Task(name='T3', reads=['X', 'Y'], writes=['Y', 'X'], run=run_t3)
    T4 = Task(name='T4', reads=[], writes=['Z'], run=run_t4)
    T5 = Task(name='T5', reads=['X', 'Y', 'Z'], writes=['Z'], run=run_t5)
    T6 = Task(name='T6', reads=['Z'], writes=['Z'], run=run_t6)

    ts = TaskSystem([T5, T3, T6, T4, T2, T1], {
        'T1': [],
        'T2': [],
        'T4': [],
        'T3': ['T1', 'T2'],
        'T5': ['T3', 'T4'],
        'T6': ['T5']
    })

    ts.par_cost()

    ts.visualize()
