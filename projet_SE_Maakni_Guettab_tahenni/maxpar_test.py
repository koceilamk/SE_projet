import random
import time
import unittest

from maxpar import Task, TaskSystem, TaskNotFoundError, DuplicateTaskError, NoTaskPrecedenceError, \
    InvalidPrecedenceDictError

X: int = 0
Y: int = 0
Z: int = 0


class TestTaskSystem(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        global X, Y, Z

        def mock_task_t1():
            global X
            time.sleep(random.random() / 10)
            X = 5

        def mock_task_t2():
            global Y
            time.sleep(random.random() / 10)
            Y = 3

        def mock_task_t3():
            global X, Y
            time.sleep(random.random() / 10)
            X *= X
            Y *= Y

        def mock_task_t4():
            global Z
            time.sleep(random.random() / 10)
            Z = 10

        def mock_task_t5():
            global X, Y, Z
            time.sleep(random.random() / 10)
            Z += X + Y

        def mock_task_t6():
            global Z
            time.sleep(random.random() / 10)
            Z += 5

        t1 = Task(name='T1', reads=[], writes=['X'], run=mock_task_t1)
        t2 = Task(name='T2', reads=[], writes=['Y'], run=mock_task_t2)
        t3 = Task(name='T3', reads=['X', 'Y'], writes=['Y', 'X'], run=mock_task_t3)
        t4 = Task(name='T4', reads=[], writes=['Z'], run=mock_task_t4)
        t5 = Task(name='T5', reads=['X', 'Y', 'Z'], writes=['Z'], run=mock_task_t5)
        t6 = Task(name='T6', reads=['Z'], writes=['Z'], run=mock_task_t6)

        cls.tasks = [t1, t2, t3, t4, t5, t6]
        cls.precedence = {
            'T1': [],
            'T2': [],
            'T3': ['T1', 'T2'],
            'T4': [],
            'T5': ['T3', 'T4'],
            'T6': ['T5']
        }

    def test_constructor(self):
        """Should set tasks and precedence dictionary properly"""
        ts = TaskSystem(self.tasks, self.precedence)
        self.assertDictEqual(ts.precedence, self.precedence)
        self.assertDictEqual(ts.tasks, {task.name: task for task in self.tasks})

    def test_task_not_found_exception(self):
        """Should raise TaskNotFoundError"""
        with self.assertRaises(TaskNotFoundError):
            TaskSystem(self.tasks[:-1], self.precedence)

    def test_duplicate_task_exception(self):
        """Should raise DuplicateTaskError"""
        with self.assertRaises(DuplicateTaskError):
            TaskSystem([*self.tasks, self.tasks[0]], self.precedence)

    def test_no_task_precedence_exception(self):
        """Should raise NoTaskPrecedenceError"""
        precedence = self.precedence.copy()
        precedence.pop(self.tasks[0].name)
        with self.assertRaises(NoTaskPrecedenceError):
            TaskSystem(self.tasks, precedence)

    def test_invalid_precedence_exception(self):
        """Should raise InvalidPrecedenceDictError"""
        precedence = self.precedence.copy()
        precedence['T1'] = ['T1']
        with self.assertRaises(InvalidPrecedenceDictError):
            TaskSystem(self.tasks, precedence)

    def test_cycle_exception(self):
        """Should raise InvalidPrecedenceDictError"""
        precedence = self.precedence.copy()
        precedence['T1'] = ['T2']
        precedence['T2'] = ['T3']
        precedence['T3'] = ['T1']

        with self.assertRaises(InvalidPrecedenceDictError):
            TaskSystem(self.tasks, precedence)

    def test_get_dependencies(self):
        """Should get dependencies of a Task properly"""
        ts = TaskSystem(self.tasks, self.precedence)

        self.assertListEqual(ts.get_dependencies('T1'), [])
        self.assertListEqual(ts.get_dependencies('T2'), [])
        self.assertListEqual(ts.get_dependencies('T3'), ['T1', 'T2'])
        self.assertListEqual(ts.get_dependencies('T4'), [])
        self.assertListEqual(ts.get_dependencies('T5'), ['T1', 'T2', 'T3', 'T4'])
        self.assertListEqual(ts.get_dependencies('T6'), ['T1', 'T2', 'T3', 'T4', 'T5'])

    def test_get_dependencies_of_unknown_task(self):
        """Should raise TaskNotFoundError"""
        ts = TaskSystem(self.tasks, self.precedence)

        with self.assertRaises(TaskNotFoundError):
            ts.get_dependencies('unknown_task')

    def test_run_seq(self):
        """Should run the task system properly"""
        ts = TaskSystem(self.tasks, self.precedence)
        ts.set_logging(False)
        ts.run_seq()

        self.assertEqual(Z, 49)

    def test_run(self):
        """Should run the task system using maximum parallelism properly"""
        ts = TaskSystem(self.tasks, self.precedence)
        ts.set_logging(False)
        ts.run()

        self.assertEqual(Z, 49)


if __name__ == '__main__':
    unittest.main()
