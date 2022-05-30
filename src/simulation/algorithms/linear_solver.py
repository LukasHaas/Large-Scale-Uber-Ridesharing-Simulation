import cvxpy as cp
import numpy as np
from src.utils.timing import timing

class LinearSolver(object):
    @timing
    def solve_matching(self, matrix: np.ndarray, minimize: bool=True) -> np.ndarray:
        """_summary_

        Args:
            matrix (np.ndarray): _description_
            minimize (bool, optional): _description_. Defaults to True.

        Returns:
            np.ndarray: assignment
        """
        X = cp.Variable(shape=matrix.shape, name='X', boolean=True)
        action = cp.Minimize if minimize else cp.Maximize
        objective = action(cp.sum(cp.multiply(X, matrix)))

        min_axis = np.argmin(matrix.shape)
        constraints = [
            cp.sum(X, axis=1-min_axis) >= 1,
            cp.sum(X, axis=min_axis) <= 1,
            X >= 0
        ]
        
        lp = cp.Problem(objective, constraints)
        _ = lp.solve()
        return X.value

if __name__ == '__main__':
    lp_solver = LinearSolver()
    test_matrix = np.random.rand(4, 6)
    print(test_matrix)
    assignment = lp_solver.solve_matching(test_matrix)
    print(assignment)
    print(test_matrix[assignment.astype(bool)])