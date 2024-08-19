def get_dfbetas(X: np.array, resid: np.array):
    assert X.shape[0] == resid.shape[0], "X and resid do not correspond"
    lst_dfbetas = []

    H = X @ np.linalg.inv(X.T @ X) @ X.T
    H_diag = np.diagonal(H)
    for idx in range(X.shape[0]):
        x_i = X[idx]
        e_i = resid[idx]

        lst_dfbetas.append(
            (np.linalg.inv(X.T @ X) @ x_i[None].T @ e_i[None]) / (1 - H_diag[idx])
        )
    return np.array(lst_dfbetas)


def get_cooks_distance(X: np.array, resid: np.array, flt_largest_perc: float = 97.5):
    n, p = len(resid), np.linalg.matrix_rank(X)

    s_2 = resid.T @ resid / (n - p)
    H = X @ np.linalg.inv(X.T @ X) @ X.T
    H_diag = np.diagonal(H)

    lst_cooks_dist = []
    for idx in range(X.shape[0]):
        d_i = resid[idx] ** 2 / p * s_2 * (H_diag[idx] / (1 - H_diag[idx]) ** 2)
        lst_cooks_dist.append(d_i)
    arr_cook_dist = np.array(lst_cooks_dist)
    filt_percent = arr_cook_dist >= np.percentile(arr_cook_dist, flt_largest_perc)
    return arr_cook_dist, filt_percent