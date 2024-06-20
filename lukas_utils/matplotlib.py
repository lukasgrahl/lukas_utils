import numpy as np
import matplotlib.pyplot as plt
from matplotlib.pyplot import cm
import pandas as pd

def get_fig_subplots(n_plots: int = 1, n_cols: int = 1, figsize: tuple = None, **kwargs):
    if figsize is None:
        figsize = tuple(plt.rcParams["figure.figsize"])
    n_rows = int(np.ceil(n_plots/n_cols))
    fig, ax = plt.subplots(n_rows, n_cols, figsize=(figsize[0] * n_rows, figsize[1] * n_cols), **kwargs)
    if n_plots == 1 and n_cols == 1:
        return fig, ax 
    else:
        ax = ax.ravel()[:n_plots]
    return fig, ax

def plt_stacked_bar(df, figsize: tuple =(20, 6), **kwargs):
    assert type(df) == pd.DataFrame, f"df is not type DataFrame, is type {type(df)}"

    bottom = np.zeros(df.shape[0])
    dict_df = {k: np.array(list(v.values())) for k, v in df.to_dict().items()}
    color = cm.rainbow(np.linspace(0, 1, len(dict_df)))

    fig, ax = plt.subplots(figsize=figsize)
    for i, _ in enumerate(dict_df.items()):
        l, w = _

        p = ax.bar(list(range(df.shape[0])), w, label=l, bottom=bottom, color=color[i], **kwargs)
        bottom += w

    return fig, ax