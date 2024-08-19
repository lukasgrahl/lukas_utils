import pickle
import pandas as pd
import os

def check_path_existence(path: str):
    if not os.path.exists(path):
        raise FileNotFoundError(f"{path} does not exist!")

def get_file_type(file_name: str):
    return file_name.split('.')[-1]

def load_df(file_name, file_path: str = None, **kwargs):
    """
    Load pd.DataFrame from different file types
    """
    file_type = get_file_type(file_name)

    if file_path is None:
        file_path = ""

    path = os.path.join(file_path, file_name)
    check_path_existence(file_path)

    if file_type == 'csv':
        return pd.read_csv(path, **kwargs)
    elif file_type == 'xlsx':
        return pd.read_excel(path, **kwargs)
    elif file_type == 'feather':
        df = pd.read_feather(path, **kwargs)
        return df
    else:
        raise KeyError(f"{file_type} unknown")


def save_df(df: pd.DataFrame, file_name: str, file_path: str = None):
    """
    Saves pd.DataFrame to different file types
    """
    if file_path is None:
        file_path = ""
    else:
        check_path_existence(file_path)

    file_type = get_file_type(file_name)

    if file_type == "csv":
        df.to_csv(os.path.join(file_path, file_name))
    elif file_type == "feather":
        df.to_feather(os.path.join(file_path, file_name))
    elif file_type == "xlsx":
        df.to_excel(os.path.join(file_path, file_name))
    else:
        raise KeyError(f"{file_type} unknown")


def save_pkl(file: dict, file_name: str, file_path: str = None):
    if file_path is None:
        f_path = ""
    else:
        check_path_existence(file_path)
    t = open(os.path.join(f_path, f"{file_name}"), "wb+")
    pickle.dump(file, t)
    t.close()
    pass


def load_pkl(file_name: str, file_path: str = None) -> dict:
    """

    @rtype: dict
    """
    if file_path is None:
        file_path = ""
    else:
         check_path_existence(file_path)

    t = open(os.path.join(file_path, file_name), 'rb')
    file = pickle.load(t)
    t.close()
    return file