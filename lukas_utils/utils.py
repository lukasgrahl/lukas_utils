import argparse
import functools
import multiprocessing
from collections import namedtuple
import time
import numpy as np
from tqdm import tqdm
import os
from .helpers_logging import get_logger
from joblib import Parallel, delayed


from atpbar import flush, atpbar
import threading


MY_LOGGER = get_logger(os.path.basename(__file__))

ArgParseArgument = namedtuple(
    "ArgParseArgument", ["name", "type", "default", "help", "flag"], defaults=(None,)
)


def chunk_it(
    lst: list,
    int_chunk_size: int = None,
    int_chunk_split: int = None,
    is_chunk_left_inclusive: bool = False,
):
    assert (int_chunk_size is None) ^ (
        int_chunk_split is None
    ), "select either int_chunk_size OR int_chunk_split"

    if int_chunk_split is not None:
        int_chunk_size = int(np.ceil(len(lst) / int_chunk_split))

    if not isinstance(lst, list):
        lst = list(lst)
    lst_chunk = [
        lst[i : i + int_chunk_size] for i in range(0, len(lst), int_chunk_size)
    ]
    if is_chunk_left_inclusive:
        lst_chunk = [
            [lst_chunk[i - 1][-1]] + lst_chunk[i] for i in range(1, len(lst_chunk))
        ]
    return lst_chunk


def run_with_threading(func, lst_dct_args, n_process: int, desc: str = None):
    def task(f, lst_args, name):
        for a in atpbar(lst_args, name=name):
            f(a)

    lst_chunk_args = chunk_it(lst_dct_args, int_chunk_split=n_process)

    threads = []
    for i in range(n_process):
        if desc is not None:
            name = desc + f": {i}"
        else:
            name = f": {i}"
        t = threading.Thread(target=task, args=[func, lst_chunk_args[i], name])
        t.start()
        threads.append(t)
    for t in threads:
        t.join()
    flush()
    pass


def time_it(func):
    @functools.wraps(func)
    def wrapper(*args, **kwargs):
        start = time.time()
        res = func(*args, **kwargs)
        int_exec_time = time.time() - start
        MY_LOGGER.info(f"{func.__name__} executed in {int_exec_time:.2f} seconds")
        return res

    return wrapper


def run_paralle_dec(
    func,
    lst_dct_args=list[dict],
    n_process: int = 1,
    show_progress: bool = True,
    is_time_it: bool = False,
    desc: str = None,
    parallel_engine: str = "multiprocess_imap",
    mp_map_chunksize: int = 50,
    **kwargs,
):
    assert (
        parallel_engine
        in [
            "multiprocess_imap",
            "multiprocess_map",
            "joblib",
            "multithreading",
        ]
    ), f"{parallel_engine} must be one of : multiprocess_imap, multiprocess_map, joblib, multithreading"

    if desc is None:
        desc = ""
    desc = f"Parallel processing: {desc}"

    res = []
    mp_map_chunksize = min(
        int(np.ceil(len(lst_dct_args) / n_process)), mp_map_chunksize
    )

    # run one iteration outside of parallel processing to create tables etc
    count, is_error = 0, True
    pbar = tqdm(
        total=len(lst_dct_args),
        desc="Non-parallel processing until first non-error iterration",
    )
    while is_error:
        dct = lst_dct_args.pop(0)
        is_error = func(dct)

        # warn if function does not return boolean indicator
        if not isinstance(is_error, bool):
            MY_LOGGER.error(
                f"{func.__name__} did not return error boolean indicator: {is_error}"
            )

        count += 1

        # show progess bar if error occured in first run
        if count > 1:
            pbar.update(1)

    if count > 1:
        MY_LOGGER.info(
            f"First non-error run at iteration {count}, starting parallel processing now"
        )

    # start parallel processing
    obj_iter = lst_dct_args
    if show_progress and ((parallel_engine != "multithreading") or n_process == 1):
        obj_iter = tqdm(lst_dct_args, total=len(lst_dct_args), desc=desc)

    start = time.time()
    if n_process == 1:
        res = [func(arg) for arg in obj_iter]

    elif parallel_engine == "joblib":
        res = Parallel(n_jobs=n_process)(delayed(func)(d) for d in obj_iter)

    elif parallel_engine == "multiprocess_imap":
        res = []
        with multiprocessing.Pool(processes=n_process) as pool:
            for r in tqdm(
                pool.imap(
                    func,
                    lst_dct_args,
                    chunksize=mp_map_chunksize,
                ),
                total=len(lst_dct_args),
                **kwargs,
            ):
                res.append(r)
            pool.close()
            pool.join()

    elif parallel_engine == "multithreading":
        run_with_threading(func, obj_iter, n_process, desc)
        res = []

    elif parallel_engine == "multiprocess_map":
        res = []
        with multiprocessing.Pool(processes=n_process) as pool:
            for r in tqdm(
                pool.map(
                    func,
                    lst_dct_args,
                    chunksize=mp_map_chunksize,
                ),
                total=len(lst_dct_args),
                **kwargs,
            ):
                res.append(r)
            pool.close()
            pool.join()

    if is_time_it:
        MY_LOGGER.info(
            f"\n{func.__name__} executed in {time.time() - start:.2f} seconds"
        )
    return res


@time_it
def run_parallel_wrap(
    func, arguments: list, n_process: int = 4, show_progressbar: bool = True, **kwargs
):
    if n_process == 1:
        return [func(arg) for arg in arguments]

    else:
        pool = multiprocessing.Pool(processes=n_process)

        if show_progressbar:
            res = list(
                tqdm(
                    pool.imap_unordered(func, arguments), total=len(arguments), **kwargs
                )
            )
        else:
            res = list(pool.imap_unordered(func, arguments))

    return res


def obj_parse_n_process(
    lst_tup_args: list[ArgParseArgument] = [
        ArgParseArgument(
            name="n_process", type=int, default=1, help="Number of parallel processes"
        )
    ],
):
    OBJ_PARSER = argparse.ArgumentParser(description="TAQ data n_process")
    for a in lst_tup_args:
        if a.flag is not None:
            OBJ_PARSER.add_argument(
                f"-{a.flag}", f"--{a.name}", type=a.type, default=a.default, help=a.help
            )
        else:
            OBJ_PARSER.add_argument(
                f"--{a.name}", type=a.type, default=a.default, help=a.help
            )
    OBJ_PARSER_ARGS = OBJ_PARSER.parse_args()
    return OBJ_PARSER, OBJ_PARSER_ARGS
