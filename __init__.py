# -*- encoding: utf-8 -*-

'''
@Time        :   2023/12/30 19:52:10
'''

import asyncio
import inspect
import traceback
import logging
from typing import Any, Awaitable

_STOP_ITEM = None
logger = logging.getLogger(__name__)


async def _producer(queue, task_items, args_func, args_kwargs=None):
    if args_func is not None:
        return await args_func(queue, **args_kwargs)
    for idx in range(len(task_items)):
        await queue.put((idx, task_items[idx], 0))


async def run_task(task_func, item, timeout=None):
    if isinstance(item, (list, tuple)):
        return await asyncio.wait_for(
            task_func(*item),
            timeout=timeout
        )
    elif isinstance(item, dict):
        return await asyncio.wait_for(
            task_func(**item),
            timeout=timeout
        )
    else:
        return await asyncio.wait_for(
            task_func(item),
            timeout=timeout
        )


async def _consumer(
    queue,
    result_queue,
    task_func,
    task_name,
    task_timeout=None,
    error_value=None,
    max_retry=3
):
    logger.debug(f"consumer {task_name} start")
    while True:
        try:
            item = await asyncio.wait_for(
                queue.get(),
                timeout=10
            )
            if item is _STOP_ITEM:
                queue.task_done()
                break
            idx, item, attempts = item
            logger.debug(f"Running {task_name} idx={idx}, item={item}")
            try:
                result = await run_task(task_func, item, task_timeout)
            except Exception:
                logger.error(f"Consumer {task_name} error, qsize={queue.qsize()}, error_msg={traceback.format_exc()}")
                if attempts > max_retry:
                    await result_queue.put((idx, error_value, item))
                else:
                    await queue.put((idx, item, attempts + 1))
            else:
                await result_queue.put((idx, result, item))
            finally:
                queue.task_done()
        except asyncio.TimeoutError:
            logger.error(f'Not found task in queue, qsize={queue.qsize()}')
            break


async def _stop_signal(task_queue, consumer_size):
    for _ in range(consumer_size):
        await task_queue.put(_STOP_ITEM)


async def _parse_result(result_queue, callback, **kwargs):
    results = []
    while not result_queue.empty():
        result = await result_queue.get()
        if callback is not None:
            await callback(*result, **kwargs)
        else:
            results.append(result)
        result_queue.task_done()

    if not results:
        return []
    results.sort(key=lambda x: x[0], reverse=False)
    return list(map(lambda x: x[1], results))


async def work_flow(
    task_func: Awaitable,
    task_items: list = None,
    args_func: Awaitable = None,
    args_kwargs: dict = None,
    task_timeout: int = None,
    consumer_size: int = 5,
    error_value: Any = None,
    max_retry: int = 3,
    callback: Awaitable = None,
    alpha: int = 100,
    **kwargs
):
    if not inspect.iscoroutinefunction(task_func):
        raise ValueError("task_func must be async function")
    assert consumer_size > 1, "max_retry must be greater than 1"
    assert max_retry > 0, "max_retry must be greater than 0"
    consumer_size = min(consumer_size, len(task_items)) if task_items is not None else consumer_size

    if task_items is not None and len(task_items) == 1:
        item = task_items[0]
        result = await run_task(task_func, item)
        if callback is not None:
            return await callback(0, result, item, **kwargs)
        return result
    
    result_queue = asyncio.Queue()
    task_queue = asyncio.Queue(
        maxsize=consumer_size * alpha  # 队列大小要远远大于（建议100倍）消费者数量，尤其是任务失败重试机制（当队列太小导致队列满时，重试没法推送任务到队列中，导致系统阻塞）
    )

    producers = asyncio.create_task(_producer(task_queue, task_items, args_func, args_kwargs))
    consumers = [
        asyncio.create_task(
            _consumer(
                task_queue,
                result_queue,
                task_func,
                task_name,
                task_timeout=task_timeout,
                error_value=error_value,
                max_retry=max_retry
            )
        )
        for task_name in range(consumer_size)
    ]

    await producers
    await task_queue.join()
    await _stop_signal(task_queue, consumer_size)
    await asyncio.gather(*consumers)
    return await _parse_result(result_queue, callback, **kwargs)


if __name__ == "__main__":
    import random
    async def task_func(item):
        if random.random() < 0.3:  # 假设有30%的失败概率
            raise ValueError("test error")
        await asyncio.sleep(0.5)
        print("Execute result: ", item)
        return item

    task_items = [f"test_{i}" for i in range(100)]
    results = asyncio.run(
        work_flow(
            task_func,
            task_func=task_items,
            task_timeout=10,
            consumer_size=10,
            error_value='Error',
            max_retry=3
        )
    )
    print("All result:\n", results)
