# -*- encoding: utf-8 -*-

'''
@Time        :   2023/12/30 19:52:10
'''

import asyncio
import inspect
from typing import Any, Callable
from collections.abc import Coroutine

AsyncCallable = Callable[..., ...]


async def _producer(queue, tasks):
    for idx in range(len(tasks)):
        await queue.put((idx, tasks[idx], 0))


async def _consumer(
    queue,
    result_queue,
    task_func,
    task_name,
    task_timeout=None,
    error_value=None,
    max_retry=3
):
    print(f"consumer {task_name} start")
    while True:
        try:
            idx, item, attempts = await asyncio.wait_for(
                queue.get(),
                timeout=10
            )
            if item is None:
                queue.task_done()
                break
            try:
                result = await asyncio.wait_for(
                    task_func(item),
                    timeout=task_timeout
                )
            except Exception as e:
                if attempts > max_retry:
                    print(f"Consumer {task_name} error, qsize={queue.qsize()}, error_msg={e}")
                    await result_queue.put((idx, error_value))
                else:
                    await queue.put((idx, item, attempts + 1))
            else:
                await result_queue.put((idx, result))
            finally:
                queue.task_done()
        except asyncio.TimeoutError:
            print(f'Not found task in queue, qsize={queue.qsize()}')
            break


async def _stop_signal(task_queue, consumer_size):
    for _ in range(consumer_size):
        await task_queue.put((0, None, 0))


async def _parse_result(result_queue):
    results = []
    while not result_queue.empty():
        result = await result_queue.get()
        results.append(result)
    results.sort(key=lambda x: x[0], reverse=False)
    results = list(map(lambda x: x[1], results))
    return results


async def work_flow(
    task_items: list,
    task_func: AsyncCallable,
    task_timeout: int = None,
    consumer_size: int = 5,
    error_value: Any = None,
    max_retry: int = 3
):
    if not inspect.iscoroutinefunction(task_func):
        raise ValueError("task_func must be async function")
    assert consumer_size > 1, "max_retry must be greater than 1"
    assert max_retry > 0, "max_retry must be greater than 0"
    
    result_queue = asyncio.Queue()
    task_queue = asyncio.Queue(
        maxsize=len(task_items) * 1000  # 队列大小要远远大于（建议100倍）消费者数量，尤其是任务失败重试机制（当队列太小导致队列满时，重试没法推送任务到队列中，导致系统阻塞）
    )

    producers = asyncio.create_task(_producer(task_queue, task_items))
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
    return await _parse_result(result_queue)


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
            task_items,
            task_func,
            task_timeout=10,
            consumer_size=10,
            error_value='Error',
            max_retry=3
        )
    )
    print("All result:\n", results)
