# async_queue_flow
asyncio异步高并发任务任务处理器，解决任务阻塞、批次执行耗时等待问题。

## 使用方式
git clone https://github.com/Mowuyy/async_queue_flow.git<br>

```python
import random

from async_queue_flow import work_flow


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
```
