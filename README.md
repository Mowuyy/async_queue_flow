# async_queue_flow
- 以asyncio异步队列方式实现一个灵活、轻量级、高并发任务执行器

## 使用方式
方式一：git clone https://github.com/Mowuyy/async_queue_flow.git<br>
方式二：git submodule add https://github.com/Mowuyy/async_queue_flow.git async_queue_flow

```python
import random

from async_queue_flow import work_flow


async def task_func(item):
    if random.random() < 0.3:  # 假设有30%的失败概率
        raise ValueError("任务处理失败")
    await asyncio.sleep(0.5)
    print("任务处理完成：", item)
    return item

task_items = [f"test_{i}" for i in range(100)]
results = asyncio.run(
    work_flow(
        task_items,
        task_func,
        consumer_size=10,
        error_value='Error',
        max_retry=3
    )
)
print("所有任务结果：", results)
```
