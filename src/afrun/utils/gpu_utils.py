from typing import List, Optional
from queue import Full, Queue
import asyncio

from gpustat import GPUStatCollection


class GPUQueue:
    def __init__(self, gpus: List[int]):
        self.gpus = gpus
        self.max_queue_size = len(self.gpus)
        self.queue = Queue(maxsize=self.max_queue_size)
        self._initialize_queue()

    def _initialize_queue(self):
        """初始化队列，将所有 GPU 加入池中"""
        for gpu in self.gpus:
            self.queue.put(gpu)

    async def acquire_gpu(self, min_free_memory: int = 50 * 1024) -> Optional[int]:
        """
        异步获取一个满足条件的 GPU
        :param min_free_memory: 最小空闲内存（单位：MB）
        :return: 满足条件的 GPU ID 或 None
        """
        while True:
            try:
                # 尝试从队列中获取 GPU
                gpu_id = self.queue.get_nowait()

                # 检查该 GPU 的空闲内存是否满足条件
                if self._is_gpu_suitable(gpu_id, min_free_memory):
                    print(f"Acquired GPU {gpu_id} with sufficient memory.")
                    return gpu_id
                else:
                    # 不满足条件，将 GPU 重新放回队列
                    self.release_gpu(gpu_id)
                    await asyncio.sleep(
                        30
                    )  # 等待 30 秒再尝试获取  每次 5 min / 4 个 GPU

            except Full:
                # 如果队列为空，等待 GPU 资源释放
                print("All GPUs are currently in use, waiting...")
                await asyncio.sleep(30)  # 等待 30 秒再尝试获取  每次 5 min / 4 个 GPU

    def release_gpu(self, gpu: int):
        """释放一个 GPU"""
        self.queue.put(gpu)
        print(f"Released GPU {gpu} back to the pool.")

    @staticmethod
    def _is_gpu_suitable(gpu_id: int, min_free_memory: int) -> bool:
        """
        检查 GPU 是否有足够的空闲内存
        :param gpu_id: GPU ID
        :param min_free_memory: 最小空闲内存要求
        :return: 是否满足要求
        """
        try:
            gpu_stats = GPUStatCollection.new_query()
            gpu_info = gpu_stats[gpu_id]
            free_memory = gpu_info.memory_free  # 单位是 MB

            if free_memory >= min_free_memory:
                print(f"GPU {gpu_id} has {free_memory} MB free memory.")
                return True
            else:
                print(f"GPU {gpu_id} has insufficient memory: {free_memory} MB.")
                return False
        except Exception as e:
            print(f"Error while checking GPU {gpu_id}: {e}")
            return False
