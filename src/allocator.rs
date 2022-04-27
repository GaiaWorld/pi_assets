//! 多个资产管理器的容量分配器

use pi_async::rt::{AsyncRuntime, AsyncTaskPool, AsyncTaskPoolExt};

use pi_share::Share;
use pi_time::now_millisecond;

use crate::asset::*;
use crate::mgr::AssetMgr;

// 默认满容量的比例
const FULL: f32 = 0.9;


/// 容量分配器
pub struct Allocator {
    /// 资产管理器列表
    vec: Vec<Item>,
    /// 最大容量
    total_capacity: usize,
    /// 统计总计的最小容量
    min_capacity: usize,
    /// 统计总计的最大容量
    max_capacity: usize,
    /// 整理时用的临时满容量数组
    temp_full: Vec<usize>,
    /// 整理时用的临时超出数组
    temp_overflow: Vec<(usize, usize)>,
}

impl Allocator {
    /// 用指定的最大内存容量创建资产管理器
    pub fn new(total_capacity: usize) -> Self {
        Allocator {
            vec: Vec::new(),
            total_capacity,
            min_capacity: 0,
            max_capacity: 0,
            temp_full: Vec::new(),
            temp_overflow: Vec::new(),
        }
    }
    /// 在指定组别上注册指定加载器的资产管理器，要求必须单线程注册
    pub fn register<A: Asset + Send + Sync, P: 'static, L: AssetLoader<A, P>, G: Garbageer<A>>(
        &mut self,
        mgr: Share<AssetMgr<A, P, L, G>>,
        min_capacity: usize,
        max_capacity: usize,
    ) {
        self.min_capacity += min_capacity;
        self.max_capacity += max_capacity;
        let item = Item {
            mgr,
            min_capacity,
            max_capacity,
            weight_capacity: 0,
            capacity: 0,
        };
        self.vec.push(item);
    }
    /// 获得总大小
    pub fn size(&self) -> usize {
        let mut r = 0;
        for v in self.vec.iter() {
            r += v.mgr.size();
        }
        r
    }
    /// 获得指定组别的总容量
    pub fn total_capacity(&self) -> usize {
        self.total_capacity
    }
    /// 获得指定组别的全部资产缓存的累计最小容量
    pub fn min_capacity(&self) -> usize {
        self.min_capacity
    }
    /// 整理方法， 清理超时和超容量的资源。
    /// 分配器有总内存容量， 按权重分给其下的Mgr。
    /// 如果总容量有空闲， 则按权重提高那些满的Mgr的容量
    pub fn collect(&mut self, now: u64) {
        // 如果有未计算的权重容量， 表示需要重新计算权重
        if self.vec.len() > 0 && self.vec[self.vec.len() - 1].weight_capacity == 0 {
            // 计算理论容量
            let c1 = self.max_capacity - self.min_capacity;
            // 计算实际容量
            let c2 = if self.total_capacity > self.min_capacity {
                self.total_capacity - self.min_capacity
            } else {
                0
            };
            // 计算每个资产分组缓存队列的权重容量
            for i in self.vec.iter_mut() {
                i.weight_capacity = i.min_capacity + (i.max_capacity - i.min_capacity) * c2 / c1;
                i.capacity = i.weight_capacity;
            }
        }
        // 最小容量下，仅进行最小容量清理操作
        if self.total_capacity <= self.min_capacity {
            for i in self.vec.iter() {
                i.mgr.capacity_collect(i.min_capacity);
            }
            return;
        }
        // 超过权重的容量和， 溢出容量
        let mut overflow_size = 0;
        // 超过权重的容量和，空闲容量
        let mut free_size = 0;
        // 满的容量和
        let mut full_size = 0;
        // 先用超时整理腾出空间，统计每种资源的占用
        for i in 0..self.vec.len() {
            let item = &mut self.vec[i];
            item.mgr.timeout_collect(item.min_capacity, now);
            let size = item.mgr.size();
            // println!("item i: {}, {:?}, {:?}", i, size, item.weight_capacity);
            if size <= item.weight_capacity {
                // 如果当前内存小于权重容量，将多余容量放到free_size上
                free_size += item.weight_capacity - size;
                // 将当前容量设置为权重容量
                item.capacity = item.weight_capacity;
            } else {
                // 如果当前内存大于权重容量，则记录该条目，并累计overflow权重
                let overflow = size - item.weight_capacity;
                self.temp_overflow.push((i, overflow));
                // 并将超出大小放到overflow_size上
                overflow_size += overflow;
            }
            // 如果条目已经满了，则记录该条目，并累计capacity权重
            if size as f32 > item.capacity as f32 * FULL {
                self.temp_full.push(i);
                full_size += item.capacity;
            }
        }
        // println!("free_size : {}, {:?}, {:?}, {:?}", free_size, overflow_size, self.temp_full.len(), self.temp_overflow.len());
        if free_size > overflow_size {
            let size = free_size - overflow_size;
            // 空闲比溢出的多，表示需要扩张容量，将满的条目按capacity权重进行扩大
            for index in &self.temp_full {
                let item = &mut self.vec[*index];
                let fix = size * item.capacity / full_size;
                item.capacity += fix;
            }
        }else if free_size < overflow_size {
            let size = overflow_size - free_size;
            // 空闲比溢出的少，表示需要收缩，将溢出的条目按overflow权重进行缩小
            for (index, overflow) in &self.temp_overflow {
                let item = &mut self.vec[*index];
                let fix = size * *overflow / overflow_size;
                item.capacity = item.weight_capacity + *overflow - fix;
            }
        }
        self.temp_full.clear();
        self.temp_overflow.clear();
        // 超量整理
        for i in self.vec.iter() {
            // println!("size:{:?}, capacity:{:?}", i.mgr.size(), i.capacity);
            i.mgr.capacity_collect(i.capacity);
        }
    }
    /// 用指定的间隔时间进行自动整理
    pub fn auto_collect<P>(mut self, rt: AsyncRuntime<(), P>, interval: usize)
    where
        P: AsyncTaskPoolExt<()> + AsyncTaskPool<(), Pool = P>,
    {
        let rt1 = rt.clone();
        let _ = rt.clone().spawn(rt.alloc(), async move {
            loop {
                rt1.wait_timeout(interval).await;
                self.collect(now_millisecond());
            }
        });
    }
}
struct Item {
    mgr: Share<dyn Collect + 'static>,
    min_capacity: usize,
    max_capacity: usize,
    weight_capacity: usize,
    capacity: usize,
}

trait Collect: Send + Sync {
    /// 获得资产的大小
    fn size(&self) -> usize;
    /// 超时整理方法， 清理最小容量外的超时资产
    fn timeout_collect(&self, capacity: usize, now: u64);
    /// 超量整理方法， 按照先进先出的原则，清理超出容量的资产
    fn capacity_collect(&self, capacity: usize);
}
impl<A: Asset, P, L: AssetLoader<A, P>, G: Garbageer<A>> Collect
    for AssetMgr<A, P, L, G>
{
    fn size(&self) -> usize {
        self.size()
    }
    fn timeout_collect(&self, capacity: usize, now: u64) {
        self.timeout_collect(capacity, now)
    }
    fn capacity_collect(&self, capacity: usize) {
        self.capacity_collect(capacity)
    }
}



#[cfg(test)]
mod test_mod {
    use crate::{asset::*, mgr::*, allocator::Allocator};
    use futures::FutureExt;
    use futures::future::BoxFuture;
    use pi_async::rt::multi_thread::{MultiTaskRuntimeBuilder, MultiTaskRuntime};
    use std::{time::Duration, sync::Arc, io};

    #[derive(Debug, Eq, PartialEq)]
    struct R1(usize, usize, usize);

    impl Asset for R1 {
        type Key = usize;
        /// 资源的大小
        fn size(&self) -> usize {
            self.1
        }
    }
    #[derive(Debug, Eq, PartialEq)]
    struct R2(usize, usize, usize);

    impl Asset for R2 {
        type Key = usize;
        /// 资源的大小
        fn size(&self) -> usize {
            self.1
        }
    }
    #[derive(Debug, Eq, PartialEq)]
    struct R3(usize, usize, usize);

    impl Asset for R3 {
        type Key = usize;
        /// 资源的大小
        fn size(&self) -> usize {
            self.1
        }
    }
    struct Loader();

    impl AssetLoader<R1, MultiTaskRuntime<()>> for Loader {
        fn load(&self, k: usize, p: MultiTaskRuntime<()>) -> BoxFuture<'static, io::Result<R1>> {
            println!("async1 id1:{}", k);
            async move {
                p.wait_timeout(1000).await;
                println!("async1 id2:{}", k);
                Ok(R1(k, k, 0))
            }.boxed()
        }
    }
    impl AssetLoader<R2, MultiTaskRuntime<()>> for Loader {
        fn load(&self, k: usize, p: MultiTaskRuntime<()>) -> BoxFuture<'static, io::Result<R2>> {
            println!("async2 id1:{}", k);
            async move {
                p.wait_timeout(1000).await;
                println!("async2 id2:{}", k);
                Ok(R2(k, k, 0))
            }.boxed()
        }
    }
    impl AssetLoader<R3, MultiTaskRuntime<()>> for Loader {
        fn load(&self, k: usize, p: MultiTaskRuntime<()>) -> BoxFuture<'static, io::Result<R3>> {
            println!("async3 id1:{}", k);
            async move {
                p.wait_timeout(1000).await;
                println!("async3 id2:{}", k);
                Ok(R3(k, k, 0))
            }.boxed()
        }
    }

    #[test]
    pub fn test() {
        let pool = MultiTaskRuntimeBuilder::default();
        let rt0 = pool.build();
        let rt1 = rt0.clone();
        let _ = rt0.spawn(rt0.alloc(), async move {
            let mgr = Arc::new(AssetMgr::<R1, _, _, _>::new(
                Loader(),
                GarbageEmpty(), 
                1024*1024,
                3*60*1000,
            ));
            let m = Arc::new(AssetMgr::<R2, _, _, _>::new(
                Loader(),
                GarbageEmpty(), 
                1024*1024,
                3*60*1000,
            ));
            let mm = Arc::new(AssetMgr::<R3, _, _, _>::new(
                Loader(),
                GarbageEmpty(), 
                1024*1024,
                3*60*1000,
            ));
            let mut all = Allocator::new(100);
            all.register(mgr.clone(), 1, 100);
            all.register(m.clone(), 1, 100);
            all.register(mm.clone(), 1, 50);
            mgr.cache(1, R1(1, 1, 0));
            mgr.cache(10, R1(10, 10, 0));
            //mgr.cache(100, R1(100, 100, 0));
            m.cache(20, R2(20, 20, 0));
            m.cache(19, R2(19, 19, 0));
            println!("mgr_info: {:?}", mgr.info());
            all.collect(u64::MAX);
        });
        std::thread::sleep(Duration::from_millis(5000));
    }
}
