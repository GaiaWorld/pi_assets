//! 单类型资产管理器
//! 包括了资产的重用，资产的加载
//! 加载新资源和定时整理时，会清理缓存，并调用回收器

use futures::io;
use parking_lot::Mutex;
use pi_cache::Metrics;
use std::io::{Error, ErrorKind};
use std::marker::PhantomData;
use std::sync::atomic::{AtomicUsize, Ordering};

use crate::asset::*;

#[derive(Debug)]
pub struct AssetMgrInfo {
    pub timeout: usize,
    pub size: usize,
    pub capacity: usize,
    pub use_count: usize,
    pub cache_size: usize,
    pub cache_count: usize,
    pub cache_metrics: Metrics,
}
/// 单类型资产管理器
pub struct AssetMgr<A: Asset, P, L: AssetLoader<A, P>, G: Garbageer<A>> {
    /// 正在使用及缓存的资产表
    lock: Mutex<AssetTable<A>>,
    /// 当前资产的大小
    size: AtomicUsize,
    /// 当前管理器的容量
    capacity: AtomicUsize,
    /// 加载器
    loader: L,
    /// 回收器
    garbage: G,
    _p: PhantomData<P>,
}
unsafe impl<A: Asset, P, L: AssetLoader<A, P>, G: Garbageer<A>> Send
    for AssetMgr<A, P, L, G>
{
}
unsafe impl<A: Asset, P, L: AssetLoader<A, P>, G: Garbageer<A>> Sync
    for AssetMgr<A, P, L, G>
{
}
impl<A: Asset, P, L: AssetLoader<A, P>, G: Garbageer<A>> AssetMgr<A, P, L, G> {
    /// 用指定的参数创建资产管理器
    pub fn new(
        loader: L,
        garbage: G,
        capacity: usize,
        timeout: usize,
    ) -> Self {
        AssetMgr {
            lock: Mutex::new(AssetTable::<A>::new(timeout)),
            size: AtomicUsize::new(0),
            capacity: AtomicUsize::new(capacity),
            loader,
            garbage,
            _p: PhantomData,
        }
    }
    /// 获得资产的大小
    pub fn size(&self) -> usize {
        self.size.load(Ordering::Relaxed)
    }
    /// 获得当前容量
    pub fn get_capacity(&self) -> usize {
        self.capacity.load(Ordering::Relaxed)
    }
    /// 设置当前容量
    pub fn set_capacity(&self, capacity: usize) {
        self.capacity.store(capacity, Ordering::Relaxed)
    }
    /// 获得基本信息
    pub fn info(&self) -> AssetMgrInfo {
        let size = self.size();
        let capacity = self.get_capacity();
        let table = self.lock.lock();
        AssetMgrInfo {
            timeout: table.timeout,
            size,
            capacity,
            use_count: table.use_count(),
            cache_size: table.cache_size(),
            cache_count: table.cache_count(),
            cache_metrics: table.cache_metrics(),
        }
    }
    /// 判断是否有指定键的数据
    pub fn contains_key(&self, k: &A::Key) -> bool {
        let table = self.lock.lock();
        table.contains_key(k)
    }
    /// 缓存指定的资产
    pub fn cache(&self, k: A::Key, v: A) -> Option<A> {
        let add = v.size();
        let mut table = self.lock.lock();
        let r = table.cache(k, v);
        let mut sub = if let Some(r) = &r { r.size() } else { 0 };
        if add > sub {
            let amount = self.size.load(Ordering::Relaxed);
            let c = self.capacity.load(Ordering::Relaxed);
            if amount + add > c + sub {
                sub += table.capacity_collect(&self.garbage, c);
            }
        }
        fetch(&self.size, add, sub);
        r
    }
    /// 放入资产， 并获取资产句柄
    pub fn insert(&self, k: A::Key, v: A) -> Option<ArcDrop<A>> {
        let add = v.size();
        let lock = &self.lock as *const Mutex<AssetTable<A>> as usize;
        let mut table = self.lock.lock();
        let r = table.insert(k, v, lock);
        let mut sub = 0;
        if r.is_some() {
            let amount = self.size.load(Ordering::Relaxed);
            let c = self.capacity.load(Ordering::Relaxed);
            if amount + add > c {
                sub = table.capacity_collect(&self.garbage, c);
            }
        }
        fetch(&self.size, add, sub);
        r
    }
    /// 同步获取已经存在或被缓存的资产
    pub fn get(&self, k: A::Key) -> Option<ArcDrop<A>> {
        let lock = &self.lock as *const Mutex<AssetTable<A>> as usize;
        loop {
            let mut table = self.lock.lock();
            if let Some(r) = table.get(k.clone(), lock) {
                if *&r.is_some() {
                    return r;
                }
                // 如果r是None, 表示正在释放，退出当前的锁，循环尝试
            } else {
                return None;
            }
        }
    }

    /// 异步检查已经存在或被缓存的资产，如果资产正在被加载，则挂起等待
    pub async fn check<AP>(&self, k: A::Key) -> io::Result<ArcDrop<A>> {
        let lock = &self.lock as *const Mutex<AssetTable<A>> as usize;
        let receiver = loop {
            let mut table = self.lock.lock();
            match table.check(k.clone(), lock, false) {
                Result::Ok(r) => {
                    if let Some(rr) = r {
                        return Ok(rr);
                    }
                    // 如果r是None, 表示正在释放，退出当前的锁，循环尝试
                }
                Result::Err(r) => {
                    if let Some(r) = r {
                        break r;
                    }
                    return Err(Error::new(ErrorKind::NotFound, ""));
                }
            }
        };
        // 离开同步锁范围，await等待
        match receiver.recv_async().await {
            Ok(r) => r,
            Err(e) => {
                //接收错误，则立即返回
                return Err(Error::new(
                    ErrorKind::Other,
                    format!("asset load fail, reason: {:?}", e),
                ));
            }
        }
    }
    /// 异步加载指定参数的资产
    pub async fn load(&self, k: A::Key, p: P) -> io::Result<ArcDrop<A>> {
        let lock = &self.lock as *const Mutex<AssetTable<A>> as usize;
        let receiver = loop {
            let mut table = self.lock.lock();
            match table.check(k.clone(), lock, true) {
                Result::Ok(r) => {
                    if let Some(rr) = r {
                        return Ok(rr);
                    }
                    // 如果r是None, 表示正在释放，退出当前的锁，循环尝试
                }
                Result::Err(r) => break r,
            }
        };
        // 离开同步锁范围
        if let Some(r) = receiver {
            // 已经在异步加载中， await等待
            return match r.recv_async().await {
                Ok(r) => r,
                Err(e) => {
                    //接收错误，则立即返回
                    Err(Error::new(
                        ErrorKind::Other,
                        format!("asset load fail, reason: {:?}", e),
                    ))
                }
            };
        }
        let f = self.loader.load(k.clone(), p);
        // 执行异步加载任务
        let v = match f.await {
            Err(e) => return Err(e),
            Ok(v) => v,
        };
        // 将数据放入，并获取等待的接收器
        let (r, wait) = self.receive(k, v);
        if let Some(rr) = wait {
            match rr {
                AssetResult::Wait(vec) => {
                    for s in vec {
                        let _ = s.into_send_async(Ok(r.clone())).await;
                    }
                }
                _ => (),
            }
        }
        Ok(r)
    }
    /// 接受数据， 返回等待的接收器
    fn receive(&self, k: A::Key, v: A) -> (ArcDrop<A>, Option<AssetResult<A>>) {
        let add = v.size();
        let lock = &self.lock as *const Mutex<AssetTable<A>> as usize;
        let mut table = self.lock.lock();
        let mut sub = 0;
        let amount = self.size.load(Ordering::Relaxed);
        let c = self.capacity.load(Ordering::Relaxed);
        if amount + add > c {
            sub = table.capacity_collect(&self.garbage, c);
        }
        fetch(&self.size, add, sub);
        table.receive(k, v, lock)
    }
    /// 超时整理
    pub fn timeout_collect(&self, min_capacity: usize, now: u64) {
        let size = self.size.load(Ordering::Relaxed);
        if size <= min_capacity {
            return
        }
        let mut table = self.lock.lock();
        // 获得使用大小 = 总大小 - 缓存大小
        let s = size + table.cache_size();
        // 获得对应缓存部分的容量， 容量-使用大小
        let c = if s <  min_capacity {
            min_capacity - s
        }else{
            0
        };
        let sub = table.timeout_collect(&self.garbage, c, now);
        self.size.fetch_sub(sub, Ordering::Relaxed);
    }
    /// 超容量整理， 并设置当前容量
    pub fn capacity_collect(&self, capacity: usize) {
        self.capacity.store(capacity, Ordering::Relaxed);
        let size = self.size.load(Ordering::Relaxed);
        if size <= capacity {
            return
        }
        let mut table = self.lock.lock();
        // 获得使用大小 = 总大小 - 缓存大小
        let s = size + table.cache_size();
        // 获得对应缓存部分的容量， 容量-使用大小
        let c = if s <  capacity {
            capacity - s
        }else{
            0
        };
        let sub = table.capacity_collect(&self.garbage, c);
        self.size.fetch_sub(sub, Ordering::Relaxed);
    }
}

fn fetch(i: &AtomicUsize, add: usize, sub: usize) {
    if add > sub {
        i.fetch_add(add - sub, Ordering::Relaxed);
    }
    if add < sub {
        i.fetch_sub(sub - add, Ordering::Relaxed);
    }
}

#[cfg(test)]
mod test_mod {
    use crate::{asset::*, mgr::*};
    use futures::FutureExt;
    use futures::future::BoxFuture;
    use pi_async::rt::multi_thread::{MultiTaskRuntimeBuilder, MultiTaskRuntime};
    use std::{time::Duration, sync::Arc};

    #[derive(Debug, Eq, PartialEq)]
    struct R1(usize, usize, usize);

    impl Asset for R1 {
        type Key = usize;
        /// 资源的大小
        fn size(&self) -> usize {
            self.1
        }
    }
    struct Loader();

    impl AssetLoader<R1, MultiTaskRuntime<()>> for Loader {
        fn load(&self, k: usize, p: MultiTaskRuntime<()>) -> BoxFuture<'static, io::Result<R1>> {
            println!("async id1:{}", k);
            async move {
                p.wait_timeout(1000).await;
                println!("async id2:{}", k);
                Ok(R1(k, k, 0))
            }.boxed()
        }
    }

    #[test]
    pub fn test() {
        let pool = MultiTaskRuntimeBuilder::default();
        let rt0 = pool.build();
        let rt1 = rt0.clone();
        let _ = rt0.spawn(rt0.alloc(), async move {
            let mgr = Arc::new(AssetMgr::new(
                Loader(),
                GarbageEmpty(), 
                1024*1024,
                3*60*1000,
            ));
            let mgr1 = mgr.clone();
            let rt2 = rt1.clone();
            let _ = rt1.spawn(rt1.alloc(), async move {
                let r = mgr1.load(1, rt2).await;
                println!("r11:{:?}", r);
            });
            let rt3 = rt1.clone();
            {
                let r = mgr.load(1, rt3).await;
                println!("r1:{:?}", r);
                let r1 = mgr.get(1);
                println!("r2:{:?}", r1);
            }
            println!("mgr1:{:?}", mgr.info());
            {
                // 从cache中提出来
                let r1 = mgr.get(1);
                println!("r3:{:?}", r1);
            }
            println!("mgr2:{:?}", mgr.info());
            mgr.timeout_collect(0, u64::MAX);
            println!("mgr3:{:?}", mgr.info());
        });
        std::thread::sleep(Duration::from_millis(5000));
    }
}
