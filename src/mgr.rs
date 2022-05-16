//! 单类型资产管理器
//! 包括了资产的重用，资产的加载
//! 加载新资源和定时整理时，会清理缓存，并调用回收器

use crate::asset::*;
use futures::io;
use pi_cache::Metrics;
use pi_cache::{FREQUENCY_DOWN_RATE, WINDOW_SIZE};
use pi_share::{Share, ShareMutex, ShareUsize};
use std::io::{Error, ErrorKind};
use std::marker::PhantomData;
use std::sync::atomic::Ordering;

#[derive(Debug)]
pub struct AssetMgrInfo {
    pub timeout: usize,
    pub len: usize,
    pub size: usize,
    pub capacity: usize,
    pub cache_len: usize,
    pub cache_size: usize,
    pub cache_metrics: Metrics,
}
/// 单类型资产管理器
pub struct AssetMgr<A: Asset, P, L: AssetLoader<A, P>, G: Garbageer<A>> {
    /// 资产锁， 包括正在使用及缓存的资产表，及当前资产的大小
    lock: Lock<A>,
    /// 当前资产的数量
    len: ShareUsize,
    /// 当前管理器的容量
    capacity: ShareUsize,
    /// 加载器
    loader: L,
    /// 回收器
    garbage: G,
    /// 是否采用引用回收及锁指针
    ref_garbage_lock: usize,
    _p: PhantomData<P>,
}
unsafe impl<A: Asset, P, L: AssetLoader<A, P>, G: Garbageer<A>> Send for AssetMgr<A, P, L, G> {}
unsafe impl<A: Asset, P, L: AssetLoader<A, P>, G: Garbageer<A>> Sync for AssetMgr<A, P, L, G> {}
impl<A: Asset, P, L: AssetLoader<A, P>, G: Garbageer<A>> AssetMgr<A, P, L, G> {
    /// 用指定的参数创建资产管理器， ref_garbage为是否采用引用整理
    pub fn new(
        loader: L,
        garbage: G,
        ref_garbage: bool,
        capacity: usize,
        timeout: usize,
    ) -> Share<Self> {
        Self::with_config(
            loader,
            garbage,
            ref_garbage,
            capacity,
            timeout,
            0,
            WINDOW_SIZE,
            FREQUENCY_DOWN_RATE,
        )
    }
    /// 用指定的参数创建资产管理器
    pub fn with_config(
        loader: L,
        garbage: G,
        ref_garbage: bool, // 是否采用引用整理
        capacity: usize,
        timeout: usize,
        cache_init_capacity: usize,
        cuckoo_filter_window_size: usize,
        frequency_down_rate: usize,
    ) -> Share<Self> {
        let mut mgr = Share::new(AssetMgr {
            lock: Lock(ShareMutex::new(AssetTable::<A>::with_config(
                timeout,
                cache_init_capacity,
                cuckoo_filter_window_size,
                frequency_down_rate,
            )), ShareUsize::new(0)),
            len: ShareUsize::new(0),
            capacity: ShareUsize::new(capacity),
            loader,
            garbage,
            ref_garbage_lock: 0,
            _p: PhantomData,
        });
        if ref_garbage {
            let mgr = Share::get_mut(&mut mgr).unwrap();
            mgr.ref_garbage_lock = &mgr.lock as *const Lock<A> as usize;
        }
        mgr
    }
    /// 获得资产的数量
    pub fn len(&self) -> usize {
        self.len.load(Ordering::Acquire)
    }
    /// 获得资产的大小
    pub fn size(&self) -> usize {
        self.lock.1.load(Ordering::Acquire)
    }
    /// 获得当前容量
    pub fn get_capacity(&self) -> usize {
        self.capacity.load(Ordering::Acquire)
    }
    /// 设置当前容量
    pub fn set_capacity(&self, capacity: usize) {
        self.capacity.store(capacity, Ordering::Release)
    }
    /// 获得基本信息
    pub fn info(&self) -> AssetMgrInfo {
        let len = self.len();
        let size = self.size();
        let capacity = self.get_capacity();
        let table = self.lock.0.lock();
        AssetMgrInfo {
            timeout: table.timeout,
            len,
            size,
            capacity,
            cache_len: table.cache_len(),
            cache_size: table.cache_size(),
            cache_metrics: table.cache_metrics(),
        }
    }
    /// 判断是否有指定键的数据
    pub fn contains_key(&self, k: &A::Key) -> bool {
        let table = self.lock.0.lock();
        table.contains_key(k)
    }
    /// 缓存指定的资产
    pub fn cache(&self, k: A::Key, v: A) -> Option<A> {
        let add = v.size();
        let (r, b) = {
            let mut table = self.lock.0.lock();
            let r = table.cache(k, v);
            let (mut len, mut sub) = if let Some(r) = &r {
                (1, r.size())
            } else {
                (0, 0)
            };
            if add > sub {
                let amount = self.lock.1.load(Ordering::Acquire);
                let capacity = self.capacity.load(Ordering::Acquire);
                let size = amount + add - sub;
                if size > capacity {
                    let t = capacity + table.cache_size();
                    // 获得对应缓存部分的容量， 容量-使用大小
                    let c = if size < t { t - size } else { 0 };
                    let (l, s) = table.capacity_collect(&self.garbage, c, self.ref_garbage_lock);
                    len += l;
                    sub += s;
                }
            }
            fetch(&self.len, 1, len);
            fetch(&self.lock.1, add, sub);
            (r, len > 0)
        };
        if b {
            self.garbage.finished();
        }
        r
    }
    /// 放入资产， 并获取资产句柄
    pub fn insert(&self, k: A::Key, v: A) -> Option<Handle<A>> {
        let add = v.size();
        let lock = &self.lock as *const Lock<A> as usize;
        let (r, b) = {
            let mut table = self.lock.0.lock();
            let r = table.insert(k, v, lock);
            let (len, sub) = if r.is_some() {
                let amount = self.lock.1.load(Ordering::Acquire);
                let capacity = self.capacity.load(Ordering::Acquire);
                let size = amount + add;
                if size > capacity {
                    let t = capacity + table.cache_size();
                    // 获得对应缓存部分的容量， 容量-使用大小
                    let c = if size < t { t - size } else { 0 };
                    table.capacity_collect(&self.garbage, c, self.ref_garbage_lock)
                }else{
                (0, 0)
                }
            }else{
                (0, 0)
            };
            fetch(&self.len, 1, len);
            fetch(&self.lock.1, add, sub);
            (r, len > 0)
        };
        if b {
            self.garbage.finished();
        }
        r
    }
    /// 同步获取已经存在或被缓存的资产
    pub fn get(&self, k: A::Key) -> Option<Handle<A>> {
        let lock = &self.lock as *const Lock<A> as usize;
        loop {
            let mut table = self.lock.0.lock();
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
    pub async fn check<AP>(&self, k: A::Key) -> io::Result<Handle<A>> {
        let lock = &self.lock as *const Lock<A> as usize;
        let receiver = loop {
            let mut table = self.lock.0.lock();
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
    pub async fn load(&self, k: A::Key, p: P) -> io::Result<Handle<A>> {
        let lock = &self.lock as *const Lock<A> as usize;
        let receiver = loop {
            let mut table = self.lock.0.lock();
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
    fn receive(&self, k: A::Key, v: A) -> (Handle<A>, Option<AssetResult<A>>) {
        let add = v.size();
        let lock = &self.lock as *const Lock<A> as usize;
        let (r, b) = {
            let mut table = self.lock.0.lock();
            let amount = self.lock.1.load(Ordering::Acquire);
            let capacity = self.capacity.load(Ordering::Acquire);
            let size = amount + add;
            let (len, sub) = if size > capacity {
                let t = capacity + table.cache_size();
                // 获得对应缓存部分的容量， 容量-使用大小
                let c = if size < t { t - size } else { 0 };
                table.capacity_collect(&self.garbage, c, self.ref_garbage_lock)
            }else{
                (0, 0)
            };
            fetch(&self.len, 1, len);
            fetch(&self.lock.1, add, sub);
            (table.receive(k, v, lock), len > 0)
        };
        if b {
            self.garbage.finished();
        }
        r
    }
    /// 超时整理
    pub fn timeout_collect(&self, min_capacity: usize, now: u64) {
        let size = self.lock.1.load(Ordering::Acquire);
        if size <= min_capacity {
            return;
        }
        let b = {
            let mut table = self.lock.0.lock();
            let t = min_capacity + table.cache_size();
            // 获得对应缓存部分的容量， 容量-使用大小
            let c = if size < t { t - size } else { 0 };
            let (len, sub) = table.timeout_collect(&self.garbage, c, now, self.ref_garbage_lock);
            self.len.fetch_sub(len, Ordering::Acquire);
            self.lock.1.fetch_sub(sub, Ordering::Acquire);
            len > 0
        };
        if b {
            self.garbage.finished();
        }
    }
    /// 超容量整理， 并设置当前容量
    pub fn capacity_collect(&self, capacity: usize) {
        self.capacity.store(capacity, Ordering::Release);
        let size = self.lock.1.load(Ordering::Acquire);
        if size <= capacity {
            return;
        }
        let b = {
            let mut table = self.lock.0.lock();
            let t = capacity + table.cache_size();
            // 获得对应缓存部分的容量， 容量-使用大小
            let c = if size < t { t - size } else { 0 };
            let (len, sub) = table.capacity_collect(&self.garbage, c, self.ref_garbage_lock);
            self.len.fetch_sub(len, Ordering::Acquire);
            self.lock.1.fetch_sub(sub, Ordering::Acquire);
            len > 0
        };
        if b {
            self.garbage.finished();
        }
    }
    /// 迭代
    pub fn iter<Arg>(&self, arg: &mut Arg, func: fn(&mut Arg, k: &A::Key, v: &A, u64)) {
        let table = self.lock.0.lock();
        for (k, item) in table.cache_iter() {
            func(arg, k, &item.0, item.1)
        }
    }
}

fn fetch(i: &ShareUsize, add: usize, sub: usize) {
    if add > sub {
        i.fetch_add(add - sub, Ordering::Release);
    }else if add < sub {
        i.fetch_sub(sub - add, Ordering::Release);
    }
}

#[cfg(test)]
mod test_mod {
    use crate::{asset::*, mgr::*};
    use futures::future::BoxFuture;
    use futures::FutureExt;
    use pi_async::rt::multi_thread::{MultiTaskRuntime, MultiTaskRuntimeBuilder};
    use pi_share::cell::TrustCell;
    use pi_time::now_millisecond;
    use std::borrow::BorrowMut;
    use std::ops::Deref;
    use std::time::Duration;
    extern crate pcg_rand;
    extern crate rand_core;

    use std::{
        time::{SystemTime, UNIX_EPOCH},
    };

    use self::rand_core::{RngCore, SeedableRng};

    #[derive(Debug)]
    struct R1(pub TrustCell<(usize, usize, usize)>);

    impl Asset for R1 {
        type Key = usize;
        /// 资源的大小
        fn size(&self) -> usize {
            self.0.borrow().1
        }
    }
    struct Loader();

    impl AssetLoader<R1, MultiTaskRuntime<()>> for Loader {
        fn load(&self, k: usize, p: MultiTaskRuntime<()>) -> BoxFuture<'static, io::Result<R1>> {
            async move {
                p.wait_timeout(1).await;
                println!("---------------load:{:?}", k);
                Ok(R1(TrustCell::new((k, k, 0))))
            }
            .boxed()
        }
    }
    struct G(MultiTaskRuntime<()>);

    impl Garbageer<R1> for G {
        fn garbage(&self, k: usize, _v: R1, _timeout: u64) {
            println!("garbage: {:?}", k)
        }
        fn garbage_ref(&self, k: &usize, _v: &R1, _timeout: u64, guard: GarbageGuard<R1>) {
            let key = k.clone();
            let _ = self.0.spawn(self.0.alloc(), async move {
                let a = guard;
                println!("garbage_guard: {:?}", a);
            });
        }
    }
    #[test]
    pub fn test() {
        let pool = MultiTaskRuntimeBuilder::default();
        let rt0 = pool.build();
        let rt1 = rt0.clone();
        let seed = SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_secs();
        println!("---------------seed:{:?}", seed);
        let mut rng = pcg_rand::Pcg32::seed_from_u64(seed);
        let mgr = AssetMgr::new(Loader(), G(rt1.clone()), true, 1024 * 1024, 3 * 60 * 1000);
        let mgr1 = mgr.clone();
        mgr.set_capacity(5500);
        let _ = rt0.spawn(rt0.alloc(), async move {
            for i in 1..100 {
                let r = mgr1.load(i, rt1.clone()).await.unwrap();
            }
            println!("----rrr:{:?}", mgr.info());
            let rt2 = rt1.clone();
            let _ = rt1.spawn(rt1.alloc(), async move {
                loop {
                let k = (rng.next_u32() % 150) as usize;
                let r = mgr1.load(k, rt2.clone()).await.unwrap();
                let rr = r.as_ref();
                let rrr = Deref::deref(rr);
                let mut x= rrr.0.borrow_mut();
                x.1 += k * 10;
                rr.adjust_size((k*10) as isize);
                rt2.wait_timeout(100).await;
                }
            });

            loop {
                rt1.wait_timeout(1000).await;
                let now = now_millisecond();
                println!("----time:{}, mgr2:{:?}", now, mgr.info());
                mgr.timeout_collect(0, now);
                //println!("mgr3:{:?}", mgr.info());
                mgr.capacity_collect(5500/2);
                println!("----time:{}, mgr3:{:?}", now, mgr.info());
            }
            
            
        });
        std::thread::sleep(Duration::from_millis(30000));
    }
}
