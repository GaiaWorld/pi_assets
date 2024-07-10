//! 单类型资产管理器
//! 包括了资产的重用，资产的加载
//! 加载新资源和定时整理时，会清理缓存，并调用回收器

use crate::allocator::AssetMgrAccount;
use crate::asset::*;
use pi_futures::BoxFuture;
use futures::io;
use pi_cache::Metrics;
use pi_cache::FREQUENCY_DOWN_RATE;
use pi_share::{Share, ShareMutex, ShareUsize};
use std::fmt::Debug;
use std::io::{Error, ErrorKind};
use std::sync::atomic::Ordering;
use std::result::Result;

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
/// load方法返回的资源接收器
pub enum LoadResult<'a, A: Asset, G: Garbageer<A>> {
    Ok(Handle<A>),
    Wait(BoxFuture<'a, io::Result<Handle<A>>>),
    Receiver(Receiver<A, G>),
}

/// load方法返回的资源接收器
pub struct Receiver<A: Asset, G: Garbageer<A>>(Share<AssetMgr<A, G>>);
impl<A: Asset, G: Garbageer<A>> Receiver<A, G> {
    pub async fn receive(self, k: A::Key, r: io::Result<A>) -> io::Result<Handle<A>> {
        let (r, wait) = self.0.receive(k, r);
        if let Some(rr) = wait {
            match rr {
                AssetResult::Wait(vec) => match &r {
                    Ok(v) => {
                        for s in vec {
                            let _ = s.into_send_async(Ok(v.clone())).await;
                        }
                    }
                    Err(e) => {
                        for s in vec {
                            let _ = s.into_send_async(Err(Error::new(e.kind(), ""))).await;
                        }
                    }
                },
                _ => (),
            }
        }
        r
    }
}
/// 单类型资产管理器
pub struct AssetMgr<A: Asset, G: Garbageer<A> = GarbageEmpty> {
    /// 资产锁， 包括正在使用及缓存的资产表，及当前资产的大小
    lock: Lock<A>,
    /// 当前资产的数量
    len: ShareUsize,
    /// 当前管理器的容量
    capacity: ShareUsize,
    /// 回收器
    garbage: G,
    /// 是否采用引用回收及锁指针
    ref_garbage_lock: usize,

    /// 一些资产可能需要标记类型
    pub ty: u32,
}
unsafe impl<A: Asset, G: Garbageer<A>> Send for AssetMgr<A, G> {}
unsafe impl<A: Asset, G: Garbageer<A>> Sync for AssetMgr<A, G> {}
impl<A: Asset, G: Garbageer<A>> AssetMgr<A, G> {
    /// 用指定的参数创建资产管理器， ref_garbage为是否采用引用整理
    pub fn new(garbage: G, ref_garbage: bool, capacity: usize, timeout: usize) -> Share<Self> {
        Self::with_config(
            garbage,
            ref_garbage,
            capacity,
            timeout,
            0,
            // WINDOW_SIZE,
            FREQUENCY_DOWN_RATE,
        )
    }
    /// 用指定的参数创建资产管理器
    pub fn with_config(
        garbage: G,
        ref_garbage: bool, // 是否采用引用整理
        capacity: usize,
        timeout: usize,
        cache_init_capacity: usize,
        // cuckoo_filter_window_size: usize,
        frequency_down_rate: usize,
    ) -> Share<Self> {
        let mut mgr = Share::new(Self {
            lock: Lock(
                ShareMutex::new(AssetTable::<A>::with_config(
                    timeout,
                    cache_init_capacity,
                    // cuckoo_filter_window_size,
                    frequency_down_rate,
                )),
                ShareUsize::new(0),
            ),
            len: ShareUsize::new(0),
            capacity: ShareUsize::new(capacity),
            garbage,
            ref_garbage_lock: 0,
            ty: std::u32::MAX,
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

    /// 正在使用的大小
    pub fn using_size(&self) -> usize {
        self.size() - self.lock.0.lock().unwrap().cache_size()
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
        // log::error!("info start============={:p}", &self.lock.0);
        let table = self.lock.0.lock().unwrap();
        // log::error!("info end============={:p}", &self.lock.0);
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
        // log::error!("contains_key start============={:p}", &self.lock.0);
        let table = self.lock.0.lock().unwrap();
        // log::error!("contains_key end============={:p}", &self.lock.0);
        table.contains_key(k)
    }
    /// 缓存指定的资产
    pub fn cache(&self, k: A::Key, v: A) -> Option<A> {
        let add = v.size();
        let (r, len) = {
            // log::error!("cache start============={:p}", &self.lock.0);
            let mut table = self.lock.0.lock().unwrap();
            // log::error!("cache end============={:p}", &self.lock.0);
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
            (r, len)
        };
        if len > 0 {
            self.garbage.finished();
        }
        r
    }
    /// 放入资产， 并获取资产句柄， 如果已有资产，则重用已有资产。返回Err表示正在异步加载等待
    pub fn insert(&self, k: A::Key, mut v: A) -> Result<Handle<A>, A> {
        let add = v.size();
        let lock = &self.lock as *const Lock<A> as usize;
        let (r, len) = loop {
            // log::error!("insert start============={:p}", &self.lock.0);
            let mut table = self.lock.0.lock().unwrap();
            let (r, b) = table.insert(k.clone(), v, lock);
            // log::error!("insert end============={:p}", &self.lock.0);
            let r = match r {
                Ok(h) => {
                    if b {
                        // 表示为已有资产
                        return Ok(h)
                    }
                    h
                },
                Err(e) => {
                    if b {
                        // b 表示正在释放，退出当前的锁，循环尝试
                        v = e;
                        continue
                    }
                    return Err(e)
                }
            };
            // 表示新插入，需要统计大小数量及清理
            let amount = self.lock.1.load(Ordering::Acquire);
            let capacity = self.capacity.load(Ordering::Acquire);
            let size = amount + add;
            let (len, sub) = if size > capacity {
                let t = capacity + table.cache_size();
                // 获得对应缓存部分的容量， 容量-使用大小
                let c = if size < t { t - size } else { 0 };
                table.capacity_collect(&self.garbage, c, self.ref_garbage_lock)
            } else {
                (0, 0)
            };
            fetch(&self.len, 1, len);
            fetch(&self.lock.1, add, sub);
            break (Ok(r), len)
        };
        if len > 0 {
            self.garbage.finished();
        }
        r
    }

    pub fn remove(&self, _k: &A::Key) -> bool {
        // TODO
        false
    }
    /// 同步获取已经存在或被缓存的资产
    pub fn get(&self, k: &A::Key) -> Option<Handle<A>> {
        let lock = &self.lock as *const Lock<A> as usize;
        loop {
            // log::error!("get start============={:p}", &self.lock.0);
            let mut table = self.lock.0.lock().unwrap();
            // log::error!("get end============={:p}", &self.lock.0);
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
    /// 异步加载指定参数的资产
    pub fn load<'a>(mgr: &Share<Self>, k: &A::Key) -> LoadResult<'a, A, G> {
        let lock = &mgr.lock as *const Lock<A> as usize;
        let receiver = loop {
            // log::error!("load start============={:p}", &mgr.lock.0);
            let mut table = mgr.lock.0.lock().unwrap();
            // log::error!("load end============={:p}", &mgr.lock.0);
            match table.check(k.clone(), lock, true) {
                Result::Ok(r) => {
                    if let Some(rr) = r {
                        return LoadResult::Ok(rr);
                    }
                    // 如果r是None, 表示正在释放，退出当前的锁，循环尝试
                }
                Result::Err(r) => break r,
            }
        };
        // 离开同步锁范围
        if let Some(r) = receiver {
            // 已经在异步加载中， 返回await等待
            let f = Box::pin( async move {
                match r.recv_async().await {
                    Ok(r) => r,
                    Err(e) => {
                        //接收错误，则立即返回
                        Err(Error::new(
                            ErrorKind::Other,
                            format!("asset load fail, reason: {:?}", e),
                        ))
                    }
                }
            });
            return LoadResult::Wait(f);
        }
        return LoadResult::Receiver(Receiver(mgr.clone()));
    }
    /// 接受数据， 返回等待的接收器
    fn receive(
        &self,
        k: A::Key,
        r: io::Result<A>,
    ) -> (io::Result<Handle<A>>, Option<AssetResult<A>>) {
        match r {
            Ok(v) => {
                let add = v.size();
                let lock: usize = &self.lock as *const Lock<A> as usize;
                let (r, len) = {
                    // log::error!("receive start============={:p}", &self.lock.0);
                    let mut table = self.lock.0.lock().unwrap();
                    // log::error!("receive end============={:p}", &self.lock.0);
                    let amount = self.lock.1.load(Ordering::Acquire);
                    let capacity = self.capacity.load(Ordering::Acquire);
                    let size = amount + add;
                    let (len, sub) = if size > capacity {
                        let t = capacity + table.cache_size();
                        // 获得对应缓存部分的容量， 容量-使用大小
                        let c = if size < t { t - size } else { 0 };
                        table.capacity_collect(&self.garbage, c, self.ref_garbage_lock)
                    } else {
                        (0, 0)
                    };
                    fetch(&self.len, 1, len);
                    fetch(&self.lock.1, add, sub);
                    (table.receive(k, v, lock), len)
                };
                if len > 0 {
                    self.garbage.finished();
                }
                r
            }
            Err(e) => {
                // log::error!("receive1 start============={:p}", &self.lock.0);
                let mut table = self.lock.0.lock().unwrap();
                // log::error!("receive1 end============={:p}", &self.lock.0);
                (Err(e), table.remove(&k))
            }
        }
    }

    /// 超时整理
    pub fn timeout_collect(&self, min_capacity: usize, now: u64) {
        let size = self.lock.1.load(Ordering::Acquire);
        if size <= min_capacity {
            return;
        }
        let b = {
            // log::error!("timeout_collect start============={:p}", &self.lock.0);
            let mut table = self.lock.0.lock().unwrap();
            // log::error!("timeout_collect end============={:p}", &self.lock.0);
            let mut c = table.cache_size();
            if c == 0 {
                return;
            }
            c += min_capacity;
            // 获得对应缓存部分的容量， 容量-使用大小
            let c = if size < c { c - size } else { 0 };
            let (len, sub) = table.timeout_collect(&self.garbage, c, now, self.ref_garbage_lock);
            self.len.fetch_sub(len, Ordering::Acquire);
            self.lock.1.fetch_sub(sub, Ordering::Acquire);
            len > 0
        };
        if b {
            self.garbage.finished();
        }
    }
    /// 超容量整理
    pub fn capacity_collect(&self, capacity: usize) {  
        let size = self.lock.1.load(Ordering::Acquire);
        if size <= capacity {
            return;
        }
        let b = {
            // log::error!("capacity_collect start============={:p}", &self.lock.0);
            let mut table = self.lock.0.lock().unwrap();
            let mut c = table.cache_size();
            if c == 0 {
                return;
            }
            c += capacity;
            // 获得对应缓存部分的容量， 容量-使用大小
            let c = if size < c { c - size } else { 0 };
            let (len, sub) = table.capacity_collect(&self.garbage, c, self.ref_garbage_lock);
            self.len.fetch_sub(len, Ordering::Acquire);
            self.lock.1.fetch_sub(sub, Ordering::Acquire);
            len > 0
        };
        if b {
            self.garbage.finished();
        }
    }
    /// 迭代使用表的键
    pub fn map_keys<Arg>(&self, arg: &mut Arg, func: fn(&mut Arg, k: &A::Key)) {
        // log::error!("map_keys start============={:p}", &self.lock.0);
        let table = self.lock.0.lock().unwrap();
        // log::error!("map_keys end============={:p}", &self.lock.0);
        for k in table.map_keys() {
            func(arg, k)
        }
    }
    /// 迭代缓存
    pub fn cache_iter<Arg>(&self, arg: &mut Arg, func: fn(&mut Arg, k: &A::Key, v: &A, u64)) {
        // log::error!("cache_iter start============={:p}", &self.lock.0);
        let table = self.lock.0.lock().unwrap();
        // log::error!("cache_iter end============={:p}", &self.lock.0);
        for (k, item) in table.cache_iter() {
            func(arg, k, &item.0, item.1)
        }
    }

	/// 资源大小
	pub fn account(&self) -> AssetMgrAccount {
        // log::error!("account start============={:p}", &self.lock.0);
		let table = self.lock.0.lock().unwrap();
        // log::error!("account end============={:p}", &self.lock.0);
		let mut account = AssetMgrAccount::default();
		table.account(&mut account);
		account.name = std::any::type_name::<Self>().to_string();
        account.ty = self.ty;
		account

	}
}

fn fetch(i: &ShareUsize, add: usize, sub: usize) {
    if add > sub {
        i.fetch_add(add - sub, Ordering::Release);
    } else if add < sub {
        i.fetch_sub(sub - add, Ordering::Release);
    }
}

#[cfg(test)]
mod test_mod {
    use crate::{asset::*, mgr::*};
    use pi_async_rt::prelude::AsyncRuntime;
    use pi_async_rt::prelude::multi_thread::{MultiTaskRuntime, MultiTaskRuntimeBuilder};
    use pi_share::cell::TrustCell;
    use pi_time::now_millisecond;
    use std::ops::Deref;
    use std::time::Duration;
    extern crate pcg_rand;
    extern crate rand_core;

    use std::time::{SystemTime, UNIX_EPOCH};

    use self::rand_core::{RngCore, SeedableRng};

    #[derive(Debug)]
    struct R1(pub TrustCell<(usize, usize, usize)>);

    impl Asset for R1 {
        type Key = usize;
    }
    impl Size for R1 {
        /// 资源的大小
        fn size(&self) -> usize {
            self.0.borrow().1
        }
    }
    async fn load(
        mgr: &Share<AssetMgr<R1, G>>,
        k: usize,
        p: MultiTaskRuntime<()>,
    ) -> io::Result<Handle<R1>> {
        match AssetMgr::load(mgr, &k) {
            LoadResult::Ok(r) => Ok(r),
            LoadResult::Wait(f) => f.await,
            LoadResult::Receiver(recv) => {
                p.timeout(1).await;
                println!("---------------load:{:?}", k);
                recv.receive(k, Ok(R1(TrustCell::new((k, k, 0))))).await
            }
        }
    }
    struct G(MultiTaskRuntime<()>);

    impl Garbageer<R1> for G {
        fn garbage(&self, k: usize, _v: R1, _timeout: u64) {
            println!("garbage: {:?}", k)
        }
        fn garbage_ref(&self, k: &usize, _v: &R1, _timeout: u64, guard: GarbageGuard<R1>) {
            let _key = k.clone();
            let _ = self.0.spawn(async move {
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
        let seed = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_secs();
        println!("---------------seed:{:?}", seed);
        let mut rng = pcg_rand::Pcg32::seed_from_u64(seed);
        let mgr = AssetMgr::new(G(rt1.clone()), true, 1024 * 1024, 3 * 60 * 1000);
        let mgr1 = mgr.clone();
        mgr.set_capacity(5500);
        let _ = rt0.spawn(async move {
            for i in 1..100 {
                let _r = load(&mgr1, i, rt1.clone()).await.unwrap();
            }
            println!("----rrr:{:?}", mgr.info());
            let rt2 = rt1.clone();
            let _ = rt1.spawn(async move {
                loop {
                    let k = (rng.next_u32() % 150) as usize;
                    let r = load(&mgr1, k, rt2.clone()).await.unwrap();
                    let rr = r.as_ref();
                    let rrr = Deref::deref(rr);
                    let mut x = rrr.0.borrow_mut();
                    x.1 += k * 10;
                    rr.adjust_size((k * 10) as isize);
                    rt2.timeout(1).await;
                }
            });

            loop {
                rt1.timeout(1000).await;
                let now = now_millisecond();
                println!("----time:{}, mgr2:{:?}", now, mgr.info());
                mgr.timeout_collect(0, now);
                //println!("mgr3:{:?}", mgr.info());
                mgr.capacity_collect(5500 / 2);
                println!("----time:{}, mgr3:{:?}", now, mgr.info());
            }
        });
        std::thread::sleep(Duration::from_millis(30000));
    }
}
