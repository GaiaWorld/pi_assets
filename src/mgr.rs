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

    /// 移除指定键的资产。
    ///
    /// 委托给 [`AssetTable::delete`]，从 map 和 cache 中删除资产，并维护
    /// `len` 和 `lock.1` 两个原子计数器。
    ///
    /// # 返回值
    /// - `DeleteResult::Ok(size)`：删除成功。size=0 表示删除的是 map 中待加载(Waiter)
    ///   的条目，无需调整计数；size>0 表示删除的是 cache 中的条目，需要调减计数器。
    /// - `DeleteResult::InUse`：资产在 map 中（外部持有 Handle 或正在释放），无法删除。
    /// - `DeleteResult::NotFound`：资产不存在。
    ///
    /// # 内部处理分支
    ///
    /// ## 1. map 中已加载或正在释放（AssetResult::Ok）
    ///
    /// 外部持有 Handle 或最后一个 Handle 刚 drop（Drop 正在执行），资产尚在途。
    /// 条目被插回 map，返回 `InUse`，计数器不变。
    ///
    /// ## 2. map 中正在异步加载（AssetResult::Wait）
    ///
    /// Wait 条目从未计入计数器，返回 `Ok(0)`。
    ///
    /// **两级通知机制**：
    /// - **主加载者**：`table.receive` → `map.remove(&k)` 返回 `None` →
    ///   `Err("asset loading cancelled")`。
    /// - **次级等待者**：`Vec<Sender>` 随条目 drop，flume 的 `recv_async().await`
    ///   返回 `Err(RecvError::Disconnected)`。
    ///
    /// **防重入保护**：`table.receive` 先 `map.remove(&k)` 再匹配，
    ///   key 已被 delete 移除则返回 `None`，不会"复活"已删除条目。
    ///
    /// ## 3. cache 中（不在使用中）
    ///
    /// 从缓存中移除，返回 `Ok(size)`，递减计数器。
    ///
    /// ## 4. 不存在
    ///
    /// 返回 `NotFound`。
    pub fn remove(&self, k: &A::Key) -> DeleteResult {
        let mut table = self.lock.0.lock().unwrap();
        match table.delete(k) {
            DeleteResult::Ok(size) => {
                drop(table);
                if size > 0 {
                    self.len.fetch_sub(1, Ordering::Release);
                    self.lock.1.fetch_sub(size, Ordering::Release);
                }
                DeleteResult::Ok(size)
            }
            other => other,
        }
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
                    let mut table = self.lock.0.lock().unwrap();
                    let (handle, old) = table.receive(k, v, lock);
                    let r = (handle, old);
                    if r.0.is_ok() {
                        // key 仍在 map 中（加载未被 delete 取消），计入学
                        let amount = self.lock.1.load(Ordering::Acquire);
                        let capacity = self.capacity.load(Ordering::Acquire);
                        let size = amount + add;
                        let (len, sub) = if size > capacity {
                            let t = capacity + table.cache_size();
                            let c = if size < t { t - size } else { 0 };
                            table.capacity_collect(&self.garbage, c, self.ref_garbage_lock)
                        } else {
                            (0, 0)
                        };
                        fetch(&self.len, 1, len);
                        fetch(&self.lock.1, add, sub);
                        (r, len)
                    } else {
                        // key 已被 delete 移除，不插入不计数
                        (r, 0)
                    }
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
    use crate::mgr::*;
    use pi_async_rt::prelude::{AsyncRuntime, AsyncRuntimeExt};
    use pi_async_rt::prelude::multi_thread::{MultiTaskRuntime, MultiTaskRuntimeBuilder};
    use pi_async_rt::rt::AsyncValue;
    use pi_share::cell::TrustCell;
    use pi_time::now_millisecond;
    use std::ops::Deref;
    use std::sync::{Arc, Barrier};
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

    /// 测试删除不存在的资产
    #[test]
    fn test_remove_not_exist() {
        let mgr: Share<AssetMgr<R1, GarbageEmpty>> =
            AssetMgr::new(GarbageEmpty(), false, 1024, 1000);

        assert!(matches!(mgr.remove(&1), DeleteResult::NotFound), "remove non-existent key should return NotFound");
        assert_eq!(mgr.len(), 0);
        assert_eq!(mgr.size(), 0);
    }

    /// 测试从缓存中删除资产（不在使用中）
    #[test]
    fn test_remove_from_cache() {
        let mgr: Share<AssetMgr<R1, GarbageEmpty>> =
            AssetMgr::new(GarbageEmpty(), false, 1024, 1000);

        // 放入缓存
        mgr.cache(1, R1(TrustCell::new((1, 1, 0))));
        assert_eq!(mgr.len(), 1);
        assert_eq!(mgr.size(), 1);

        assert!(matches!(mgr.remove(&1), DeleteResult::Ok(_)), "remove from cache should return Ok");
        assert_eq!(mgr.len(), 0, "len should be decremented");
        assert_eq!(mgr.size(), 0, "size should be decremented");
        assert!(mgr.get(&1).is_none(), "asset should not exist after remove");
    }

    /// 测试删除正在使用中的资产（外部持有Handle）
    /// 有外部引用时删除应失败，返回 InUse
    #[test]
    fn test_remove_in_use() {
        let mgr: Share<AssetMgr<R1, GarbageEmpty>> =
            AssetMgr::new(GarbageEmpty(), false, 1024, 1000);

        // 插入资产，获取 Handle
        let handle = mgr.insert(1, R1(TrustCell::new((1, 10, 0)))).unwrap();
        assert_eq!(mgr.len(), 1);
        assert_eq!(mgr.size(), 10);

        // 持有 Handle 的情况下去删除 —— 应返回 InUse
        assert!(matches!(mgr.remove(&1), DeleteResult::InUse), "remove in-use asset should return InUse");
        assert_eq!(mgr.len(), 1, "len should remain unchanged");
        assert_eq!(mgr.size(), 10, "size should remain unchanged");

        // 资产仍可通过 get 获取
        assert!(mgr.get(&1).is_some(), "get should still work after failed remove");

        // 释放 Handle —— 资产正常进入 cache
        drop(handle);
        assert!(mgr.get(&1).is_some(), "asset should enter cache after drop");
        assert_eq!(mgr.len(), 1, "len should be 1 (asset now in cache)");
        assert_eq!(mgr.size(), 10, "size should be 10 (asset now in cache)");
    }

    /// 测试删除正在异步加载中的资产（主加载者）
    #[test]
    fn test_remove_while_loading() {
        let pool = MultiTaskRuntimeBuilder::default();
        let rt = pool.build();
        let mgr: Share<AssetMgr<R1, GarbageEmpty>> =
            AssetMgr::new(GarbageEmpty(), false, 1024, 1000);

        let barrier = Arc::new(Barrier::new(2));
        let b = barrier.clone();
        // 用 AsyncValue 同步：主线程 remove 后通知异步任务继续执行
        let continue_signal = AsyncValue::<()>::new();
        let cs = continue_signal.clone();
        // 用 AsyncValue 同步：异步任务完成后通知主线程
        let task_done = AsyncValue::<()>::new();
        let td = task_done.clone();
        let mgr1 = mgr.clone();

        // 启动异步加载
        let _ = rt.spawn(async move {
            match AssetMgr::load(&mgr1, &1) {
                LoadResult::Receiver(recv) => {
                    b.wait(); // 通知主线程：Wait 条目已注册
                    cs.await; // 等待主线程完成 remove
                    let result = recv.receive(1, Ok(R1(TrustCell::new((1, 1, 0))))).await;
                    assert!(result.is_err(), "primary loader should get cancelled error");
                    td.set(()); // 通知主线程：断言完成
                }
                other => panic!("expected Receiver, got {:?}", match other {
                    LoadResult::Ok(_) => "Ok",
                    LoadResult::Wait(_) => "Wait",
                    _ => "?",
                }),
            }
        });

        barrier.wait(); // 确认加载者已注册 Wait 条目

        // remove 正在加载的资产
        assert!(matches!(mgr.remove(&1), DeleteResult::Ok(_)), "remove loading asset should return Ok");
        assert_eq!(mgr.len(), 0, "len should be 0 (Wait was never counted)");
        assert_eq!(mgr.size(), 0, "size should be 0");

        // 通知异步任务继续（此时 Wait 已被删除，receive 将返回 Err）
        continue_signal.set(());

        // 用 block_on 同步等待异步任务完成
        rt.block_on(async { task_done.await }).unwrap();

        // 验证 receive 失败后资产不会残留
        assert!(mgr.get(&1).is_none(), "asset should not exist after cancelled load");
        assert_eq!(mgr.len(), 0, "len should be 0 after cancelled load");
        assert_eq!(mgr.size(), 0, "size should be 0 after cancelled load");
    }

    /// 测试删除正在异步加载中的资产（次级等待者通过 flume 断开感知）
    #[test]
    fn test_remove_loading_with_waiters() {
        let pool = MultiTaskRuntimeBuilder::default();
        let rt = pool.build();
        let mgr: Share<AssetMgr<R1, GarbageEmpty>> =
            AssetMgr::new(GarbageEmpty(), false, 1024, 1000);

        let b_primary = Arc::new(Barrier::new(2));
        let b_secondary = Arc::new(Barrier::new(2));
        let bp = b_primary.clone();
        let bs = b_secondary.clone();
        let mgr1 = mgr.clone();
        let mgr2 = mgr.clone();
        let rt1 = rt.clone();

        // 主加载者
        let _ = rt.spawn(async move {
            match AssetMgr::load(&mgr1, &1) {
                LoadResult::Receiver(_recv) => {
                    bp.wait(); // 通知：Wait 已注册
                    // 保持 Wait 条目存活，等 remove 来取消
                    rt1.timeout(1000).await;
                }
                _ => {}
            }
        });

        b_primary.wait(); // 确认主加载者已注册 Wait
        std::thread::sleep(Duration::from_millis(10));

        // 次级等待者
        let secondary_done = Arc::new(std::sync::atomic::AtomicBool::new(false));
        let sd = secondary_done.clone();
        let _ = rt.spawn(async move {
            match AssetMgr::load(&mgr2, &1) {
                LoadResult::Wait(f) => {
                    bs.wait(); // 通知：我已加入等待队列
                    let result = f.await;
                    assert!(
                        result.is_err(),
                        "secondary waiter should get error on disconnect"
                    );
                    sd.store(true, std::sync::atomic::Ordering::Release);
                }
                other => panic!("expected Wait, got {:?}", match other {
                    LoadResult::Ok(_) => "Ok",
                    LoadResult::Receiver(_) => "Receiver",
                    _ => "?",
                }),
            }
        });

        b_secondary.wait(); // 确认次级等待者也加入了等待队列
        std::thread::sleep(Duration::from_millis(10));

        // 删除 —— Vec<Sender> 被 drop，次级等待者的 flume channel 断开
        assert!(matches!(mgr.remove(&1), DeleteResult::Ok(_)), "remove loading asset should return Ok");

        // 等待次级等待者的 future 完成
        std::thread::sleep(Duration::from_millis(200));
        assert!(
            secondary_done.load(std::sync::atomic::Ordering::Acquire),
            "secondary waiter should have completed"
        );

        assert_eq!(mgr.len(), 0);
        assert_eq!(mgr.size(), 0);
    }

    /// 验证 Arc<T> drop 顺序：
    /// 1. 强计数先减为 0 → 然后才调用 T::drop()
    /// 2. T::drop() 执行期间，Weak::upgrade() 返回 None
    #[test]
    fn test_arc_drop_order() {
        use std::cell::Cell;
        use std::sync::{Arc, Weak};

        struct D {
            flag: Cell<bool>,
            weak: Cell<Option<Weak<D>>>,
        }

        impl Drop for D {
            fn drop(&mut self) {
                // 此时 Arc 强计数已是 0，weak 无法升级
                self.flag.set(true);
                let weak = self.weak.take().unwrap();
                assert!(
                    weak.upgrade().is_none(),
                    "Weak::upgrade() must return None while T is being dropped"
                );
            }
        }

        // 持有第 2 个 Arc 使得强计数 > 1，验证不会错误触发 drop
        let a1 = Arc::new(D { flag: Cell::new(false), weak: Cell::new(None) });
        let weak_before = Arc::downgrade(&a1);
        // 强计数 > 1 时 drop 一个 Arc 不会触发 T::drop
        let a2 = a1.clone();
        drop(a2);
        assert!(!weak_before.upgrade().is_none(), "strong > 0, upgrade should succeed");
        assert!(!a1.flag.get(), "should not drop while strong > 0");

        // 保存 weak 进入 D 自身，用于在 drop 时验证
        a1.weak.set(Some(Arc::downgrade(&a1)));
        // 只有一个 strong 了，drop 会触发 D::drop → 内部断言 weak.upgrade().is_none()
        drop(a1);
    }
}
