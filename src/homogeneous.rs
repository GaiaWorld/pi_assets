//! 同质资产管理器，缓存同质资产

use core::fmt;
use pi_share::{Share, ShareMutex, ShareUsize};
use pi_time::now_millisecond;
use std::collections::VecDeque;
use std::fmt::Debug;
use std::ops::Deref;
use std::sync::atomic::Ordering;

/// 回收器定义
pub trait Garbageer<V>: 'static {
    /// 回收方法，在锁内执行， 直接拥有v的所有权
    fn garbage(&self, _v: V, _timeout: u64) {}
}
/// 默认的空回收器
pub struct GarbageEmpty();
impl<V> Garbageer<V> for GarbageEmpty {}

pub struct Droper<V> {
    data: Option<V>,
    lock: usize,
}
unsafe impl<V> Send for Droper<V> {}
unsafe impl<V> Sync for Droper<V> {}
impl<V: fmt::Debug> fmt::Debug for Droper<V> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        fmt::Debug::fmt(&**self, f)
    }
}
impl<V> Deref for Droper<V> {
    type Target = V;

    fn deref(&self) -> &Self::Target {
        match &self.data {
            Some(d) => d,
            _ => panic!("called `deref()` on a `None` value"),
        }
    }
}

impl<V> Drop for Droper<V> {
    fn drop(&mut self) {
        let v = unsafe { self.data.take().unwrap_unchecked() };
        let lock: &Lock<V> = unsafe { &*(self.lock as *const Lock<V>) };
        let timeout = lock.timeout as u64 + now_millisecond();
        lock.pool.lock().push_back(Item(v, timeout));
    }
}

#[derive(Debug)]
pub struct HomogeneousMgrInfo {
    pub timeout: usize,
    pub len: usize,
    pub size: usize,
    pub capacity: usize,
    pub cache_len: usize,
}

/// 同质资产管理器
pub struct HomogeneousMgr<V, G: Garbageer<V> = GarbageEmpty> {
    /// 同质资产锁， 包括正在缓存的同质资产池，及当前同质资产的数量
    lock: Lock<V>,
    /// 单个同质资产的大小
    unit_size: usize,
    /// 当前管理器的容量
    capacity: ShareUsize,
    /// 回收器
    garbage: G,
}
unsafe impl<V, G: Garbageer<V>> Send for HomogeneousMgr<V, G> {}
unsafe impl<V, G: Garbageer<V>> Sync for HomogeneousMgr<V, G> {}
impl<V, G: Garbageer<V>> HomogeneousMgr<V, G> {
    /// 用指定的参数创建同质资产管理器， ref_garbage为是否采用引用整理
    pub fn new(garbage: G, capacity: usize, unit_size: usize, timeout: usize) -> Share<Self> {
        Share::new(Self {
            lock: Lock {
                pool: ShareMutex::new(VecDeque::new()),
                len: ShareUsize::new(0),
                timeout,
            },
            unit_size,
            capacity: ShareUsize::new(capacity),
            garbage,
        })
    }
    /// 获得同质资产的数量
    pub fn len(&self) -> usize {
        self.lock.len.load(Ordering::Acquire)
    }
    /// 获得同质资产的大小
    pub fn size(&self) -> usize {
        self.len() * self.unit_size
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
    pub fn info(&self) -> HomogeneousMgrInfo {
        HomogeneousMgrInfo {
            timeout: self.lock.timeout,
            len: self.len(),
            size: self.size(),
            capacity: self.get_capacity(),
            cache_len: self.lock.pool.lock().len(),
        }
    }
    /// 创建可回收的同质资产
    pub fn create(&self, v: V) -> Droper<V> {
        self.lock.len.fetch_add(1, Ordering::Release);
        Droper {
            data: Some(v),
            lock: &self.lock as *const Lock<V> as usize,
        }
    }
    /// 获取被缓存的可回收的同质资产
    pub fn get(&self) -> Option<Droper<V>> {
        self.lock.pool.lock().pop_back().map(|item| Droper {
            data: Some(item.0),
            lock: &self.lock as *const Lock<V> as usize,
        })
    }
    /// 获取一个被过滤器选中的同质资产
    pub fn get_by_filter<P>(&self, mut pred: P) -> Option<Droper<V>>
    where
        P: FnMut(&V) -> bool,
    {
        let mut pool = self.lock.pool.lock();
        let mut i = 0;
        for item in pool.iter() {
            if pred(&item.0) {
                break;
            }
            i += 1;
        }
        if i >= pool.len() {
            return None;
        }
        Some(Droper {
            data: Some(pool.swap_remove_front(i).unwrap().0),
            lock: &self.lock as *const Lock<V> as usize,
        })
    }
    /// 缓存同质资产
    pub fn push(&self, v: V) {
        let timeout = self.lock.timeout as u64 + now_millisecond();
        self.lock.pool.lock().push_back(Item(v, timeout));
        self.lock.len.fetch_add(1, Ordering::Release);
    }
    /// 弹出被缓存的同质资产
    pub fn pop(&self) -> Option<V> {
        self.lock.pool.lock().pop_back().map(|item| {
            self.lock.len.fetch_sub(1, Ordering::Release);
            item.0
        })
    }
    /// 弹出一个被过滤器选中的同质资产
    pub fn pop_by_filter<P>(&self, mut pred: P) -> Option<V>
    where
        P: FnMut(&V) -> bool,
    {
        let mut pool = self.lock.pool.lock();
        let mut i = 0;
        for item in pool.iter() {
            if pred(&item.0) {
                break;
            }
            i += 1;
        }
        if i >= pool.len() {
            return None;
        }
        Some(pool.swap_remove_front(i).unwrap().0)
    }
    /// 超时整理
    pub fn timeout_collect(&self, min_capacity: usize, now: u64) {
        let min_capacity = min_capacity / self.unit_size;
        let len = self.len();
        if len <= min_capacity {
            return;
        }
        let mut pool = self.lock.pool.lock();
        if pool.is_empty() {
            return;
        }
        let t = min_capacity + pool.len();
        // 获得对应缓存部分的容量， 容量-使用大小
        let c = if len < t { t - len } else { 0 };
        let mut sub = 0;
        while pool.len() > c && pool.front().unwrap().1 < now {
            let r = pool.pop_front().unwrap();
            self.garbage.garbage(r.0, r.1);
            sub += 1;
        }
        self.lock.len.fetch_sub(sub, Ordering::Acquire);
    }
    /// 超容量整理， 并设置当前容量
    pub fn capacity_collect(&self, capacity: usize) {
        let capacity = capacity / self.unit_size;
        let len = self.len();
        if len <= capacity {
            return;
        }
        let mut pool = self.lock.pool.lock();
        if pool.is_empty() {
            return;
        }
        let t = capacity + pool.len();
        // 获得对应缓存部分的容量， 容量-使用大小
        let c = if len < t { t - len } else { 0 };
        let sub = pool.len() - c;
        for _ in 0..sub {
            let r = pool.pop_back().unwrap();
            self.garbage.garbage(r.0, r.1);
        }
        self.lock.len.fetch_sub(sub, Ordering::Acquire);
    }
}

/// 同质资产锁， 包括正在缓存的同质资产池，及当前同质资产的数量，及缓存超时时间
struct Lock<V> {
    pool: ShareMutex<VecDeque<Item<V>>>,
    len: ShareUsize,
    timeout: usize,
}
/// 同质资产条目
struct Item<V>(pub V, pub u64);

#[cfg(test)]
mod test_mod {
    use crate::homogeneous::*;
    use pi_time::now_millisecond;
    extern crate pcg_rand;
    extern crate rand_core;

    use std::time::{SystemTime, UNIX_EPOCH};

    use self::rand_core::{RngCore, SeedableRng};

    #[test]
    pub fn test() {
        let seed = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_secs();
        println!("---------------seed:{:?}", seed);
        let mut rng = pcg_rand::Pcg32::seed_from_u64(seed);
        let mgr = HomogeneousMgr::new(GarbageEmpty(), 1024 * 1024, 20, 3 * 60 * 1000);
        mgr.set_capacity(1000);
        let _r = mgr.create(1u128);
        {
            let _r = mgr.create(2u128);
        }
        println!("----rrr:{:?}", mgr.info());
        println!("----pop:{:?}", mgr.pop());
        for i in 1..100 {
            mgr.push(i as u128 + rng.next_u64() as u128);
        }

        let now = now_millisecond();
        println!("----time:{}, mgr2:{:?}", now, mgr.info());
        mgr.timeout_collect(0, now);
        println!("mgr3:{:?}", mgr.info());
        mgr.capacity_collect(600);
        println!("----time:{}, mgr4:{:?}", now, mgr.info());
    }
}
