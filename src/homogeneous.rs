//! 同质资产管理器，缓存同质资产

use core::fmt;
use pi_share::{Share, ShareMutex, ShareUsize};
use pi_time::now_millisecond;
use std::collections::VecDeque;
use std::fmt::Debug;
use std::ops::Deref;
use std::sync::atomic::Ordering;
use crate::allocator::{AssetMgrAccount, AssetInfo};
use crate::asset::Size;

/// 回收器定义
pub trait Garbageer<V: Size>: 'static {
    /// 回收方法，在锁内执行， 直接拥有v的所有权
    fn garbage(&self, _v: V, _timeout: u64) {}
}
/// 默认的空回收器
pub struct GarbageEmpty();
impl<V: Size> Garbageer<V> for GarbageEmpty {}

pub struct Droper<V: Size> {
    data: Option<V>,
    lock: usize,
}
unsafe impl<V: Size> Send for Droper<V> {}
unsafe impl<V: Size> Sync for Droper<V> {}
impl<V: Size + fmt::Debug> fmt::Debug for Droper<V> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        fmt::Debug::fmt(&**self, f)
    }
}
impl<V: Size> Deref for Droper<V> {
    type Target = V;

    fn deref(&self) -> &Self::Target {
        match &self.data {
            Some(d) => d,
            _ => panic!("called `deref()` on a `None` value"),
        }
    }
}

impl<V: Size> Drop for Droper<V> {
    fn drop(&mut self) {
        let v = unsafe { self.data.take().unwrap_unchecked() };
        let lock: &Lock<V> = unsafe { &*(self.lock as *const Lock<V>) };
        let timeout = lock.timeout as u64 + now_millisecond();
        let mut p = lock.pool.lock().unwrap();
        p.1 += v.size();
        p.0.push_back(Item(v, timeout));
    }
}

#[derive(Debug)]
pub struct HomogeneousMgrInfo {
    pub timeout: usize,
    pub size: usize,
    pub capacity: usize,
    pub cache_size: usize,
}

/// 同质资产管理器
pub struct HomogeneousMgr<V: Size, G: Garbageer<V> = GarbageEmpty> {
    /// 同质资产锁， 包括正在缓存的同质资产池，及当前同质资产的数量
    lock: Lock<V>,
    /// 当前管理器的容量
    capacity: ShareUsize,
    /// 回收器
    garbage: G,
}
unsafe impl<V: Size, G: Garbageer<V>> Send for HomogeneousMgr<V, G> {}
unsafe impl<V: Size, G: Garbageer<V>> Sync for HomogeneousMgr<V, G> {}
impl<V: Size, G: Garbageer<V>> HomogeneousMgr<V, G> {
    /// 用指定的参数创建同质资产管理器， ref_garbage为是否采用引用整理
    pub fn new(garbage: G, capacity: usize, timeout: usize) -> Share<Self> {
        Share::new(Self {
            lock: Lock {
                pool: ShareMutex::new((VecDeque::new(), 0)),
                size: ShareUsize::new(0),
                timeout,
            },
            capacity: ShareUsize::new(capacity),
            garbage,
        })
    }
    /// 获得同质资产的大小
    pub fn size(&self) -> usize {
        self.lock.size.load(Ordering::Acquire)
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
            size: self.size(),
            capacity: self.get_capacity(),
            cache_size: self.lock.pool.lock().unwrap().1,
        }
    }
    /// 创建可回收的同质资产
    pub fn create(&self, v: V) -> Droper<V> {
        self.lock.size.fetch_add(v.size(), Ordering::Release);
        Droper {
            data: Some(v),
            lock: &self.lock as *const Lock<V> as usize,
        }
    }
    /// 获取被缓存的可回收的同质资产
    pub fn get(&self) -> Option<Droper<V>> {
        let mut p = self.lock.pool.lock().unwrap();
        if let Some(item) = p.0.pop_back() {
            p.1 -= item.0.size();
            Some(Droper {
                data: Some(item.0),
                lock: &self.lock as *const Lock<V> as usize,
            })
        } else {
            None
        }
    }
    /// 获取一个被过滤器选中的同质资产
    pub fn get_by_filter<P>(&self, mut pred: P) -> Option<Droper<V>>
    where
        P: FnMut(&V) -> bool,
    {
        let mut pool = self.lock.pool.lock().unwrap();
        let mut i = 0;
        for item in pool.0.iter() {
            if pred(&item.0) {
                break;
            }
            i += 1;
        }
        if i >= pool.0.len() {
            return None;
        }
        if let Some(item) = pool.0.swap_remove_front(i) {
            pool.1 -= item.0.size();
            Some(Droper {
                data: Some(item.0),
                lock: &self.lock as *const Lock<V> as usize,
            })
        } else {
            None
        }
    }
    /// 缓存同质资产
    pub fn push(&self, v: V) {
        let timeout = self.lock.timeout as u64 + now_millisecond();
        self.lock.size.fetch_add(v.size(), Ordering::Release);
        let mut p = self.lock.pool.lock().unwrap();
        p.1 += v.size();
        p.0.push_back(Item(v, timeout));
    }
    /// 弹出被缓存的同质资产
    pub fn pop(&self) -> Option<V> {
        let mut p = self.lock.pool.lock().unwrap();
        if let Some(item) = p.0.pop_back() {
            p.1 -= item.0.size();
            self.lock.size.fetch_sub(item.0.size(), Ordering::Release);
            Some(item.0)
        } else {
            None
        }
    }
    /// 弹出一个被过滤器选中的同质资产
    pub fn pop_by_filter<P>(&self, mut pred: P) -> Option<V>
    where
        P: FnMut(&V) -> bool,
    {
        let mut pool = self.lock.pool.lock().unwrap();
        let mut i = 0;
        for item in pool.0.iter() {
            if pred(&item.0) {
                break;
            }
            i += 1;
        }
        if i >= pool.0.len() {
            return None;
        }
        if let Some(item) = pool.0.swap_remove_front(i) {
            pool.1 -= item.0.size();
            self.lock.size.fetch_sub(item.0.size(), Ordering::Release);
            Some(item.0)
        } else {
            None
        }
    }
    /// 超时整理
    pub fn timeout_collect(&self, min_capacity: usize, now: u64) {
        let size = self.size();
        if size <= min_capacity {
            return;
        }
        let mut pool = self.lock.pool.lock().unwrap();
        let mut sub = 0;
        while !pool.0.is_empty() {
            let r = pool.0.front().unwrap();
            let s = r.0.size();
            if r.1 > now || size < min_capacity + sub + s {
                break;
            }
            let r = pool.0.pop_front().unwrap();
            sub += s;
            self.garbage.garbage(r.0, r.1);
        }
        if sub > 0 {
            pool.1 -= sub;
            self.lock.size.fetch_sub(sub, Ordering::Acquire);
        }
    }
    /// 超容量整理， 并设置当前容量
    pub fn capacity_collect(&self, capacity: usize) {
        let size = self.size();
        if size <= capacity {
            return;
        }
        let mut pool = self.lock.pool.lock().unwrap();
        let mut sub = 0;
        while size > capacity + sub && !pool.0.is_empty() {
            let r = pool.0.pop_front().unwrap();
            sub += r.0.size();
            self.garbage.garbage(r.0, r.1);
        }
        if sub > 0 {
            pool.1 -= sub;
            self.lock.size.fetch_sub(sub, Ordering::Acquire);
        }
    }

	/// 资源大小
	pub fn account(&self) -> AssetMgrAccount {
		let pool = self.lock.pool.lock().unwrap();
		let mut account = AssetMgrAccount::default();

		for item in pool.0.iter() {
			let size = item.0.size() as f32 / 1024.0;
            account.unused.push(AssetInfo {
                name: "".to_string(),
                size,
                remain_timeout: if item.1 > now_millisecond() {item.1 - now_millisecond()} else {0},
            });
			account.unused_size += size;
		}
		account.name = std::any::type_name::<Self>().to_string();
		account

	}
}

/// 同质资产锁， 包括正在缓存的同质资产池，及当前同质资产的数量，及缓存超时时间
struct Lock<V> {
    pool: ShareMutex<(VecDeque<Item<V>>, usize)>,
    size: ShareUsize,
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

    #[derive(Debug)]
    struct Uu(u128);
    impl Size for Uu {}
    #[test]
    pub fn test() {
        let seed = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_secs();
        println!("---------------seed:{:?}", seed);
        let mut rng = pcg_rand::Pcg32::seed_from_u64(seed);
        let mgr = HomogeneousMgr::new(GarbageEmpty(), 1024 * 1024, 3 * 60 * 1000);
        mgr.set_capacity(1000);
        let _r = mgr.create(Uu(1u128));
        {
            let _r = mgr.create(Uu(2u128));
        }
        println!("----rrr:{:?}", mgr.info());
        println!("----pop:{:?}", mgr.pop());
        for i in 1..100 {
            mgr.create(Uu(i as u128 + rng.next_u64() as u128));
        }

        let now = now_millisecond();
        println!("----time:{}, mgr2:{:?}", now, mgr.info());
        mgr.timeout_collect(0, now);
        println!("mgr3:{:?}", mgr.info());
        mgr.capacity_collect(60);
        println!("----time:{}, mgr4:{:?}", now, mgr.info());
    }
}
