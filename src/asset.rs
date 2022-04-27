//! Asset 资产

use flume::{bounded, Receiver, Sender};
use futures::future::BoxFuture;
use futures::io;
use pi_cache::{Cache, Data, Metrics, Iter};
use pi_hash::XHashMap;
use pi_share::{ShareWeak, Share, ShareMutex};
use pi_time::now_millisecond;
use core::fmt;
use std::collections::hash_map::Entry;
use std::fmt::Debug;
use std::hash::Hash;
use std::io::Result;
use std::ops::Deref;
use std::result::Result as Result1;

/// 资产定义
pub trait Asset: 'static {
    /// 关联键的类型
    type Key: Hash + Eq + Clone + Debug;
    /// 资产的大小
    fn size(&self) -> usize {
        1
    }
}

/// 回收器定义
pub trait Garbageer<A: Asset>: 'static {
    fn garbage(&self, _k: A::Key, _v: A) {}
}

/// 默认的空回收器
pub struct GarbageEmpty();
impl<A: Asset> Garbageer<A> for GarbageEmpty {}

/// 加载器定义
pub trait AssetLoader<A: Asset, P>: 'static {
    fn load(
        &self,
        k: <A as Asset>::Key,
        p: P,
    ) -> BoxFuture<'static, io::Result<A>>;
}

/// 资产表
#[derive(Default)]
pub(crate) struct AssetTable<A: Asset> {
    /// 正在使用的资产表
    map: XHashMap<<A as Asset>::Key, AssetResult<A>>,
    /// 没有被使用的资产缓存表
    cache: Cache<A::Key, Item<A>>,
    // /// 正在使用的资产的大小
    // size: usize,
    /// 缓存超时时间
    pub timeout: usize,
}
pub(crate) enum AssetResult<A: Asset> {
    Ok(ShareWeak<Droper<A>>),
    Wait(Vec<Sender<Result<Handle<A>>>>),
}

impl<A: Asset> AssetTable<A> {
    /// 超时整理方法， 清理最小容量外的超时资产
    pub fn new(timeout: usize) -> Self {
        AssetTable {
            map: Default::default(),
            cache: Default::default(),
            // size: 0,
            timeout,
        }
    }
    /// 获得缓存的大小
    pub fn cache_size(&self) -> usize {
        self.cache.size()
    }
    /// 获得缓存的指标
    pub fn cache_metrics(&self) -> Metrics {
        self.cache.metrics()
    }
    /// 获得缓存的数量
    pub fn cache_len(&self) -> usize {
        self.cache.count()
    }
    /// 判断是否有指定键的数据
    pub fn contains_key(&self, k: &A::Key) -> bool {
        self.map.contains_key(k) || self.cache.contains_key(k)
    }
    /// 获得缓存的迭代器
    pub fn cache_iter(&self) -> Iter<'_, <A as Asset>::Key, Item<A>> {
        self.cache.iter()
    }
    /// 缓存指定的资产
    pub fn cache(&mut self, k: A::Key, v: A) -> Option<A> {
        self.cache.put(k, Item(v, self.timeout as u64 + now_millisecond())).map(|v| v.0)
    }
    /// 放入资产， 并获取资产句柄， 返回None表示已有条目
    pub fn insert(&mut self, k: A::Key, v: A, lock: usize) -> Option<Handle<A>> {
        match self.map.entry(k) {
            Entry::Occupied(_) => None,
            Entry::Vacant(e) => {
                // self.size += v.size();
                let r = Share::new(Droper {
                    key: e.key().clone(),
                    data: Some(v),
                    lock,
                });
                e.insert(AssetResult::Ok(Share::downgrade(&r)));
                Some(r)
            }
        }
    }
    /// 获取已经存在或被缓存的资产
    pub fn get(&mut self, k: A::Key, lock: usize) -> Option<Option<Handle<A>>> {
        match self.map.entry(k) {
            Entry::Occupied(e) => match e.get() {
                AssetResult::Ok(r) => Some(r.upgrade()),
                _ => None,
            },
            Entry::Vacant(e) => {
                let v = match self.cache.take(e.key()) {
                    Some(v) => v,
                    None => return None,
                };
                // self.size += v.0.size();
                let r = Share::new(Droper {
                    key: e.key().clone(),
                    data: Some(v.0),
                    lock,
                });
                e.insert(AssetResult::Ok(Share::downgrade(&r)));
                Some(Some(r))
            }
        }
    }
    /// 检查已经存在或被缓存的资产
    pub fn check(
        &mut self,
        k: A::Key,
        lock: usize,
        wait: bool,
    ) -> Result1<Option<Handle<A>>, Option<Receiver<Result<Handle<A>>>>> {
        match self.map.entry(k) {
            Entry::Occupied(mut e) => match e.get_mut() {
                AssetResult::Ok(r) => Ok(r.upgrade()),
                AssetResult::Wait(vec) => {
                    let (sender, receiver) = bounded(1);
                    vec.push(sender);
                    Err(Some(receiver))
                }
            },
            Entry::Vacant(e) => match self.cache.take(e.key()) {
                Some(v) => {
                    // self.size += v.0.size();
                    let r = Share::new(Droper {
                        key: e.key().clone(),
                        data: Some(v.0),
                        lock,
                    });
                    e.insert(AssetResult::Ok(Share::downgrade(&r)));
                    Ok(Some(r))
                }
                None => {
                    if wait {
                        e.insert(AssetResult::Wait(Vec::new()));
                    }
                    Err(None)
                }
            },
        }
    }
    /// 接受数据， 返回等待的接收器
    pub fn receive(
        &mut self,
        k: A::Key,
        v: A,
        lock: usize,
    ) -> (Handle<A>, Option<AssetResult<A>>) {
        // self.size += v.size();
        let r = Share::new(Droper {
            key: k.clone(),
            data: Some(v),
            lock,
        });
        let weak = AssetResult::Ok(Share::downgrade(&r));
        (r, self.map.insert(k, weak))
    }
    /// 超时整理方法， 清理最小容量外的超时资产
    pub fn timeout_collect<G: Garbageer<A>>(&mut self, g: &G, capacity: usize, now: u64) -> (usize, usize) {
        let mut l = 0;
        let mut s = 0;
        for r in self.cache.timeout_collect(capacity, now) {
            l += 1;
            s += r.1 .0.size();
            g.garbage(r.0, r.1 .0)
        }
        (l, s)
    }
    /// 超量整理方法， 按照先进先出的原则，清理超出容量的资产
    pub fn capacity_collect<G: Garbageer<A>>(&mut self, g: &G, capacity: usize) -> (usize, usize) {
        let mut l = 0;
        let mut s = 0;
        for r in self.cache.capacity_collect(capacity) {
            l += 1;
            s += r.1 .0.size();
            g.garbage(r.0, r.1 .0)
        }
        (l, s)
    }
}

/// 资产条目
pub(crate) struct Item<A: Asset>(pub A, pub u64);
impl<A: Asset> Data for Item<A> {
    fn size(&self) -> usize {
        self.0.size()
    }
    fn timeout(&self) -> u64 {
        self.1
    }
}

/// 可拷贝可回收的资产句柄
pub type Handle<T> = Share<Droper<T>>;

pub struct Droper<T: Asset> {
    key: T::Key,
    data: Option<T>,
    lock: usize,
}
impl<T: Asset + fmt::Debug> fmt::Debug for Droper<T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        fmt::Debug::fmt(&**self, f)
    }
}
impl<T: Asset> Deref for Droper<T> {
    type Target = T;

    fn deref(&self) -> &Self::Target {
        match &self.data {
            Some(d) => d,
            _ => panic!("called `deref()` on a `None` value"),
        }
    }
}
impl<T: Asset> Drop for Droper<T> {
    fn drop(&mut self) {
        let v = unsafe { self.data.take().unwrap_unchecked() };
        let lock: &ShareMutex<AssetTable<T>> = unsafe { &*(self.lock as *const ShareMutex<AssetTable<T>>) };
        let mut table = lock.lock();
        // table.size -= v.size();
        table.map.remove(&self.key);
        let timeout = table.timeout as u64 + now_millisecond();
        table.cache.put(self.key.clone(), Item(v, timeout));
    }
}
