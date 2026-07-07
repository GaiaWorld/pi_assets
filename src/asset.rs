//! Asset 资产

use core::fmt;
use flume::{bounded, Receiver, Sender};
use pi_cache::{Cache, Data, Iter, Metrics};
use pi_hash::XHashMap;
use pi_share::{Share, ShareMutex, ShareUsize, ShareWeak};
use pi_time::now_millisecond;
use std::collections::hash_map::{Entry, Keys};
use std::fmt::Debug;
use std::hash::Hash;
use std::io::{Error, ErrorKind, Result};
use std::ops::Deref;
use std::result::Result as Result1;
use std::sync::atomic::Ordering;

use crate::allocator::{AssetMgrAccount, AssetInfo};

/// 资产定义
pub trait Size: 'static {
    /// 资产的大小
    fn size(&self) -> usize {
        1
    }
}

/// 资产定义
pub trait Asset: Size {
    /// 关联键的类型
    type Key: Hash + Eq + Clone + Debug;
}

/// 回收器定义
pub trait Garbageer<A: Asset>: 'static {
    /// 回收方法，在锁内执行， 直接拥有kv的所有权
    fn garbage(&self, _k: A::Key, _v: A, _timeout: u64) {}
    /// 回收引用方法，在锁内执行， 获得kv的引用， guard必须在方法外释放，释放时删除kv数据
    fn garbage_ref(&self, _k: &A::Key, _v: &A, _timeout: u64, _guard: GarbageGuard<A>) {}
    /// 回收结束方法， 在锁外执行
    fn finished(&self) {}
}

/// 默认的空回收器
pub struct GarbageEmpty();
impl<A: Asset> Garbageer<A> for GarbageEmpty {}

/// 可拷贝可回收的资产句柄
pub type Handle<A> = Share<Droper<A>>;

pub struct Droper<A: Asset> {
    key: A::Key,
    data: Option<A>,
    lock: usize,
}
unsafe impl<A: Asset> Send for Droper<A> {}
unsafe impl<A: Asset> Sync for Droper<A> {}
impl<A: Asset + fmt::Debug> fmt::Debug for Droper<A> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        fmt::Debug::fmt(&**self, f)
    }
}
impl<A: Asset> Deref for Droper<A> {
    type Target = A;

    fn deref(&self) -> &Self::Target {
        match &self.data {
            Some(d) => d,
            _ => panic!("called `deref()` on a `None` value"),
        }
    }
}
impl<A: Asset> Droper<A> {
    /// adjust size
    pub fn adjust_size(&self, size: isize) {
        let lock: &Lock<A> = unsafe { &*(self.lock as *const Lock<A>) };
        if size > 0 {
            lock.1.fetch_add(size as usize, Ordering::Release);
        } else {
            lock.1.fetch_sub(-size as usize, Ordering::Release);
        }
    }

    pub fn key(&self) -> &A::Key {
        &self.key
    }
}
impl<A: Asset> Drop for Droper<A> {
    fn drop(&mut self) {
        let v = unsafe { self.data.take().unwrap_unchecked() };
        let lock: &Lock<A> = unsafe { &*(self.lock as *const Lock<A>) };
        let mut table = lock.0.lock().unwrap();
        table.map.remove(&self.key);
        let timeout = table.timeout as u64 + now_millisecond();
        table.cache.put(self.key.clone(), Item(v, timeout));
    }
}

/// 垃圾回收守护者
#[derive(Debug)]
pub struct GarbageGuard<T: Asset> {
    key: T::Key,
    lock: usize,
}
impl<T: Asset> Drop for GarbageGuard<T> {
    fn drop(&mut self) {
        let lock: &ShareMutex<AssetTable<T>> =
            unsafe { &*(self.lock as *const ShareMutex<AssetTable<T>>) };
        let mut table = lock.lock().unwrap();
        table.cache.collect(self.key.clone());
    }
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

/// 删除资产的结果
pub enum DeleteResult {
    /// 删除成功，返回资产大小（0 表示无需调整计数器，>0 需要调减计数器）
    Ok(usize),
    /// 资产在 map 中（外部持有 Handle 或正在释放），无法删除
    InUse,
    /// 资产不存在
    NotFound,
}

impl<A: Asset> AssetTable<A> {
    /// 用超时时间，初始表大小，整理率，创建
    pub fn with_config(
        timeout: usize,
        cache_capacity: usize,
        // cuckoo_filter_window_size: usize,
        frequency_down_rate: usize,
    ) -> Self {
        AssetTable {
            map: Default::default(),
            cache: Cache::with_config(
                cache_capacity,
                // cuckoo_filter_window_size,
                frequency_down_rate,
            ),
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
        self.cache.len()
    }
    /// 判断是否有指定键的数据
    pub fn contains_key(&self, k: &A::Key) -> bool {
        self.map.contains_key(k) || self.cache.contains_key(k)
    }
    /// 获得使用表的键迭代器
    pub fn map_keys(&self) -> Keys<'_, <A as Asset>::Key, AssetResult<A>> {
        self.map.keys()
    }
    /// 获得缓存的迭代器
    pub fn cache_iter(&self) -> Iter<'_, <A as Asset>::Key, Item<A>> {
        self.cache.iter()
    }
    /// 缓存指定的资产
    pub fn cache(&mut self, k: A::Key, v: A) -> Option<A> {
        self.cache
            .put(k, Item(v, self.timeout as u64 + now_millisecond()))
            .map(|v| v.0)
    }
    /// 放入资产， 并获取资产句柄，如果已有资产，则重用已有资产
    pub fn insert(&mut self, k: A::Key, v: A, lock: usize) -> (Result1<Handle<A>, A>, bool) {
        match self.map.entry(k) {
            Entry::Occupied(e) => match e.get() {
                AssetResult::Ok(r) => match r.upgrade() {
                    Some(r) => (Ok(r), true),
                    None => (Err(v), true)
                },
                _ => (Err(v), false)
            },
            Entry::Vacant(e) => {
                let (v, b) = match self.cache.take(e.key()) {
                    Some(v) => (v.0, true),
                    None => (v, false),
                };
                let r = Share::new(Droper {
                    key: e.key().clone(),
                    data: Some(v),
                    lock,

                });
                e.insert(AssetResult::Ok(Share::downgrade(&r)));
                (Ok(r), b)
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
                    Some(v) => v.0,
                    None => return None,
                };
                let r = Share::new(Droper {
                    key: e.key().clone(),
                    data: Some(v),
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
    ) -> (Result<Handle<A>>, Option<AssetResult<A>>) {
        // self.size += v.size();
        // 先取出旧条目，确认是Wait才创建Droper
        let old = self.map.remove(&k);
        match old {
            Some(AssetResult::Wait(_)) => {
                // 加载未被取消，创建资产并插入Ok
                let r = Share::new(Droper {
                    key: k.clone(),
                    data: Some(v),
                    lock,

                });
                let weak = AssetResult::Ok(Share::downgrade(&r));
                self.map.insert(k, weak);
                (Ok(r), old)
            }
            Some(other) => {
                // 状态异常，放回
                self.map.insert(k, other);
                (Err(Error::new(ErrorKind::Other, "asset not in loading state")), None)
            }
            None => {
                // 已被delete取消加载
                (Err(Error::new(ErrorKind::Other, "asset loading cancelled")), None)
            }
        }
    }
    /// 移除等待的接收器
    pub fn remove(&mut self, k: &A::Key) -> Option<AssetResult<A>> {
        self.map.remove(k)
    }
    /// 从表中删除资产（包括map和cache）
    ///
    /// 返回值：
    /// - `DeleteResult::Ok(size)`：删除成功。size=0 表示删除的是 map 中待加载(Waiter)
    ///   的条目，无需调整计数；size>0 表示删除的是缓存中的条目，需要调减计数。
    /// - `DeleteResult::InUse`：资产在 map 中（外部持有 Handle 或正在释放），无法删除。
    /// - `DeleteResult::NotFound`：资产不存在。
    pub(crate) fn delete(&mut self, k: &A::Key) -> DeleteResult {
        // 先查使用表
        if let Some(entry) = self.map.remove(k) {
            return match entry {
                AssetResult::Ok(weak) => {
                    // 外部持有 Handle或弱引用失效，正在释放，插回 map，返回 InUse
                    self.map.insert(k.clone(), AssetResult::Ok(weak));
                    DeleteResult::InUse
                },
                AssetResult::Wait(_) => DeleteResult::Ok(0), // 加载中，未记入计数
            };
        }
        // 再查缓存表
        match self.cache.take(k) {
            Some(item) => DeleteResult::Ok(item.0.size()),
            None => DeleteResult::NotFound,
        }
    }
    /// 超时整理方法， 清理最小容量外的超时资产
    pub fn timeout_collect<G: Garbageer<A>>(
        &mut self,
        g: &G,
        capacity: usize,
        now: u64,
        lock: usize,
    ) -> (usize, usize) {
        let mut l = 0;
        let mut s = 0;
        if lock > 0 {
            for r in self.cache.timeout_ref_collect(capacity, now) {
                l += 1;
                s += r.1 .0.size();
                g.garbage_ref(
                    &r.0,
                    &r.1 .0,
                    r.1 .1,
                    GarbageGuard {
                        key: r.0.clone(),
                        lock,
                    },
                )
            }
        } else {
            for r in self.cache.timeout_collect(capacity, now) {
                l += 1;
                s += r.1 .0.size();
                g.garbage(r.0, r.1 .0, r.1 .1)
            }
        }
        (l, s)
    }
    /// 超量整理方法， 按照先进先出的原则，清理超出容量的资产
    pub fn capacity_collect<G: Garbageer<A>>(
        &mut self,
        g: &G,
        capacity: usize,
        lock: usize,
    ) -> (usize, usize) {
        let mut l = 0;
        let mut s = 0;
        if lock > 0 {
            for r in self.cache.capacity_ref_collect(capacity) {
                l += 1;
                s += r.1 .0.size();
                g.garbage_ref(
                    &r.0,
                    &r.1 .0,
                    r.1 .1,
                    GarbageGuard {
                        key: r.0.clone(),
                        lock,
                    },
                )
            }
        } else {
            for r in self.cache.capacity_collect(capacity) {
                // if std::any::type_name::<A>().contains("BufferRes") || std::any::type_name::<A>().contains("GeometryRes") {
                //     log::error!("capacity_collect====={:?}", &r.0);
                // }
                l += 1;
                s += r.1.0.size();
                g.garbage(r.0, r.1 .0, r.1 .1)
            }
        }
        (l, s)
    }

	/// 资源大小
	pub fn account(&self, account: &mut AssetMgrAccount) {
		for (k, item) in self.cache.iter() {
			let size = item.size() as f32 / 1024.0;
            account.unused.push(AssetInfo {
                name: format!("{k:?}"),
                size,
                remain_timeout: if item.timeout() > now_millisecond() {item.timeout() - now_millisecond()} else {0},
            });
			account.unused_size += size;
        }

		for (k, item) in self.map.iter() {
			if let AssetResult::Ok(r) = item {
				if let Some(r) = ShareWeak::upgrade(r) {
					let size = r.size() as f32 / 1024.0;
					account.used.push(AssetInfo {
						name: format!("{k:?}"),
						size,
						remain_timeout: std::u64::MAX,
					});

					account.used_size += size;
				}
			}
		}

        account.timeout = self.timeout;
        account.size = account.used_size + account.unused_size;
        let m = self.cache.metrics();
        account.visit_count = m.hit + m.miss;
        if account.visit_count > 0 {
            account.hit_rate = m.hit as f32 / account.visit_count as f32;
        } 
	}
}

/// 资产锁， 包括正在使用及缓存的资产表，及当前资产的大小
pub(crate) struct Lock<A: Asset>(pub ShareMutex<AssetTable<A>>, pub ShareUsize);

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
