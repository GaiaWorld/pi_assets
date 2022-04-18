//! Asset 资产
//! 包括了资产的重用，资产的加载
//! 假设AssetsMgr和AssetMap都是全生命周期的， 所以在其他数据结构中采用裸指针方式记录该对象
//! 加载新资源和定时整理时，会清理缓存，并调用回收器

use futures::io;
use pi_any::{BoxAny, impl_downcast_box};
use pi_hash::XHashMap;
use slotmap::{DefaultKey, Key};
use std::ops::Deref;
use std::sync::{Arc, Mutex};
use std::{collections::hash_map, any::TypeId};
use std::hash::Hash;
use std::fmt::Debug;
use std::mem::replace;
use pi_cache::{Cache, Data};
use futures::future::BoxFuture;

use crate::asset::*;


pub(crate) trait ResCollect: BoxAny {
    /// 获得资产的内存占用
    fn size(&self) -> usize;
    /// 超时整理方法， 清理最小容量外的超时资产
    fn timeout_collect(&mut self, now: usize);
    /// 超量整理方法， 按照先进先出的原则，清理超出容量的资产
    fn capacity_collect(&mut self);
}
impl_downcast_box!(ResCollect);

/// 单类型资产管理器
struct AssetMgr<T: Asset, P, L:AssetLoader<A=T, Param=P>, G: Garbageer<T>> {
    map: Mutex<AssetMap<T>>,
    /// 最小容量
    pub min_capacity: usize,
    /// 最大容量
    pub max_capacity: usize,
    /// 缓存超时时间
    pub timeout: usize,
    /// 加载器
    loader: L,
    /// 回收器
    garbage: G,
}
impl<T: 'static + Asset, P: 'static, L: 'static +  AssetLoader<A=T, Param=P>, G: 'static + Garbageer<T>> ResCollect for AssetMgr<T, P, L, G> {
    /// 计算资源的内存占用
    fn size(&self) -> usize {
        todo!()
    }

    fn timeout_collect(&mut self, now: usize) {
        todo!()
    }

    fn capacity_collect(&mut self) {
        todo!()
    }
}
/// 多类型资产管理器，支持最大4个分组设置容量
pub struct AssetsMgr {
    /// 资产类型表
    tables: XHashMap<TypeId, Box<dyn ResCollect>>,
    /// 最大4个分组
    groups: [Group; 4],
    /// 整理时用的临时数组，超过预期容量的Cache
    temp: Vec<(TypeId, usize, usize)>,
}

impl AssetsMgr {
    /// 用指定的最大内存容量创建资产管理器
    pub fn with_capacity(total_capacity: [usize; 4]) -> Self {
        AssetsMgr {
            tables: Default::default(),
            groups: Default::default(),
            temp: Vec::new(),
        }
    }
    /// 获得总容量
    pub fn total_capacity(&self, group: u8) -> usize {
        self.groups[group as usize].total_capacity
    }
    /// 获得全部资产缓存的累计最小容量
    pub fn min_capacity(&self, group: u8) -> usize {
        self.groups[group as usize].min_capacity
    }
    /// 获得资产管理器的容量
    pub fn mem_size(&self, group: u8) -> usize {
        let mut r = 0;
        for v in self.tables.values() {
            r += v.size();
        }
        r
    }
    /// 缓存指定的资产
    pub fn cache<T: Asset>(&self, k: T::Key, v: T) {
        todo!()
    }
    /// 放入资产， 并获取资产句柄
    pub fn insert<T: Asset>(&self, k: T::Key, v: T) -> ArcDrop<T> {
        todo!()
    }
    /// 同步获取已经存在或被缓存的资产
    pub fn get<T: Asset>(&self, k: T::Key) -> Option<ArcDrop<T>> {
        None
    }
    /// 异步检查已经存在或被缓存的资产，如果资产正在被加载，则挂起等待
    pub async fn check<T: Asset>(&self, k: T::Key) -> io::Result<ArcDrop<T>> {
        todo!()
    }
    /// 异步加载指定参数的资产
    pub async fn load<T: AssetLoader>(k: <<T as AssetLoader>::A as Asset>::Key, p: T::Param) -> io::Result<T> {
        todo!()
    }
}

/// 多类型资产管理构建器
pub struct AssetsMgrBuilder {

}
impl AssetsMgrBuilder {
    pub fn group(&mut self, total_capacity: usize, group: u8){
        todo!()
    }
    pub fn register<T: AssetLoader>(&mut self, loader: T, garbage: usize, min_capacity: usize, max_capacity: usize, group: u8){

    }
    pub fn finished(&self, mgr: &mut AssetsMgr) {
        todo!()
    }
}
/// 分组管理多个资产的权重
#[derive(Clone, Default)]
struct Group {
    /// 最大容量
    total_capacity: usize,
    /// 统计每个资产分组缓存队列的最小容量
    min_capacity: usize,
    /// 统计每个资产分组缓存队列的权重（最大容量 - 最小容量）
    weight: usize,
}