// Copyright (c) 2022-2025 Alex Chi Z
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

#![allow(unused_variables)] // TODO(you): remove this lint after implementing this mod
#![allow(dead_code)] // TODO(you): remove this lint after implementing this mod

use equivalent::Comparable;
use std::sync::Arc;

use crate::key::{KeySlice, KeyVec};

use super::Block;

/// Iterates on a block.
pub struct BlockIterator {
    /// The internal `Block`, wrapped by an `Arc`
    block: Arc<Block>,
    /// The current key, empty represents the iterator is invalid
    key: KeyVec,
    /// the current value range in the block.data, corresponds to the current key
    value_range: (usize, usize),
    /// Current index of the key-value pair, should be in range of [0, num_of_elements)
    idx: usize,
    /// The first key in the block
    first_key: KeyVec,
}

impl BlockIterator {
    fn new(block: Arc<Block>) -> Self {
        Self {
            block,
            key: KeyVec::new(),
            value_range: (0, 0),
            idx: 0,
            first_key: KeyVec::new(),
        }
    }

    fn seek_to_idx(&mut self, idx: usize) {
        // if idx is out of bounds, mark iterator as invalid by clearing the key
        if idx >= self.block.offsets.len() {
            self.key.clear();
            return;
        }

        self.idx = idx;
        let offset = self.block.offsets[idx] as usize;
        let data = &self.block.data;

        // parse key_len (2 bytes) at offset
        let key_len = u16::from_le_bytes([data[offset], data[offset + 1]]) as usize;
        let key_start = offset + 2;
        let key_end = key_start + key_len;

        // parse val_len (2 bytes) right after the key
        let val_len = u16::from_le_bytes([data[key_end], data[key_end + 1]]) as usize;
        let val_start = key_end + 2;
        let val_end = val_start + val_len;

        // store key (copied, as per spec) and value range (not copied)
        self.key = KeyVec::from_vec(data[key_start..key_end].to_vec());
        self.value_range = (val_start, val_end);
    }

    /// Creates a block iterator and seek to the first entry.
    pub fn create_and_seek_to_first(block: Arc<Block>) -> Self {
        let mut iter = Self::new(block);
        iter.seek_to_first();
        iter
    }

    /// Creates a block iterator and seek to the first key that >= `key`.
    pub fn create_and_seek_to_key(block: Arc<Block>, key: KeySlice) -> Self {
        let mut iter = Self::new(block);
        iter.seek_to_key(key);
        iter
    }

    /// Returns the key of the current entry.
    pub fn key(&self) -> KeySlice<'_> {
        self.key.as_key_slice()
    }

    /// Returns the value of the current entry.
    pub fn value(&self) -> &[u8] {
        &self.block.data[self.value_range.0..self.value_range.1]
    }

    /// Returns true if the iterator is valid.
    /// Note: You may want to make use of `key`
    pub fn is_valid(&self) -> bool {
        !self.key.is_empty()
    }

    /// Seeks to the first key in the block.
    pub fn seek_to_first(&mut self) {
        self.seek_to_idx(0);
        self.first_key = self.key.clone();
    }

    /// Move to the next key in the block.
    pub fn next(&mut self) {
        self.seek_to_idx(self.idx + 1);
    }

    /// Seek to the first key that >= `key`.
    /// Note: You should assume the key-value pairs in the block are sorted when being added by
    /// callers.
    pub fn seek_to_key(&mut self, key: KeySlice) {
        // binary search over the offsets using the key at each index
        let mut lo = 0;
        let mut hi = self.block.offsets.len();

        while lo < hi {
            let mid = lo + (hi - lo) / 2;
            self.seek_to_idx(mid);
            match self.key().compare(&key) {
                std::cmp::Ordering::Less => lo = mid + 1,
                _ => hi = mid,
            }
        }

        self.seek_to_idx(lo);
    }
}
