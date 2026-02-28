// Copyright (c) 2023 The TQUIC Authors.
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

use log::debug;
use std::time::Instant;

use std::fs::File;
use std::io::{BufRead, BufReader};
use std::collections::HashMap;

use crate::Result;


/// A collection of `VideoGop` objects, indexed by `gop_number`.
/// Ensures that each `VideoGop` in the collection has a unique `gop_number`.
#[derive(Debug, Default, Clone)]
pub struct VideoGopCollection {
    gops: HashMap<u32, VideoGop>,
    request_timestamp: Option<Instant>, // record the timestamp that server receive the video play request
}

impl VideoGopCollection {
    /// Creates a new empty `VideoGopCollection`.
    pub fn new() -> Self {
        VideoGopCollection {
            gops: HashMap::new(),
            request_timestamp: None, 
        }
    }

    /// Adds a `VideoGop` to the collection.
    /// Returns `true` if the `VideoGop` was added successfully, or `false` if a `VideoGop` with the same `gop_number` already exists.
    pub fn add_gop(&mut self, gop: VideoGop) -> bool {
        if self.gops.contains_key(&gop.gop_number) {
            false
        } else {
            self.gops.insert(gop.gop_number, gop);
            true
        }
    }

    /// read from file and generate VideoGopCollection。
    /// gop msg file record format：gop_number,gop_start_byte,gop_length,gop_time_stamp,gop_duration_time
    pub fn from_file(file_path: &str) -> Result<Self> {
        
        let file_path = if file_path.starts_with('/') {
            &file_path[1..]
        } else {
            file_path
        };

        let file = File::open(file_path)?;
        let reader = BufReader::new(file);
        let mut collection = VideoGopCollection::new();

        for line in reader.lines() {
            let line = line?;
            let parts: Vec<&str> = line.split(',').collect();
            if parts.len() != 5 {
                continue; // 跳过格式错误的行
            }

            let gop_number = parts[0].parse().unwrap_or_default();
            let gop_start_byte = parts[1].parse().unwrap_or_default();
            let gop_length = parts[2].parse().unwrap_or_default();
            let gop_time_stamp = parts[3].parse().unwrap_or_default();
            let gop_duration_time = parts[4].parse().unwrap_or_default();

            let gop = VideoGop::new(
                gop_number,
                gop_start_byte,
                gop_length,
                gop_time_stamp,
                gop_duration_time,
            );
            collection.add_gop(gop);
        }

        Ok(collection)
    }

    /// Retrieves a reference to the `VideoGop` with the specified `gop_number`.
    /// Returns `None` if no such `VideoGop` exists.
    pub fn get_gop(&self, gop_number: u32) -> Option<&VideoGop> {
        self.gops.get(&gop_number)
    }

    pub fn get_gop_by_stream_offset(&self, stream_offset: u64) -> Option<&VideoGop>{
        for (_, gop) in self.gops.iter() {
            if stream_offset >= gop.gop_start_byte && stream_offset < gop.gop_start_byte + gop.gop_length {
                return Some(gop);
            }
        }
        None
    }

    /// Retrieves a mutable reference to the `VideoGop` with the specified `gop_number`.
    /// Returns `None` if no such `VideoGop` exists.
    pub fn get_gop_mut(&mut self, gop_number: u32) -> Option<&mut VideoGop> {
        self.gops.get_mut(&gop_number)
    }

    /// Removes the `VideoGop` with the specified `gop_number` from the collection.
    /// Returns the removed `VideoGop` if it existed, or `None` otherwise.
    pub fn remove_gop(&mut self, gop_number: u32) -> Option<VideoGop> {
        self.gops.remove(&gop_number)
    }

    /// Returns the number of `VideoGop` objects in the collection.
    pub fn len(&self) -> usize {
        self.gops.len()
    }

    /// Checks if the collection is empty.
    pub fn is_empty(&self) -> bool {
        self.gops.is_empty()
    }

    /// Sets the timestamp when the server receives the video request.
    pub fn set_request_timestamp(&mut self, timestamp: Instant) {
        self.request_timestamp = Some(timestamp);
    }
    
    /// Gets the timestamp when the server received the video request.
    /// Returns `None` if the timestamp is not set.
    pub fn get_request_timestamp(&self) -> Option<Instant> {
        self.request_timestamp
    }
} 


/// Represents a Group of Pictures (GOP) in a video stream.
/// This struct stores metadata about a GOP, including its number, byte range, and timing information.
#[derive(Debug, Clone)]
pub struct VideoGop {
    /// The sequential number of the GOP in the video stream.
    pub gop_number: u32,
    /// The starting byte position of the GOP in the video stream.
    pub gop_start_byte: u64,
    /// The length of the GOP in bytes.
    pub gop_length: u64,
    /// The timestamp of the GOP in the video stream (e.g., in milliseconds).
    pub gop_time_stamp: u128,
    /// The duration of the GOP in the video stream (e.g., in milliseconds).
    pub gop_duration_time: u128,
}

impl Default for VideoGop {
    fn default() -> Self {
        VideoGop {
            gop_number: 0,
            gop_start_byte: 0,
            gop_length: 0,
            gop_time_stamp: 0,
            gop_duration_time: 0,
        }
    }
}

impl VideoGop {
    /// Creates a new `VideoGop` instance with the specified metadata.
    pub fn new(
        gop_number: u32,
        gop_start_byte: u64,
        gop_length: u64,
        gop_time_stamp: u128,
        gop_duration_time: u128,
    ) -> Self {
        VideoGop {
            gop_number,
            gop_start_byte,
            gop_length,
            gop_time_stamp,
            gop_duration_time,
        }
    }

    /// Rturn the gop number.
    pub fn get_gop_number(&self) -> u32 {
        self.gop_number
    }

    // Return the start byte of the GOP.
    pub fn get_gop_start_byte(&self) -> u64 {
        self.gop_start_byte
    }

    // Return the end byte of the GOP.
    pub fn get_gop_end_byte(&self) -> u64 {
        self.gop_start_byte + self.gop_length
    }

    // Return the gop start time stamp
    pub fn get_gop_start_time_stamp(&self) -> u128 {
        self.gop_time_stamp
    }

    // Return the gop end time stamp
    pub fn get_gop_end_time_stamp(&self) -> u128 {
        self.gop_time_stamp + self.gop_duration_time
    }
}
