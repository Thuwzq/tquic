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

use bytes::Bytes;
use log::*;
use core::error;
use std::time::Instant;
use std::cmp;

use crate::congestion_control::Bbr;
use crate::connection::path::PathMap;
use crate::connection::space::BufferType;
use crate::connection::space::PacketNumSpaceMap;
use crate::connection::space::PacketNumSpace;
use crate::connection::stream;
use crate::connection::stream::StreamMap;
use crate::frame;
use crate::multipath_scheduler::MultipathScheduler;
use crate::Error;
use crate::MultipathConfig;
use crate::Result;
use crate::PathEvent;
use crate::videogop::VideoGopCollection;
use crate::frame::Frame;

/// EMVOD scheduler is designed for video streaming.
/// The goal of EMVOD scheduler is minimize the first frame delay and cost of backup path.
pub struct EMVODScheduler {

        /// Current selected main path.
        main_pid: Option<usize>,

        /// Current selected backup path.
        backup_pid: Option<usize>,
}

impl EMVODScheduler {
    pub fn new(_conf: &MultipathConfig) -> EMVODScheduler {
        EMVODScheduler {
            main_pid: Some(0),
            backup_pid: None,
        }
    }

    fn try_update_main_backup(&mut self, paths: &mut PathMap) {
        // TODO: select best two paths.
        for (pid, path) in paths.iter() {
            if !path.active() {
                continue;
            }

            if self.main_pid.is_none() {
                self.main_pid = Some(pid);
                if self.backup_pid.is_some() {
                    return;
                }
            }

            if self.backup_pid.is_none() {
                self.backup_pid = Some(pid);
                return;
            }
        }
    }

    fn buffer_frame_to_path(
        &mut self, 
        streams: &mut StreamMap, 
        path_space: &mut PacketNumSpace, 
        stream_id: u64, 
        stream_offset: u64, 
        frame_len: u64
    ) -> Option<bool>{

        let stream = streams.get_mut(stream_id)?;
        
        // Inject frame to path buffer.
        let mut limit = frame_len;
        let mut stream_start_offset = stream_offset;

        while limit > 0 {
            let mut out: Vec<u8> = vec![0; limit as usize];
            let frame_hdr_len = frame::stream_header_wire_len(stream_id, stream_offset);
            let (frame_data_len, fin) = match stream.send.read(&mut out) {
                Ok((len, fin)) => (len, fin),
                Err(_) => {
                    error!("buffer frame to path: read frame from stream failed!");
                    return Some(false);
                }
            };

            path_space.buffered.push_back(Frame::Stream { 
                stream_id, 
                offset: stream_start_offset,
                length: frame_data_len, 
                fin,
                data: Bytes::copy_from_slice(&out[..frame_data_len]), 
            }, BufferType::High);

            stream_start_offset += frame_data_len as u64;
            limit -= frame_data_len as u64;
        }

        debug!("buffer frame to path: {:?}, frame length: {}", path_space.id, frame_len);

        return Some(true);
    }

}

impl MultipathScheduler for EMVODScheduler {

    fn on_select(
        &mut self,
        paths: &mut PathMap,
        spaces: &mut PacketNumSpaceMap,
        streams: &mut StreamMap,
    ) -> Result<usize> {
        debug!("EMVODScheduler::on_select");
        let mut is_use_main_path = false;
        let mut is_use_backup_path = false;

        // If main path active, return main path.
        let main_pid = self.main_pid.ok_or(Error::Done)?;
        let main_path = paths.get_mut(main_pid)?;
        let main_path_cwnd = main_path.recovery.congestion.congestion_window();
        let main_path_srtt = main_path.recovery.rtt.smoothed_rtt().as_millis();
        let main_path_space_id = main_path.space_id;

        let mut lost_bytes_total_main: u64 = 0;
        for (_, lost_bytes) in &main_path.recovery.loss_timestamps {
            lost_bytes_total_main += lost_bytes;
        }

        if main_path.active() && main_path.recovery.can_send() {
            is_use_main_path = true;
        }

        // If main path inactive and nobackup path or backup path inactive, return Error:Done.
        let backup_pid = self.backup_pid.ok_or(Error::Done)?;
        let backup_path = paths.get_mut(backup_pid)?;
        if (!backup_path.active() || !backup_path.recovery.can_send()) && !is_use_main_path {
            return Err(Error::Done);
        } 

        let backup_path_cwnd = backup_path.recovery.congestion.congestion_window(); 
        let backup_path_srtt = backup_path.recovery.rtt.smoothed_rtt().as_millis();
        let backup_path_space_id = backup_path.space_id;

        let mut lost_bytes_total_backup: u64 = 0;
        for (_, lost_bytes) in &backup_path.recovery.loss_timestamps {
            lost_bytes_total_backup += lost_bytes;
        }
        
        // Main logic of emvod scheduler.
        let now = Instant::now();
        let main_path_estimated_bandwidth = main_path_cwnd.saturating_sub(lost_bytes_total_main) as f64 / main_path_srtt as f64 * 1000.0;
        let backup_path_estimated_bandwidth = backup_path_cwnd.saturating_sub(lost_bytes_total_backup) as f64 / backup_path_srtt as f64 * 1000.0;
        debug!("backup path can send, start emvod! main path cwnd: {}, backup path cwnd: {}, main path lost bytes: {}, backup path lost bytes: {}, main path estimate bandwidth: {}, backup path estimate bandwidth: {}", main_path_cwnd, backup_path_cwnd, lost_bytes_total_main, lost_bytes_total_backup, main_path_estimated_bandwidth, backup_path_estimated_bandwidth);
        let mut sum_bandwidth : f64 = 0.0;
        
        
        // Calculate bandwidth that all video stream need.
        let stream_iter = streams.iter();
        //loop stream_iter and get all stream_ids and add all bandwidth that video stream need.
        for stream_id in stream_iter {
            
            // Skip streams that were already stopped.
            let stream = match streams.get_mut(stream_id) {
                Some(s) if !s.send.is_stopped() => s,
                _ => {
                    continue;
                }
            };
            
            
            // Get stream next send offset.
            let next_offset_without_retransmit = stream.send.send_off_without_retrans();
            let stream_offset = stream.send.send_off();

            // Get stream context.
            let sctx = match stream.get_context() {
                Some(any) => {
                    if let Some(gop_collection) = any.downcast_mut::<VideoGopCollection>() {
                        gop_collection
                    } else {
                        continue;
                    }
                }
                _ => {
                    continue;
                }
            };
            let sctx = sctx.clone(); // give back the 
            
            // Get stream request timestamp
            let stream_start_time = match sctx.get_request_timestamp() {
                Some(t) => t,
                _ => {
                    debug!("stream {} has no request timestamp, do not need allocate bandwidth.", stream_id);
                    continue;
                }
            };
            
            // Get the video gop will be sent by this stream.
            let play_gop = match sctx.get_gop_by_stream_offset(next_offset_without_retransmit) {
                Some(gop) => gop,
                _ => {
                    error!("stream {} has illegal gop structure, next stream offset: {}", stream_id, next_offset_without_retransmit);
                    if is_use_main_path {
                        return Ok(main_pid);
                    } else {
                        return Err(Error::Done);
                    }
                    
                }
            };

            let gop_end_byte = play_gop.get_gop_end_byte();

            // If the video gop is first gop(gop_num: 0), it means the first frame of one video, preallocate whole gop to path buffer.
            if play_gop.get_gop_number() == 0 && next_offset_without_retransmit == stream_offset { //allocate all first frame, ignore retransmit
                
                debug!("stream {} find first frame, preallocate this frame to path buffer. main path srtt: {}, backup path srtt: {}, main path estimate bandwidth: {}, backup path estimate bandwidth: {}", stream_id, main_path_srtt, backup_path_srtt, main_path_estimated_bandwidth, backup_path_estimated_bandwidth);
                
                // Calculate the OWD Gap between main path and backup path.
                if main_path_srtt <= backup_path_srtt {
                    // Main path faster
                    let owd_gap = (main_path_srtt - backup_path_srtt) as f64/2.0;
        
                    if(gop_end_byte - next_offset_without_retransmit) as f64 <= owd_gap * main_path_estimated_bandwidth as f64 {
                        // Main path send only
                        let main_path_space = spaces.get_mut(main_path_space_id).unwrap();
                        let frame_len = gop_end_byte - stream_offset;
                        
                        self.buffer_frame_to_path(streams, main_path_space, stream_id, stream_offset, frame_len);

                    } else {
                        // Main path send frame front, backup path send frame back.
                        let t_share = ((gop_end_byte - next_offset_without_retransmit) as f64 - 0.5 * (backup_path_srtt - main_path_srtt) as f64 * main_path_estimated_bandwidth)/(main_path_estimated_bandwidth + backup_path_estimated_bandwidth);
                        let cur_pos = 0.5 * (backup_path_srtt - main_path_srtt) as f64 * main_path_estimated_bandwidth + t_share * (main_path_estimated_bandwidth)/t_share * backup_path_estimated_bandwidth;

                        let main_path_send_length = ((gop_end_byte - next_offset_without_retransmit) as f64 * cur_pos).ceil() as u64;
                        let main_path_space = spaces.get_mut(main_path_space_id).unwrap();
                        self.buffer_frame_to_path(streams, main_path_space, stream_id, stream_offset, main_path_send_length);
                        
                        let stream_offset_new = stream_offset + main_path_send_length;
                        let backup_path_send_length = gop_end_byte - next_offset_without_retransmit - main_path_send_length;
                        let backup_path_space = spaces.get_mut(backup_path_space_id).unwrap();
                        self.buffer_frame_to_path(streams, backup_path_space, stream_id, stream_offset_new, backup_path_send_length);
                    }
                } else {
                    // Backup path faster
                    let owd_gap = (backup_path_srtt - main_path_srtt) as f64/2.0;

                    if(gop_end_byte - next_offset_without_retransmit) as f64 <= owd_gap * backup_path_estimated_bandwidth as f64 {
                        // Backup path send only

                        let backup_path_space = spaces.get_mut(backup_path_space_id).unwrap();
                        let frame_len = gop_end_byte - stream_offset;

                        self.buffer_frame_to_path(streams, backup_path_space, stream_id, stream_offset, frame_len);
                        
                    } else {
                        // Backup path send frame front, main path send frame back.
                        let t_share = ((gop_end_byte - next_offset_without_retransmit) as f64 - 0.5 * (main_path_srtt - backup_path_srtt) as f64 * backup_path_estimated_bandwidth)/(backup_path_estimated_bandwidth + main_path_estimated_bandwidth);
                        let cur_pos = 0.5 * (main_path_srtt - backup_path_srtt) as f64 * backup_path_estimated_bandwidth + t_share * (backup_path_estimated_bandwidth)/t_share * main_path_estimated_bandwidth;

                        let backup_path_send_length = ((gop_end_byte - next_offset_without_retransmit) as f64 * cur_pos).ceil() as u64;
                        let backup_path_space = spaces.get_mut(backup_path_space_id).unwrap();
                        self.buffer_frame_to_path(streams, backup_path_space, stream_id, stream_offset, backup_path_send_length);

                        let stream_offset_new = stream_offset + backup_path_send_length;
                        let main_path_send_length = stream_offset + backup_path_send_length;
                        let main_path_space = spaces.get_mut(main_path_space_id).unwrap();
                        self.buffer_frame_to_path(streams, main_path_space, stream_id, stream_offset_new, main_path_send_length);

                        
                    }
                }
            }
            
            //Get the remaining bytes that this gop need send.
            let bytes_need_send = play_gop.get_gop_end_byte() - next_offset_without_retransmit;
            let time_stream_duration = now.duration_since(stream_start_time).as_millis();
            
            if time_stream_duration >= play_gop.get_gop_end_time_stamp() {
                // Stream duration time has surpassed the gop end time, this gop should be trans immediately.
                debug!("stream {} gop end time has surpassed, trans immediately.", stream_id);
                is_use_backup_path = true;
                break;
            } else {
                // Calculate bandwidth that this stream need.
                let time_left = play_gop.get_gop_end_time_stamp() - time_stream_duration;
                let bandwidth_stream_need = bytes_need_send as f64 / time_left as f64 * 1000.0;
                sum_bandwidth += bandwidth_stream_need;
            }
            debug!("stream: {}, current send pos: {}, bytes_need_send: {}, time_stream_duration: {},  main_path_estimated_bandwidth: {}, sum_bandwidth: {}",stream_id, next_offset_without_retransmit, bytes_need_send, time_stream_duration, main_path_estimated_bandwidth, sum_bandwidth);
        }

        if sum_bandwidth > main_path_estimated_bandwidth {
            debug!("Open backup path, sum bandwidth :{} need more than main path estimated bandwidth : {}", sum_bandwidth, main_path_estimated_bandwidth);
            is_use_backup_path = true;
        }

        if is_use_main_path {
            // main path and backup path both available, return main path
            return Ok(main_pid);
        } else if is_use_backup_path {
            backup_path.set_allow_stream_frames(true);
        } else {
            backup_path.set_allow_stream_frames(false);
        }
        Ok(backup_pid)
    }

    fn on_path_updated(&mut self, paths: &mut PathMap, event: PathEvent) {
        match event {
            PathEvent::Validated(pid) => match (self.main_pid, self.backup_pid) {
                (None, None) => self.main_pid = Some(pid),
                (Some(_), None) => self.backup_pid = Some(pid),
                (Some(_), Some(_)) => self.try_update_main_backup(paths),
                _ => unreachable!(),
            },

            PathEvent::Abandoned(pid) => {
                if Some(pid) == self.main_pid {
                    self.main_pid = self.backup_pid;
                    self.backup_pid = None;
                }
                if Some(pid) == self.backup_pid {
                    self.backup_pid = None;
                }
            }
        }
    }
}
