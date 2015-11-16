// Copyright 2012 Cloudera Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.


#ifndef IMPALA_RUNTIME_DATA_STREAM_MGR_H
#define IMPALA_RUNTIME_DATA_STREAM_MGR_H

#include <list>
#include <set>
#include <boost/thread/mutex.hpp>
#include <boost/shared_ptr.hpp>
#include <boost/thread/condition_variable.hpp>
#include <boost/unordered_map.hpp>

#include "common/status.h"
#include "common/object-pool.h"
#include "runtime/descriptors.h"  // for PlanNodeId
#include "runtime/mem-tracker.h"
#include "util/promise.h"
#include "util/runtime-profile.h"
#include "gen-cpp/Types_types.h"  // for TUniqueId

namespace impala {

class DescriptorTbl;
class DataStreamRecvr;
class RowBatch;
class RuntimeState;
class TRowBatch;

/// Singleton class which manages all incoming data streams at a backend node. It
/// provides both producer and consumer functionality for each data stream.
/// - ImpalaBackend service threads use this to add incoming data to streams
///   in response to TransmitData rpcs (AddData()) or to signal end-of-stream conditions
///   (CloseSender()).
/// - Exchange nodes extract data from an incoming stream via a DataStreamRecvr,
///   which is created with CreateRecvr().
//
/// DataStreamMgr also allows asynchronous cancellation of streams via Cancel()
/// which unblocks all DataStreamRecvr::GetBatch() calls that are made on behalf
/// of the cancelled fragment id.
//
/// TODO: The recv buffers used in DataStreamRecvr should count against
/// per-query memory limits.
class DataStreamMgr {
 public:
  DataStreamMgr() {}

  /// Create a receiver for a specific fragment_instance_id/node_id destination;
  /// If is_merging is true, the receiver maintains a separate queue of incoming row
  /// batches for each sender and merges the sorted streams from each sender into a
  /// single stream.
  /// Ownership of the receiver is shared between this DataStream mgr instance and the
  /// caller.
  boost::shared_ptr<DataStreamRecvr> CreateRecvr(
      RuntimeState* state, const RowDescriptor& row_desc,
      const TUniqueId& fragment_instance_id, PlanNodeId dest_node_id,
      int num_senders, int buffer_size, RuntimeProfile* profile,
      bool is_merging);

  /// Adds a row batch to the recvr identified by fragment_instance_id/dest_node_id
  /// if the recvr has not been cancelled. sender_id identifies the sender instance
  /// from which the data came.
  /// The call blocks if this ends up pushing the stream over its buffering limit;
  /// it unblocks when the consumer removed enough data to make space for
  /// row_batch.
  /// TODO: enforce per-sender quotas (something like 200% of buffer_size/#senders),
  /// so that a single sender can't flood the buffer and stall everybody else.
  /// Returns OK if successful, error status otherwise.
  Status AddData(const TUniqueId& fragment_instance_id, PlanNodeId dest_node_id,
                 const TRowBatch& thrift_batch, int sender_id);

  /// Notifies the recvr associated with the fragment/node id that the specified
  /// sender has closed.
  /// Returns OK if successful, error status otherwise.
  Status CloseSender(const TUniqueId& fragment_instance_id, PlanNodeId dest_node_id,
      int sender_id);

  /// Closes all receivers registered for fragment_instance_id immediately.
  void Cancel(const TUniqueId& fragment_instance_id);

 private:
  friend class DataStreamRecvr;

  /// protects all fields below
  boost::mutex lock_;

  /// map from hash value of fragment instance id/node id pair to stream receivers;
  /// Ownership of the stream revcr is shared between this instance and the caller of
  /// CreateRecvr().
  /// we don't want to create a map<pair<TUniqueId, PlanNodeId>, DataStreamRecvr*>,
  /// because that requires a bunch of copying of ids for lookup
  typedef boost::unordered_multimap<uint32_t,
      boost::shared_ptr<DataStreamRecvr> > StreamMap;
  StreamMap receiver_map_;

  /// less-than ordering for pair<TUniqueId, PlanNodeId>
  struct ComparisonOp {
    bool operator()(const std::pair<impala::TUniqueId, PlanNodeId>& a,
                    const std::pair<impala::TUniqueId, PlanNodeId>& b) {
      if (a.first.hi < b.first.hi) {
        return true;
      } else if (a.first.hi > b.first.hi) {
        return false;
      } else if (a.first.lo < b.first.lo) {
        return true;
      } else if (a.first.lo > b.first.lo) {
        return false;
      }
      return a.second < b.second;
    }
  };

  /// ordered set of registered streams' fragment instance id/node id
  typedef std::set<std::pair<TUniqueId, PlanNodeId>, ComparisonOp > FragmentStreamSet;
  FragmentStreamSet fragment_stream_set_;

  /// Return the receiver for given fragment_instance_id/node_id,
  /// or NULL if not found. If 'acquire_lock' is false, assumes lock_ is already being
  /// held and won't try to acquire it.
  boost::shared_ptr<DataStreamRecvr> FindRecvr(
      const TUniqueId& fragment_instance_id, PlanNodeId node_id,
      bool acquire_lock = true);

  /// Remove receiver block for fragment_instance_id/node_id from the map.
  Status DeregisterRecvr(const TUniqueId& fragment_instance_id, PlanNodeId node_id);

  inline uint32_t GetHashValue(const TUniqueId& fragment_instance_id, PlanNodeId node_id);
};

}

#endif
