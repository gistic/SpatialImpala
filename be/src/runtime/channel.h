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

// A channel sends data asynchronously via calls to TransmitData
// to a single destination ipaddress/node.
// It has a fixed-capacity buffer and allows the caller either to add rows to
// that buffer individually (AddRow()), or circumvent the buffer altogether and send
// TRowBatches directly (SendBatch()). Either way, there can only be one in-flight RPC
// at any one time (ie, sending will block if the most recent rpc hasn't finished,
// which allows the receiver node to throttle the sender by withholding acks).
// *Not* thread-safe.


#ifndef IMPALA_RUNTIME_CHANNEL_H
#define IMPALA_RUNTIME_CHANNEL_H

#include "runtime/row-batch.h"
#include "runtime/client-cache.h"

#include <boost/bind.hpp>
#include <boost/thread.hpp>
#include <boost/scoped_ptr.hpp>
#include <boost/thread/mutex.hpp>
#include <boost/thread/condition_variable.hpp>

#include "gen-cpp/Types_types.h"
#include "gen-cpp/ImpalaInternalService.h"
#include "gen-cpp/ImpalaInternalService_types.h"

#include "util/thread-pool.h"

namespace impala {
class DataStreamSender;
class Channel {
 public:
  // Create channel to send data to particular ipaddress/port/query/node
  // combination. buffer_size is specified in bytes and a soft limit on
  // how much tuple data is getting accumulated before being sent; it only applies
  // when data is added via AddRow() and not sent directly via SendBatch().
  Channel(DataStreamSender* parent, const RowDescriptor& row_desc,
          const TNetworkAddress& destination, const TUniqueId& fragment_instance_id,
          PlanNodeId dest_node_id, int buffer_size)
    : parent_(parent),
      buffer_size_(buffer_size),
      client_cache_(NULL),
      row_desc_(row_desc),
      address_(MakeNetworkAddress(destination.hostname, destination.port)),
      fragment_instance_id_(fragment_instance_id),
      dest_node_id_(dest_node_id),
      num_data_bytes_sent_(0),
      rpc_thread_("DataStreamSender", "SenderThread", 1, 1,
          boost::bind<void>(boost::mem_fn(&Channel::TransmitData), this, _1, _2)),
      rpc_in_flight_(false) {
  }

  // Initialize channel.
  // Returns OK if successful, error indication otherwise.
  Status Init(RuntimeState* state);

  // Copies a single row into this channel's output buffer and flushes buffer
  // if it reaches capacity.
  // Returns error status if any of the preceding rpcs failed, OK otherwise.
  Status AddRow(TupleRow* row);

  // Asynchronously sends a row batch.
  // Returns the status of the most recently finished TransmitData
  // rpc (or OK if there wasn't one that hasn't been reported yet).
  Status SendBatch(TRowBatch* batch);

  // Return status of last TransmitData rpc (initiated by the most recent call
  // to either SendBatch() or SendCurrentBatch()).
  Status GetSendStatus();

  // Waits for the rpc thread pool to finish the current rpc.
  void WaitForRpc();

  // Flush buffered rows and close channel.
  // Logs errors if any of the preceding rpcs failed.
  void Close(RuntimeState* state);

  int64_t num_data_bytes_sent() const { return num_data_bytes_sent_; }
  TRowBatch* thrift_batch() { return &thrift_batch_; }

 private:
  DataStreamSender* parent_;
  int buffer_size_;

  ImpalaInternalServiceClientCache* client_cache_;

  const RowDescriptor& row_desc_;
  TNetworkAddress address_;
  TUniqueId fragment_instance_id_;
  PlanNodeId dest_node_id_;

  // the number of TRowBatch.data bytes sent successfully
  int64_t num_data_bytes_sent_;

  // we're accumulating rows into this batch
  boost::scoped_ptr<RowBatch> batch_;
  TRowBatch thrift_batch_;

  // We want to reuse the rpc thread to prevent creating a thread per rowbatch.
  // TODO: currently we only have one batch in flight, but we should buffer more
  // batches. This is a bit tricky since the channels share the outgoing batch
  // pointer we need some mechanism to coordinate when the batch is all done.
  // TODO: if the order of row batches does not matter, we can consider increasing
  // the number of threads.
  ThreadPool<TRowBatch*> rpc_thread_; // sender thread.
  boost::condition_variable rpc_done_cv_;   // signaled when rpc_in_flight_ is set to true.
  boost::mutex rpc_thread_lock_; // Lock with rpc_done_cv_ protecting rpc_in_flight_
  bool rpc_in_flight_;  // true if the rpc_thread_ is busy sending.

  Status rpc_status_;  // status of most recently finished TransmitData rpc

  // Serialize batch_ into thrift_batch_ and send via SendBatch().
  // Returns SendBatch() status.
  Status SendCurrentBatch();

  // Synchronously call TransmitData() on a client from client_cache_ and update
  // rpc_status_ based on return value (or set to error if RPC failed).
  // Called from a thread from the rpc_thread_ pool.
  void TransmitData(int thread_id, const TRowBatch*);
  void TransmitDataHelper(const TRowBatch*);

  Status CloseInternal();
};

}

#endif
