// Copyright 2015 GISTIC.

#include "runtime/spatial-data-stream-sender.h"

#include <iostream>
#include <boost/shared_ptr.hpp>
#include <thrift/protocol/TDebugProtocol.h>

#include "common/logging.h"
#include "exprs/expr.h"
#include "exprs/expr-context.h"
#include "runtime/descriptors.h"
#include "runtime/tuple-row.h"
#include "runtime/row-batch.h"
#include "runtime/raw-value.h"
#include "runtime/runtime-state.h"
#include "runtime/client-cache.h"
#include "runtime/mem-tracker.h"
#include "util/debug-util.h"
#include "util/network-util.h"
#include "rpc/thrift-client.h"
#include "rpc/thrift-util.h"

#include "gen-cpp/Types_types.h"
#include "gen-cpp/ImpalaInternalService.h"
#include "gen-cpp/ImpalaInternalService_types.h"

using namespace std;
using namespace boost;
using namespace apache::thrift;
using namespace apache::thrift::protocol;
using namespace apache::thrift::transport;
using namespace impala;
using namespace spatialimpala;

SpatialDataStreamSender::SpatialDataStreamSender(ObjectPool* pool, int sender_id,
    const RowDescriptor& row_desc, const TDataStreamSink& sink,
    const std::vector<TPlanFragmentDestination>& destinations,
    int per_channel_buffer_size)
    : DataStreamSender(pool, sender_id, row_desc, sink, destinations, per_channel_buffer_size) {
  this->partitions_ = sink.output_partition.intersected_partitions;
}

SpatialDataStreamSender::~SpatialDataStreamSender() {
}

Status SpatialDataStreamSender::Send(RuntimeState* state, RowBatch* batch, bool eos) {
  SCOPED_TIMER(profile_->total_time_counter());
  DCHECK(!closed_);
  if (broadcast_ || channels_.size() == 1) {
    // current_thrift_batch_ is *not* the one that was written by the last call
    // to Serialize()
    SerializeBatch(batch, current_thrift_batch_, channels_.size());
    // SendBatch() will block if there are still in-flight rpcs (and those will
    // reference the previously written thrift batch)
    for (int i = 0; i < channels_.size(); ++i) {
      RETURN_IF_ERROR(channels_[i]->SendBatch(current_thrift_batch_));
    }
    current_thrift_batch_ =
        (current_thrift_batch_ == &thrift_batch1_ ? &thrift_batch2_ : &thrift_batch1_);
  } else if (random_) {
    // Round-robin batches among channels. Wait for the current channel to finish its
    // rpc before overwriting its batch.
    Channel* current_channel = channels_[current_channel_idx_];
    current_channel->WaitForRpc();
    SerializeBatch(batch, current_channel->thrift_batch());
    current_channel->SendBatch(current_channel->thrift_batch());
    current_channel_idx_ = (current_channel_idx_ + 1) % channels_.size();
  } else {
    // hash-partition batch's rows across channels
    int num_channels = channels_.size();
    for (int r = 0; r < batch->num_rows(); ++r) {
      TupleRow* row = batch->GetRow(r);
      // It should be only one partition expr.
      for (int i = 0; i < partition_expr_ctxs_.size(); ++i) {
        ExprContext* ctx = partition_expr_ctxs_[i];
        string partition_val(reinterpret_cast<char*>(ctx->GetStringVal(row).ptr));
        std::map<string, std::vector<std::string> >::iterator it
            = partitions_.find(partition_val);
        if (it == partitions_.end()) break;
        std::vector<std::string> partitions_values = it->second;
        std::set<int> channel_values_set;
        std::set<int>::iterator set_it;
        for (int j = 0; j < partitions_values.size(); j++) {
          StringValue v(const_cast<char*>(partitions_values[j].c_str()),
              partitions_values[j].size());
          // We can't use the crc hash function here because it does not result
          // in uncorrelated hashes with different seeds.  Instead we must use
          // fnv hash.
          // TODO: fix crc hash/GetHashValue()
          uint32_t hash_val = RawValue::GetHashValueFnv(&v, ctx->root()->type(), HashUtil::FNV_SEED);
          channel_values_set.insert(hash_val % num_channels);
        }

        for (set_it = channel_values_set.begin(); set_it != channel_values_set.end(); set_it++) {
          RETURN_IF_ERROR(channels_[*set_it]->AddRow(row));
        }
        channel_values_set.clear();
      }
    }
  }
  return Status::OK();
}
