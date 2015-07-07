// Copyright 2015 GISTIC.

#ifndef IMPALA_RUNTIME_SPATIAL_DATA_STREAM_SENDER_H
#define IMPALA_RUNTIME_SPATIAL_DATA_STREAM_SENDER_H

#include <vector>
#include <string>

#include "runtime/data-stream-sender.h"

#include "exec/data-sink.h"
#include "common/global-types.h"
#include "common/object-pool.h"
#include "common/status.h"
#include "util/runtime-profile.h"
#include "gen-cpp/Results_types.h" // for TRowBatch

using namespace impala;

namespace spatialimpala {

// Single sender of an m:n data stream.
// Row batch data is routed to destinations based on the provided
// partitioning specification.
// *Not* thread-safe.
//
// TODO: capture stats that describe distribution of rows/data volume
// across channels.
class SpatialDataStreamSender : public DataStreamSender {
 public:
  // Construct a sender according to the output specification (sink),
  // sending to the given destinations. sender_id identifies this
  // sender instance, and is unique within a fragment.
  // Per_channel_buffer_size is the buffer size allocated to each channel
  // and is specified in bytes.
  // The RowDescriptor must live until Close() is called.
  // NOTE: supported partition types are UNPARTITIONED (broadcast), HASH_PARTITIONED,
  // and RANDOM.
  SpatialDataStreamSender(ObjectPool* pool, int sender_id,
    const RowDescriptor& row_desc, const TDataStreamSink& sink,
    const std::vector<TPlanFragmentDestination>& destinations,
    int per_channel_buffer_size);

  virtual ~SpatialDataStreamSender();

  // Send data in 'batch' to destination nodes according to partitioning
  // specification provided in c'tor.
  // Blocks until all rows in batch are placed in their appropriate outgoing
  // buffers (ie, blocks if there are still in-flight rpcs from the last
  // Send() call).
  virtual Status Send(RuntimeState* state, RowBatch* batch, bool eos);
};

}

#endif
