// Copyright 2015 Cloudera Inc.
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


#ifndef IMPALA_UTIL_LINEAGE_H
#define IMPALA_UTIL_LINEAGE_H


#include <rapidjson/rapidjson.h>
#include <rapidjson/stringbuffer.h>
#include <rapidjson/writer.h>

#include "common/logging.h"
#include "gen-cpp/LineageGraph_types.h"

namespace impala {

/// Utility class to serialize column lineage graphs to JSON
class LineageUtil {
  private:
    /// Serializes a TVertex object to JSON
    static string TVertexToJSON(TVertex vertex) {
      rapidjson::StringBuffer buffer;
      rapidjson::Writer<rapidjson::StringBuffer> writer(buffer);
      writer.StartObject();
      writer.String("id");
      writer.Int64(vertex.id);
      writer.String("vertexType");
      writer.String("COLUMN");
      writer.String("vertexId");
      writer.String(vertex.label.c_str());
      writer.EndObject();
      // Return a string representation of json
      return buffer.GetString();
    }

    /// Serializes a TMultiEdge object to JSON
    static string TMultiEdgeToJSON(TMultiEdge obj) {
      rapidjson::StringBuffer buffer;
      rapidjson::Writer<rapidjson::StringBuffer> writer(buffer);
      writer.StartObject();
      // Write source vertices
      writer.String("sources");
      writer.StartArray();
      for(int i=0; i < obj.sources.size(); ++i) {
        writer.Int64(obj.sources[i].id);
      }
      writer.EndArray();
      // Write target vertices
      writer.String("targets");
      writer.StartArray();
      for(int i=0; i < obj.targets.size(); ++i) {
        writer.Int64(obj.targets[i].id);
      }
      writer.EndArray();
      // Write edgetype
      writer.String("edgeType");
      string edge_type =
          (obj.edgetype == TEdgeType::PROJECTION) ? "PROJECTION" : "PREDICATE";
      writer.String(edge_type.c_str());
      writer.EndObject();
      return buffer.GetString();
    }

  public:
    /// Serializes a TLineageGraph object to JSON
    static string TLineageToJSON(TLineageGraph lineage) {
      rapidjson::StringBuffer buffer;
      rapidjson::Writer<rapidjson::StringBuffer> writer(buffer);
      writer.StartObject();
      writer.String("queryText");
      writer.String(lineage.query_text.c_str());
      writer.String("hash");
      writer.String(lineage.hash.c_str());
      writer.String("user");
      writer.String(lineage.user.c_str());
      // write query start time
      writer.String("timestamp");
      writer.Int64(lineage.started);
      // write query end time
      writer.String("endTime");
      DCHECK(lineage.ended >= lineage.started);
      writer.Int64(lineage.ended);
      // Write edges
      writer.String("edges");
      writer.StartArray();
      for(int i=0; i < lineage.edges.size(); ++i) {
        writer.String(TMultiEdgeToJSON(lineage.edges[i]).c_str());
      }
      writer.EndArray();
      // Write vertices
      writer.String("vertices");
      writer.StartArray();
      for(int i=0; i < lineage.vertices.size(); ++i) {
        writer.String(TVertexToJSON(lineage.vertices[i]).c_str());
      }
      writer.EndArray();
      writer.EndObject();
      return buffer.GetString();
    }

};

}
#endif
