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

#include "uda-sample.h"
#include <assert.h>
#include <string>
#include <stdio.h>
#include <cstring>
#include <iostream>
#include <sstream>
#include <iomanip>
#include <fstream>
#include <stdlib.h>
using namespace impala_udf;
using namespace std;

// ---------------------------------------------------------------------------
// This is a sample of implementing a COUNT aggregate function.
// ---------------------------------------------------------------------------
void CountInit(FunctionContext* context, BigIntVal* val) {
  val->is_null = false;
  val->val = 0;
}

void CountUpdate(FunctionContext* context, const IntVal& input, BigIntVal* val) {
  if (input.is_null) return;
  ++val->val;
}

void CountMerge(FunctionContext* context, const BigIntVal& src, BigIntVal* dst) {
  dst->val += src.val;
}

BigIntVal CountFinalize(FunctionContext* context, const BigIntVal& val) {
  return val;
}

// ---------------------------------------------------------------------------
// This is a sample of implementing a AVG aggregate function.
// ---------------------------------------------------------------------------
struct AvgStruct {
  double sum;
  int64_t count;
};

void AvgInit(FunctionContext* context, BufferVal* val) {
  assert(sizeof(AvgStruct) == 16);
  memset(*val, 0, sizeof(AvgStruct));
}

void AvgUpdate(FunctionContext* context, const DoubleVal& input, BufferVal* val) {
  if (input.is_null) return;
  AvgStruct* avg = reinterpret_cast<AvgStruct*>(*val);
  avg->sum += input.val;
  ++avg->count;
}

void AvgMerge(FunctionContext* context, const BufferVal& src, BufferVal* dst) {
  if (src == NULL) return;
  const AvgStruct* src_struct = reinterpret_cast<const AvgStruct*>(src);
  AvgStruct* dst_struct = reinterpret_cast<AvgStruct*>(*dst);
  dst_struct->sum += src_struct->sum;
  dst_struct->count += src_struct->count;
}

DoubleVal AvgFinalize(FunctionContext* context, const BufferVal& val) {
  if (val == NULL) return DoubleVal::null();
  AvgStruct* val_struct = reinterpret_cast<AvgStruct*>(val);
  return DoubleVal(val_struct->sum / val_struct->count);
}

// ---------------------------------------------------------------------------
// This is a sample of implementing the STRING_CONCAT aggregate function.
// Example: select string_concat(string_col, ",") from table
// ---------------------------------------------------------------------------
void StringConcatInit(FunctionContext* context, StringVal* val) {
  val->is_null = true;
}

void StringConcatUpdate(FunctionContext* context, const StringVal& arg1,
    const StringVal& arg2, StringVal* val) {
  if (val->is_null) {
    val->is_null = false;
    *val = StringVal(context, arg1.len);
    memcpy(val->ptr, arg1.ptr, arg1.len);
  } else {
    int new_len = val->len + arg1.len + arg2.len;
    StringVal new_val(context, new_len);
    memcpy(new_val.ptr, val->ptr, val->len);
    memcpy(new_val.ptr + val->len, arg2.ptr, arg2.len);
    memcpy(new_val.ptr + val->len + arg2.len, arg1.ptr, arg1.len);
    *val = new_val;
  }
}

void StringConcatMerge(FunctionContext* context, const StringVal& src, StringVal* dst) {
  if (src.is_null) return;
  StringConcatUpdate(context, src, ",", dst);
}

StringVal StringConcatFinalize(FunctionContext* context, const StringVal& val) {
  return val;
}

// ---------------------------------------------------------------------------
// This is a sample of implementing the SUM aggregate function for decimals.
// Example: select sum_small_decimal(dec_col) from table
// It is different than the builtin sum since it can easily overflow but can
// be faster for small tables.
// ---------------------------------------------------------------------------
void SumSmallDecimalInit(FunctionContext*, DecimalVal* val) {
  val->is_null = true;
  val->val4 = 0;
}

void SumSmallDecimalUpdate(FunctionContext* ctx,
    const DecimalVal& src, DecimalVal* dst) {
  assert(ctx->GetArgType(0)->scale == 2);
  assert(ctx->GetArgType(0)->precision == 9);
  if (src.is_null) return;
  dst->is_null = false;
  dst->val4 += src.val4;
}

void SumSmallDecimalMerge(FunctionContext*, const DecimalVal& src, DecimalVal* dst) {
  if (src.is_null) return;
  dst->is_null = false;
  dst->val4 += src.val4;
}


//-------------------------------------------------------------------------
//Overlap aggregate function
//-------------------------------------------------------------------------

struct RectNode{
  RectangleVal rect;
  RectNode* next;
  RectNode (RectangleVal xrect) {
    rect = xrect;
    next = NULL;
  }
};

void OverlappedInit(FunctionContext* context, StringVal* val1) {
  val1->is_null = true;
}

void OverlappedUpdate(FunctionContext* context, const RectangleVal& arg1,
    const IntVal& arg3, StringVal* val) {
  if (arg1.is_null) return;
  stringstream recordStream;
  string record;
  recordStream << setprecision(15);
  recordStream << arg1.x1 <<",";
  recordStream << arg1.y1 <<",";
  recordStream << arg1.x2 <<",";
  recordStream << arg1.y2;
  
  recordStream<<","<<arg3.val<<"/";

  record = recordStream.str();

  int MEMORY_ALLOCATED_RECORDS = 10000;

  int recordSize = record.size();
  
  if (val->is_null) {
    val->is_null = false;
    *val = StringVal(context, recordSize * MEMORY_ALLOCATED_RECORDS);
    val->ptr[0] = '\0';
    strcat((char*)val->ptr, (char*)record.c_str());
  } else {
    int valLen = strlen((char*)val->ptr);
    if ((valLen + recordSize) > val->len) {
      int new_len = val->len + recordSize * MEMORY_ALLOCATED_RECORDS;
      StringVal new_val(context, new_len);
      strcat((char*)new_val.ptr, (char*)val->ptr);
      strcat((char*)new_val.ptr, (char*)record.c_str());
      
      //delete[] val->ptr;
      *val = new_val;
    }
    else {
      strcat((char*)val->ptr, (char*)record.c_str());
    }
  }
}

void OverlappedMerge(FunctionContext* context, const StringVal& src, StringVal* dst) {
  if (src.is_null) return;
  if (dst == NULL || dst->is_null) {
    StringVal new_val(context, strlen((char*)src.ptr) + 1);
    new_val.ptr[0] = '\0';
    strcat((char*)new_val.ptr, (char*)src.ptr);
    *dst = new_val;
    return;
  }
  int new_len = strlen((char*)src.ptr) + strlen((char*)dst->ptr);
  
  StringVal new_val(context, new_len + 1);
  new_val.ptr[0] = '\0';
  strcat((char*)new_val.ptr, (char*)src.ptr);
  strcat((char*)new_val.ptr, (char*)dst->ptr);
  //delete[] dst->ptr;
  *dst = new_val;
}

StringVal OverlappedFinalize(FunctionContext* context, const StringVal& val) {
  RectNode *list1Head, *list2Head;
  list1Head = list2Head = NULL;
  double x1, y1, x2, y2;
  char* recordContext= NULL;
  char* colContext= NULL;
  int tableID;
  StringVal finalString, tempString;
  
  ofstream myfile;
  //myfile.open ("/home/ahmed/test.txt");
  //myfile << (char*)val.ptr<<"\n";
  int stringLen = strlen((char*)val.ptr);
  char *record = strtok_r((char*)val.ptr, "/", &recordContext);
  char *col;
  int count = 0;
  while (stringLen > count) {
    
    count += strlen(record) + 1;
    //myfile << "stringLen = "<<stringLen<<"count = "<<count<<"\n";
    char *temprecord = new char[strlen(record) + 1];
    strcpy(temprecord, record);
    
    col = strtok_r(temprecord, ",", &colContext);
    x1 = atof(col);
   
    col = strtok_r(NULL, ",", &colContext);
    y1 = atof(col);
    
    col = strtok_r(NULL, ",", &colContext);
    x2 = atof(col);
    
    col = strtok_r(NULL, ",", &colContext);
    y2 = atof(col);
    
    col = strtok_r(NULL, ",", &colContext);
    tableID = atoi(col);
    
    
    RectNode *temp = new RectNode(RectangleVal(x1, y1, x2, y2));
    
    if (tableID == 1) {
      
      //myfile << "Record 1:"<<temp->rect.x1<<", "<<temp->rect.y1<<", "<<temp->rect.x2<<", "<<temp->rect.y2<<", "<<"\n";
      temp->next = list1Head;
      list1Head = temp;
    }
    else if (tableID == 2) {
      
      //myfile << "Record 2:"<<temp->rect.x1<<", "<<temp->rect.y1<<", "<<temp->rect.x2<<", "<<temp->rect.y2<<", "<<"\n";
      temp->next = list2Head;
      list2Head = temp;
    }
    delete[] temprecord;
    
    
    record = strtok_r(NULL, "/", &recordContext);
    
    
  }

  RectNode *temp1, *temp2;
  temp1 = list1Head;
  int intersected = 0;
  while (temp1) {
    temp2 = list2Head;
    //myfile << "Record 1:"<<temp1->rect.x1<<", "<<temp1->rect.y1<<", "<<temp1->rect.x2<<", "<<temp1->rect.y2<<", "<<"\n";
    while (temp2) {
      if (temp1->rect.isOverlappedWith(temp2->rect)) {
      //if(true) {
        intersected++;
      }
      temp2 = temp2->next;
    }
    temp1 = temp1->next;
  }
  //delete[] val.ptr;
  stringstream countstr;
  countstr<<intersected;
  StringVal intersectedRect = StringVal(context, countstr.str().size());
  memcpy(intersectedRect.ptr, countstr.str().c_str(), countstr.str().size());
  myfile.close();
  return intersectedRect;
}
