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

struct Rect{  
  
  double x1;
  double y1; 
  double x2; 
  double y2;

    Rect (double x1, double y1, double x2, double y2) {
      this->x1 = x1;
      this->y1 = y1;
      this->x2 = x2;
      this->y2 = y2;
    }
};

typedef Rect* RectPtr;

int compareFunc (const void * val1, const void * val2)
{
  Rect* rect1 = * (Rect **) val1;
  Rect* rect2 = * (Rect **) val2; 
  
    return ( rect1->x1 - rect2->x1 );
}

bool isIntersected(Rect* r, Rect* s){
  return (s->x2 > r->x1 
    && r->x2 > s->x1 
    && s->y2 > r->y1 
    && r->y2 > s->y1);
}

void OverlappedInit(FunctionContext* context, StringVal* val1) {
  val1->is_null = true;

  ofstream myfile;
  myfile.open ("./spatialJoin.out");
  myfile << "\nStarting collecting ....\n\n ";
  myfile.close();

}

const static char defaultHeader[] = "0000000011#";
const static int headerSize = strlen(defaultHeader);

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

  int MEMORY_ALLOCATED_RECORDS = 500000;

  int recordSize = record.size();
  
  if (val->is_null) {
    val->is_null = false;
    *val = StringVal(context, recordSize * MEMORY_ALLOCATED_RECORDS);     
    strcpy((char*)val->ptr, defaultHeader);       
  } //else {
    /*int valLen = strlen((char*)val->ptr);

    if( (valLen/recordSize) % 10000 == 0 ){
      ofstream myfile;
      myfile.open ("./spatialJoin.out");
      myfile << "Records Received " << (valLen/recordSize) ;
      myfile.close();
    }

    if ((valLen + recordSize) > val->len) {
      int new_len = val->len + recordSize * MEMORY_ALLOCATED_RECORDS;
      StringVal new_val(context, new_len);
      strcat((char*)new_val.ptr, (char*)val->ptr);
      strcat((char*)new_val.ptr, (char*)record.c_str());
      
      //delete[] val->ptr;
      *val = new_val;
    }
    else {*/
  
  int lengthFieldSize = headerSize - 1;

  char totalLength_str[lengthFieldSize+1];
  
  memcpy(totalLength_str, (char*) val->ptr, sizeof(char) * lengthFieldSize);

  int totalLength = atoi(totalLength_str);

  strcat((char*)(val->ptr + totalLength), (char*)record.c_str());

  totalLength += recordSize;
  
  stringstream temp;
  temp << totalLength;
  const char* newLength_str = temp.str().c_str();

  int totalLength_str_len = strlen(newLength_str);

  memcpy((char*)(val->ptr + (lengthFieldSize - totalLength_str_len)), newLength_str, totalLength_str_len);

  if( (((totalLength-headerSize))/recordSize) % 10000 == 0 ){ 
      ofstream myfile;
      myfile.open ("./spatialJoin.out");
      myfile << "Records Received " << ((totalLength-headerSize)/recordSize) << "\n";
      myfile.close();
  }

  //printf("State : %s\n", val->ptr ); 
  //printf("New Record : %s\n", record.c_str());
  //printf("Record size : %d\n", recordSize ); 
  //printf("Records received : %d\n", ((totalLength-headerSize)/recordSize) ); 
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
  
  if(*(src.ptr + headerSize - 1) == '#')
    strcat((char*)new_val.ptr, (char*)(src.ptr + headerSize));
  else
    strcat((char*)new_val.ptr, (char*)src.ptr );
  
  if(*(dst->ptr + headerSize - 1) == '#')
    strcat((char*)new_val.ptr, (char*)(dst->ptr + headerSize));
  else
    strcat((char*)new_val.ptr, (char*)dst->ptr );

  //delete[] dst->ptr;
  *dst = new_val;
}

StringVal OverlappedFinalize(FunctionContext* context, const StringVal& val) {      
  
  char* records_str = (char*)val.ptr;

  double x1, y1, x2, y2;
  char* restOfRecords= NULL;
  char* colContext= NULL;
  int tableID;
  
  ofstream myfile;
  myfile.open ("./spatialJoin.out");
  myfile << "\nParsing ....\n\n ";

  int records_str_len = strlen((char*)records_str);

  char *record;
  char *field;
  int count = 0;

  Rect **list1 = NULL, **list2 = NULL;
  int list1Size = 0, list2Size = 0;

  try{
  
    while ((record = strtok_r(records_str, "/", &restOfRecords)) != NULL) {

    
      if(list1 == NULL) list1 = new RectPtr[records_str_len / (strlen(record)+1)];
      if(list2 == NULL) list2 = new RectPtr[records_str_len / (strlen(record)+1)];


      
      x1 = atof(strtok_r(record, ",", &colContext));     
      y1 = atof(strtok_r(NULL, ",", &colContext));      
      x2 = atof(strtok_r(NULL, ",", &colContext));      
      y2 = atof(strtok_r(NULL, ",", &colContext));      
      tableID = atoi(strtok_r(NULL, ",", &colContext));

      //printf("%f, %f, %f, %f, %d\n",x1,y1,x2,y2,tableID);     

      Rect *rect = new Rect(x1, y1, x2, y2);      

      if (tableID == 1) {               
          list1[list1Size++] = rect;
      } else if (tableID == 2) {                
          list2[list2Size++] = rect;
      }           
      
      records_str = restOfRecords;       
    

    }

    qsort(list1, list1Size, sizeof(list1[0]), compareFunc);
    qsort(list2, list2Size, sizeof(list2[0]), compareFunc);

    myfile << "\nSpatial Joining ....\n\n"; 
    myfile << "List 1 of size : " << list1Size << "\n";
    myfile << "List 2 of size : " << list2Size << "\n";

    Rect** R = list1;
    Rect** S = list2;

    int R_length = list1Size;
    int S_length = list2Size;

    int i = 0, j = 0;
    while (i < list1Size && j < list2Size) {
      Rect* r;
      Rect* s;
      if (compareFunc(&R[i], &S[j]) < 0) {
        r = R[i];
        int jj = j;

        while ((jj < S_length) && ((s = S[jj])->x1 <= r->x2)) {
          if (isIntersected(r, s)) count++;         
          jj++;
        }
        i++;        
      } else {
        s = S[j];
        int ii = i;

        while ((ii < R_length) && ((r = R[ii])->x1 <= s->x2)) {
          if (isIntersected(r,s)) count++;
          ii++;         
        }
        j++;
      }   

      if(i % 1000 == 0) 
        myfile << "List 1 : Processed " << i << " of " << list1Size << " Shapes\n";
       
    }

    myfile << "\nFinished processing, Count : " << count << "\n";

    for(int i=0; i < list1Size; i++) delete list1[i];
    for(int i=0; i < list2Size; i++) delete list2[i];

    delete list1;
    delete list2;
  
  }
  catch (int e)
  {
    myfile << "An exception occurred. Exception Nr. " << e << '\n';
  }

  stringstream countstr;
  countstr << count;
  StringVal intersectedRect = StringVal(context, countstr.str().size());
  memcpy(intersectedRect.ptr, countstr.str().c_str(), countstr.str().size());

  myfile.close();

  return intersectedRect;
}
