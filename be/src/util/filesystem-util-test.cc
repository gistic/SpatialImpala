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

#include "filesystem-util.h"

#include <boost/filesystem.hpp>
#include <gtest/gtest.h>
#include <sys/stat.h>

#include "common/init.h"
#include "common/logging.h"
#include "util/test-info.h"

#include "common/names.h"

using namespace impala;
namespace filesystem = boost::filesystem;
using filesystem::path;

TEST(FilesystemUtil, rlimit) {
  ASSERT_LT(0ul, FileSystemUtil::MaxNumFileHandles());
}

TEST(FilesystemUtil, CreateDirectory) {
  // Setup a temporary directory with one subdir
  path dir = filesystem::unique_path();
  path subdir1 = dir / "impala1";
  path subdir2 = dir / "impala2";
  path subdir3 = dir / "a" / "longer" / "path";
  filesystem::create_directories(subdir1);
  // Test error cases by removing write permissions on root dir to prevent
  // creation/deletion of subdirs
  chmod(dir.string().c_str(), 0);
  EXPECT_FALSE(FileSystemUtil::CreateDirectory(subdir1.string()).ok());
  EXPECT_FALSE(FileSystemUtil::CreateDirectory(subdir2.string()).ok());
  // Test success cases by adding write permissions back
  chmod(dir.string().c_str(), S_IRWXU);
  EXPECT_TRUE(FileSystemUtil::CreateDirectory(subdir1.string()).ok());
  EXPECT_TRUE(FileSystemUtil::CreateDirectory(subdir2.string()).ok());
  // Check that directories were created
  EXPECT_TRUE(filesystem::exists(subdir1) && filesystem::is_directory(subdir1));
  EXPECT_TRUE(filesystem::exists(subdir2) && filesystem::is_directory(subdir2));
  // Exercise VerifyIsDirectory
  EXPECT_TRUE(FileSystemUtil::VerifyIsDirectory(subdir1.string()).ok());
  EXPECT_TRUE(FileSystemUtil::VerifyIsDirectory(subdir2.string()).ok());
  EXPECT_FALSE(FileSystemUtil::VerifyIsDirectory(subdir3.string()).ok());
  // Check that nested directories can be created
  EXPECT_TRUE(FileSystemUtil::CreateDirectory(subdir3.string()).ok());
  EXPECT_TRUE(filesystem::exists(subdir3) && filesystem::is_directory(subdir3));
  // Cleanup
  filesystem::remove_all(dir);
}

int main(int argc, char** argv) {
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
