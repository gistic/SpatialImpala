#!/usr/bin/env bash
# Copyright 2014 GISTIC.

INSTALL_PACKAGES=1
CLEAN_ACTION=1
TEST_ACTION=1
BUILD_THIRDPARTY=1
FORMAT_METASTORE=0
BUILD_IMPALA_ARGS=""

ROOT=`dirname "$0"`
ROOT=`cd "$ROOT"; pwd`

export IMPALA_HOME=$ROOT

# Exit on reference to uninitialized variable
set -u

# Exit on non-zero return value
set -e

# Always run in debug mode
set -x

# parse command line options
for ARG in $*
do
  case "$ARG" in
    -noclean)
      CLEAN_ACTION=0
      BUILD_IMPALA_ARGS="${BUILD_IMPALA_ARGS} -noclean"
      ;;
    -notests)
      TESTS_ACTION=0
      BUILD_IMPALA_ARGS="${BUILD_IMPALA_ARGS} -notests"
      ;;
    -format_metastore)
      FORMAT_METASTORE=1
      BUILD_IMPALA_ARGS="${BUILD_IMPALA_ARGS} -format_metastore"
      ;;
    -nopackages)
      INSTALL_PACKAGES=0
      ;;
    -nothirdparty)
      BUILD_THIRDPARTY=0
      ;;
    -help|*)
      echo "buildimpala.sh - Setups the environment for Impala, builds Impala and runs all tests."
      echo "[-noclean] : Omits cleaning all packages before building. Will not kill"\
           "running Hadoop service."
      echo "[-notests] : Skips building and execution of all tests"
      echo "[-format_metastore] : Formats the hive metastore and recreates it"
      echo "[-nopackages] : Skips installing needed packages for impala,"\
           " used if they're already installed"
      echo "[-nothirdparty] : Skips building thirdparty libraries"
      echo "-----------------------------------------------------------------------------
Examples of common tasks:

  # Setup environment, build and run all tests
  ./buildimpala.sh

  # Setup environment, build and doens't run tests
  ./buildall.sh -notests

  # Build with no cleaning and doens't run tests
  # -noclean is useless if -nothirdparty isn't provided
  ./buildall.sh -nopackages -noclean -nothirdparty -notests"
      exit 1
      ;;
  esac
done

if [ $INSTALL_PACKAGES -eq 1 ]
then
  echo "Installing packages needed for impala"
  apt-get install \
     build-essential automake libtool flex bison \
     git subversion \
     unzip \
     libboost-test-dev libboost-program-options-dev libboost-filesystem-dev libboost-system-dev \
     libboost-regex-dev libboost-thread-dev \
     protobuf-compiler \
     libsasl2-dev \
     libbz2-dev \
     libevent1-dev \
     pkg-config \
     doxygen \
     python-setuptools \
     libssl-dev \
     python-dev

  echo "Installing cmake."
  apt-get install cmake
  
  echo "Installing/Updating protobuf."
  add-apt-repository ppa:chris-lea/protobuf
  apt-get update
  apt-get install libprotoc-dev protobuf-compiler libprotoc7

  echo "Installing and building LLVM-COMPILER."
  wget http://llvm.org/releases/3.3/llvm-3.3.src.tar.gz
  tar xvzf llvm-3.3.src.tar.gz
  cd llvm-3.3.src/tools
  svn co http://llvm.org/svn/llvm-project/cfe/tags/RELEASE_33/final/ clang
  cd ../projects
  svn co http://llvm.org/svn/llvm-project/compiler-rt/tags/RELEASE_33/final/ compiler-rt
  cd ..
  mkdir ~/contrib
  ./configure --with-pic --prefix=$HOME/contrib/
  make -j4 REQUIRES_RTTI=1
  make install
  cd ..

  echo "Installing and configuring Java 7."
  add-apt-repository ppa:webupd8team/java
  apt-get update
  apt-get install oracle-java7-installer

  echo "Installing and configuring Maven."
  apt-get install maven

  echo "Installing postgreSQL."
  apt-get install postgresql-client
fi


export PATH=$PATH:$HOME/contrib/bin/
export JAVA_HOME=/usr/lib/jvm/java-7-oracle
export M2_HOME=/usr/share/maven

. $IMPALA_HOME/bin/impala-config.sh

if [ $BUILD_THIRDPARTY -eq 1 ]
then
  echo "Building third-party libraries ..."
  $IMPALA_HOME/bin/build_thirdparty.sh
fi

mkdir -p /var/lib/hadoop-hdfs

echo "Building Impala ..."
cd $IMPALA_HOME
./buildall.sh $BUILD_IMPALA_ARGS
