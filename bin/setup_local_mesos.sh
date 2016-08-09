#!/bin/bash
if [ $(uname) = "Darwin" ]; then
brew install wget git autoconf automake libtool subversion maven
cd _dependencies/mesos-0.25.0/
# At least needed for El Capitan
patch -p1 <<EOF
commit bd40ba4f9bcb4f0b9b5d5a661304ded2ab6c6574
Author: Till Toenshoff <toenshoff@me.com>
Date:   Thu Oct 22 10:59:50 2015 +0200

    Added prevention of SASL deprecation warnings all around its invocations on OS X.

diff --git a/configure.ac b/configure.ac
index 66f9b32..35f2908 100644
--- a/configure.ac
+++ b/configure.ac
@@ -751,6 +751,14 @@ cat <<__EOF__ >crammd5_installed.c [
 #include <sasl/saslplug.h>
 #include <sasl/saslutil.h>

+// We need to disable the deprecation warnings as Apple has decided
+// to deprecate all of CyrusSASL's functions with OS 10.11
+// (see MESOS-3030). We are using GCC pragmas also for covering clang.
+#ifdef __APPLE__
+#pragma GCC diagnostic push
+#pragma GCC diagnostic ignored "-Wdeprecated-declarations"
+#endif
+
 static void search_crammd5(
   client_sasl_mechanism_t *m,
   sasl_info_callback_stage_t stage,
@@ -775,6 +783,10 @@ int main(int argc, char** argv)
   sasl_client_plugin_info(NULL, &search_crammd5, NULL);
   return 0;
 }]
+
+#ifdef __APPLE__
+#pragma GCC diagnostic pop
+#endif
 __EOF__

 # Build the SASL client check test binary. Prevent the use of possibly
diff --git a/src/authentication/cram_md5/authenticatee.cpp b/src/authentication/cram_md5/authenticatee.cpp
index b03b44a..0ce57f4 100644
--- a/src/authentication/cram_md5/authenticatee.cpp
+++ b/src/authentication/cram_md5/authenticatee.cpp
@@ -36,6 +36,14 @@

 #include "messages/messages.hpp"

+// We need to disable the deprecation warnings as Apple has decided
+// to deprecate all of CyrusSASL's functions with OS 10.11
+// (see MESOS-3030). We are using GCC pragmas also for covering clang.
+#ifdef __APPLE__
+#pragma GCC diagnostic push
+#pragma GCC diagnostic ignored "-Wdeprecated-declarations"
+#endif
+
 namespace mesos {
 namespace internal {
 namespace cram_md5 {
@@ -406,3 +414,7 @@ Future<bool> CRAMMD5Authenticatee::authenticate(
 } // namespace cram_md5 {
 } // namespace internal {
 } // namespace mesos {
+
+#ifdef __APPLE__
+#pragma GCC diagnostic pop
+#endif
diff --git a/src/authentication/cram_md5/authenticator.cpp b/src/authentication/cram_md5/authenticator.cpp
index f238872..ad95033 100644
--- a/src/authentication/cram_md5/authenticator.cpp
+++ b/src/authentication/cram_md5/authenticator.cpp
@@ -41,6 +41,14 @@

 #include "messages/messages.hpp"

+// We need to disable the deprecation warnings as Apple has decided
+// to deprecate all of CyrusSASL's functions with OS 10.11
+// (see MESOS-3030). We are using GCC pragmas also for covering clang.
+#ifdef __APPLE__
+#pragma GCC diagnostic push
+#pragma GCC diagnostic ignored "-Wdeprecated-declarations"
+#endif
+
 namespace mesos {
 namespace internal {
 namespace cram_md5 {
@@ -556,3 +564,7 @@ Future<Option<string>> CRAMMD5Authenticator::authenticate(
 } // namespace cram_md5 {
 } // namespace internal {
 } // namespace mesos {
+
+#ifdef __APPLE__
+#pragma GCC diagnostic pop
+#endif
diff --git a/src/authentication/cram_md5/auxprop.cpp b/src/authentication/cram_md5/auxprop.cpp
index abf0f8d..0e66dae 100644
--- a/src/authentication/cram_md5/auxprop.cpp
+++ b/src/authentication/cram_md5/auxprop.cpp
@@ -22,6 +22,14 @@

 #include "logging/logging.hpp"

+// We need to disable the deprecation warnings as Apple has decided
+// to deprecate all of CyrusSASL's functions with OS 10.11
+// (see MESOS-3030). We are using GCC pragmas also for covering clang.
+#ifdef __APPLE__
+#pragma GCC diagnostic push
+#pragma GCC diagnostic ignored "-Wdeprecated-declarations"
+#endif
+
 using std::list;
 using std::string;

@@ -34,7 +42,6 @@ Multimap<string, Property> InMemoryAuxiliaryPropertyPlugin::properties;
 sasl_auxprop_plug_t InMemoryAuxiliaryPropertyPlugin::plugin;
 std::mutex InMemoryAuxiliaryPropertyPlugin::mutex;

-
 int InMemoryAuxiliaryPropertyPlugin::initialize(
     const sasl_utils_t* utils,
     int api,
@@ -205,3 +212,7 @@ int InMemoryAuxiliaryPropertyPlugin::initialize(
 } // namespace cram_md5 {
 } // namespace internal {
 } // namespace mesos {
+
+#ifdef __APPLE__
+#pragma GCC diagnostic pop
+#endif
EOF
fi
./bootstrap
mkdir build
cd build
../configure
make
