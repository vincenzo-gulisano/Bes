#include "common_NativeSleep.h"

JNIEXPORT void JNICALL Java_common_NativeSleep_sleep(JNIEnv * env, jclass class, jint nanoseconds)
{
  usleep(nanoseconds);
}


