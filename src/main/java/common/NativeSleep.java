package common;

public class NativeSleep 
{
    static { System.loadLibrary("nativesleep"); }
    public static native void sleep(int nanoseconds);
    
}
