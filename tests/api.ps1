# 64 位安全读取 DLL 导出表
Add-Type -Language CSharp -TypeDefinition @"
using System;
using System.Collections.Generic;
using System.Runtime.InteropServices;

public class PeExport
{
    [DllImport("kernel32.dll", SetLastError = true, CharSet = CharSet.Auto)]
    static extern IntPtr LoadLibraryEx(string lpFileName, IntPtr hFile, uint dwFlags);
    const uint LOAD_LIBRARY_AS_DATAFILE = 0x00000002;

    public static IEnumerable<string> GetExports(string dllPath)
    {
        IntPtr hMod = LoadLibraryEx(dllPath, IntPtr.Zero, LOAD_LIBRARY_AS_DATAFILE);
        if (hMod == IntPtr.Zero)
            throw new System.ComponentModel.Win32Exception();

        int size;
        IntPtr pExportDir = NativeApi.ImageDirectoryEntryToDataEx(
                                hMod, false, NativeApi.IMAGE_DIRECTORY_ENTRY_EXPORT, out size);
        if (pExportDir == IntPtr.Zero)
            yield break;

        var exp = Marshal.PtrToStructure<NativeApi.IMAGE_EXPORT_DIRECTORY>(pExportDir);
        IntPtr pNames = hMod + (int)exp.AddressOfNames;
        for (uint i = 0; i < exp.NumberOfNames; i++)
        {
            int nameRva = Marshal.ReadInt32(pNames, (int)(i * 4));
            yield return Marshal.PtrToStringAnsi(hMod + nameRva);
        }
    }

    private static class NativeApi
    {
        public const int IMAGE_DIRECTORY_ENTRY_EXPORT = 0;
        [DllImport("dbghelp.dll", SetLastError = true)]
        public static extern IntPtr ImageDirectoryEntryToDataEx(
            IntPtr Base, bool MappedAsImage, int DirectoryEntry, out int Size);

        [StructLayout(LayoutKind.Sequential)]
        public struct IMAGE_EXPORT_DIRECTORY
        {
            public uint Characteristics;
            public uint TimeDateStamp;
            public ushort MajorVersion;
            public ushort MinorVersion;
            public uint Name;
            public uint Base;
            public uint NumberOfFunctions;
            public uint NumberOfNames;
            public uint AddressOfFunctions;
            public uint AddressOfNames;
            public uint AddressOfNameOrdinals;
        }
    }
}
"@

# ---- 调用 ----
[PeExport]::GetExports("D:\app\TAF\ITDT\LtfsApi10.dll") |
    Out-File -Encoding UTF8 D:\app\TAF\exports.txt

Get-Content D:\app\TAF\exports.txt