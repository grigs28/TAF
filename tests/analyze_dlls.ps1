# 分析 ITDT 目录下的 DLL 文件功能
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

# 要分析的 DLL 文件列表
$dllFiles = @(
    "D:\app\TAF\ITDT\LtfsApi10.dll",
    "D:\app\TAF\ITDT\LtfsCmdLib.dll",
    "D:\app\TAF\ITDT\LtfsMgmtLib.dll",
    "D:\app\TAF\ITDT\libltfs.dll",
    "D:\app\TAF\ITDT\LTFSShellEx.dll",
    "D:\app\TAF\ITDT\PanelCommon.dll",
    "D:\app\TAF\ITDT\ltfsusr.dll"
)

$outputFile = "D:\app\TAF\dll_analysis.txt"
"=== ITDT DLL Function Analysis ===" | Out-File -Encoding UTF8 $outputFile
"Generated: $(Get-Date -Format 'yyyy-MM-dd HH:mm:ss')" | Out-File -Encoding UTF8 -Append $outputFile
"" | Out-File -Encoding UTF8 -Append $outputFile

foreach ($dll in $dllFiles) {
    if (Test-Path $dll) {
        Write-Host "Analyzing: $dll"
        "`n========================================" | Out-File -Encoding UTF8 -Append $outputFile
        "DLL: $(Split-Path $dll -Leaf)" | Out-File -Encoding UTF8 -Append $outputFile
        "Path: $dll" | Out-File -Encoding UTF8 -Append $outputFile
        "========================================" | Out-File -Encoding UTF8 -Append $outputFile
        
        try {
            $exports = [PeExport]::GetExports($dll)
            $exportList = $exports | Sort-Object
            
            if ($exportList.Count -gt 0) {
                "Exported Functions Count: $($exportList.Count)" | Out-File -Encoding UTF8 -Append $outputFile
                "" | Out-File -Encoding UTF8 -Append $outputFile
                "Exported Functions:" | Out-File -Encoding UTF8 -Append $outputFile
                $exportList | ForEach-Object { "  - $_" } | Out-File -Encoding UTF8 -Append $outputFile
            } else {
                "No exported functions (may be resource DLL or static library)" | Out-File -Encoding UTF8 -Append $outputFile
            }
        } catch {
            "Error: $_" | Out-File -Encoding UTF8 -Append $outputFile
        }
    } else {
        Write-Host "File not found: $dll"
    }
}

Write-Host "`nAnalysis complete! Results saved to: $outputFile"
Get-Content $outputFile
