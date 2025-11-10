# IBM Tape Device Drivers Installation and User's Guide

# Contents

# IBM Tape Device Drivers and Diagnostic Tool User's Guide. 1

Introduction. 1Common extended features. 3Path failover and load balancing. 3Dynamic Runtime Attributes. 8Data encryption. 8Recommended access order (RAO) open function. 11AIX Tape and Medium Changer device driver. 11Product requirements. 12Installation and configuration instructions. 12Tape drive, media, and device driver parameters. 15Special files. 21Persistent Naming Support. 23Control Path failover support for tape libraries. 25Data Path failover and load balancing support for tape drives. 27System- managed encryption. 29Problem determination. 30Tape drive service aids. 36Performance considerations. 38Linux Tape and Medium Changer device driver. 39Product requirements. 40Installation and Configuration instructions. 40Tape drive, media, and device driver parameters. 45Special files. 51Control Path failover support for tape libraries. 53Data Path failover and load balancing support for tape drives. 55Open source device driver - lin_tape. 57Problem determination. 59Windows Tape and Medium Changer device driver. 64Product requirements. 65Installation and configuration instructions. 65Persistent Naming Support on Windows Server 2019, 2022 and 2025. 68Control Path failover support for tape libraries. 69Data Path failover support for tape drives. 69Problem determination. 71IBM Tape Diagnostic Tool (ITDT). 73Purpose. 73Accessing ITDT. 74Accessing documentation and software online. 75Supported systems. 77IBM Tape Diagnostic Tool - Standard Edition. 77IBM Tape Diagnostic Tool - Graphical Edition. 153Verifying correct attachment of your devices. 191Managing the microcode on the IBM tape drive. 191Notices. 192

# Index. 194

# IBM Tape Device Drivers and Diagnostic Tool User's Guide

Welcome to the IBM Tape Device Drivers and Diagnostic Tool User's Guide documentation, where you can find information about how to install, maintain, and use IBM Tape Device Drivers and the IBM Tape Diagnostic Tool (ITDT) on multiple platforms.Last updated: 2025- 10- 29.

Last updated:2025- 10- 29.

# Installation and User's Guide

"Introduction" on page 1 The IBM tape and medium changer device drivers are designed specifically to take advantage of the features that are provided by the IBM tape drives and medium changer devices.

"Common extended features" on page 3

"AIX Tape and Medium Changer device driver" on page 11

"Linux Tape and Medium Changer device driver" on page 39

"Windows Tape and Medium Changer device driver" on page 64

"IBM Tape Diagnostic Tool (ITDT)" on page 73

# Troubleshooting and support

"Verifying correct attachment of your devices" on page 191

"Managing the microcode on the IBM tape drive" on page 191

IBM tape storage support IBM Support home page

# More information

IBM Tape Device Drivers Programming Reference IBM Tape Device Drivers Installation and User's Guide (Legacy) IBM TS4500 tape library documentation IBM TS4300 tape library documentation IBM TS2900 tape autoloader documentation IBM TS2270 tape drive documentation IBM TS2280 tape drive documentation IBM Community (Community platform) IBM Support content (Product support) Redbooks home page

# Introduction

The IBM tape and medium changer device drivers are designed specifically to take advantage of the features that are provided by the IBM tape drives and medium changer devices.

The goal is to give applications access to the functions required for basic tape functions (such as backup and restore) and medium changer operations (such as cartridge mount and unmount), and to the advanced functions needed by full tape management systems. Whenever possible, the driver is designed to take advantage of the device features transparent to the application. IBM maintains the levels of device drivers and driver documentation for the drive on the Internet. You can access this material at http:// www.ibm.com/support/fixcentral.

# Hardware requirements

Hardware requirementsThe tape drivers and the IBM Tape Diagnostic Tool (ITDT) are developed to support various versions of different platforms. For the latest support, refer to the Interoperation Center website - IBM System Storage Interoperation Center (SSIC).

IBM Tape Device Drivers and Diagnostic Tool User's Guide 1

Note: A single Fibre Channel host bus adapter (HBA) for concurrent tape and disk operations is not recommended. Tape and disk devices require incompatible HBA settings for reliable operation and optimal performance characteristics. Under stress conditions (high I/O rates for either tape, disk, or both) where disk and tape subsystems share a common HBA, stability problems are observed. These issues are resolved by separating disk and tape I/O streams onto separate HBAs and by using SAN zoning to minimize contention. IBM is focused on assuring server/storage configuration interoperability. It is strongly recommended that your overall implementation plan includes provisions for separating disk and tape workloads.

For information about this issue, see the following Redbook, section 4.1.3 in http://www.redbooks.ibm.com/abstracts/sg246502. html?Open.

# Software requirements

Important: If you use a third- party application, consult with your application provider about the compatibility with IBM tape device drivers.

Industry- leading compatible software offerings provide storage and tape management software for the LTO tape drives. Supporting software and applications must be obtained separately from IBM, IBM Business Partners, or independent software vendors (ISV). For a list of compatible software and additional information, refer to the ISV Matrix that is available at Independent Software Vendor (ISV) matrix for 3592 and LTO.

# IBM tape products

The IBM tape product family provides an excellent solution for customers with small to large storage and performance requirements.

Remember: xx in the following list represents generation of the drive.

- IBM TS22xx Tape Drive- IBM TS2350/TS2360 Tape Drive- IBM TS11xx Tape Drive (Enterprise)- IBM Diamondback Tape Library- IBM TS4500 Tape Library (also known as IBM tape library 3584)- IBM TS4300 Tape Library- IBM TS2900 Tape Autoloader

The image illustrates the attachment of various current products to an open systems server.

![](images/79fe3f36bef406df03842c5740b8741ccf4a7f1a6e33af3a6e13fe0fa7793984.jpg)  
Figure 1. Current attachment array

1 Open Systems Server  2 IBM TS4300 Tape Library

3 IBM TS22xx Tape Drive

4 IBM TS2350/TS2360 (or 3580) Tape Drive  5 IBM TS11xx Tape Drive [also known as Enterprise]  6 IBM TS4500 tape library

# Common extended features

# Purpose

This chapter provides general information about the IBM device drivers, requirements, and advanced functionality.

# Path failover and load balancing

Device driver path failover support configures multiple physical paths to the same device within the device driver and provides two basic functions:

1. Automatic failover to an alternate physical path when a permanent error occurs on one path.  
2. Dynamic load balancing for tape devices by using multiple host bus adapters (HBA).

Path failover is supported on certain tape products with the latest IBM device drivers available on the following website - http://www.ibm.com/support/fixcentral. Instructions for downloading drivers can be found in "Accessing documentation and software online" on page 75. Some devices require a path failover feature code to be installed before path failover support is enabled in the device driver. Refer

to "Supported devices and feature codes" on page 4 for a list of supported devices and what path failover feature code is required for your machine type.

At startup or configuration, the system detects multiple logical devices of the tape drive. Each logical device is a physical path to the same tape drive. A backup and restore application can open and use only one logical device at a time because they represent the same physical device.

Without path failover support, if a permanent path error occurs (because of an HBA or cable failure, for example), the application fails. It is possible to initiate manual failover by restarting the application on the alternate logical device, but the application must be restarted from the beginning. A long backup or restore operation might be in progress when the path error occurred. Sometimes manual failover requires operator intervention to reset the drive because a SCSI Reservation might still exist on the failing HBA path.

When path failover support is enabled on all logical devices, the device driver configures them internally as a single device with multiple paths. The application can still open and use only one logical device at a time. If an application opens the primary device and a permanent path error occurs, the device driver initiates failover error recovery automatically on an alternate path. If successful, the current operation continues on an alternate path without interrupting the application. The data path failover error recovery first restores the previous device state, SCSI Reservation, and tape position. Then, it tries the failing operation again.

# Supported devices and feature codes

Path failover is supported only for the devices that are listed in Table 1 on page 4. Path failover includes Control Path failover (CPF) for tape libraries and Data Path failover (DPF) for tape drives. To use path failover support, some devices require feature codes as listed in Table 1 on page 4.

<table><tr><td colspan="2">Table 1. Supported devices and feature codes</td></tr><tr><td>Supported tape library/drive</td><td>Feature code (FC), if required</td></tr><tr><td>Diamondback Tape Library</td><td>Standard, no FC required (CPF and DPF)</td></tr><tr><td>TS4500</td><td>Standard, no FC required (DPF)
FC 1682 (CPF)</td></tr><tr><td>TS4300/LTO</td><td>FC 1682 (CPF and DPF)</td></tr></table>

# Notes:

1. Path failover is only supported on SAS devices that are attached to Windows and Linux for Intel/AMD processor-based servers. SAS is not supported on System p servers (AIX and Linux). 
2. If your device does not support path failover, you must disable this option in the device driver. See the specific platform section for driver default behavior and enable/disable failover instructions.

# Automatic failover

The automatic failover support provides error recovery on an alternate path when a permanent error occurs on the primary path. This support is transparent to the running application. The two types of path failover are Data Path failover (DPF) and Control Path failover (CPF). They are closely related. However, the difference is that DPF is an automatic failover support for the transfer of data, which provides error recovery for systems that are connected to tape drives. CPF is an automatic failover support for the transfer of commands to move tape cartridges. Examples of different configurations that can be constructed follow.

# Data path failover

The following flowcharts outline the different types of configurations for data path failover (DPF). These configurations are presented in order of best practices as recommended by IBM.

4 IBM Tape Device Drivers Installation and User's Guide

# Dual Host Bus Adapters (HBAs) to a multi-port drive

Consider a multipath connection that consists of two Host Bus Adapters (HBAs) connected through a fabric to a multi- port drive.

![](images/d9d8a1cf92ebe78219046cd96fe6f0602e089581cec66efe0752d6f7ece04c40.jpg)  
Figure 2. Dual HBA and multi-port drives

As seen in Figure 2 on page 5, four available paths are available between the drive and the host system. These paths are

HBA A to drive port 1 [A, p1]  HBA A to drive port 2 [A, p2]  HBA B to drive port 1 [B, p1]  HBA B to drive port 2 [B, p2]

One path is the primary path and the other three are alternate paths. If [A, p1] is the primary path and if HBA A fails, two valid paths ([B, p1] and [B, p2]) remain. The DPF tries to switch to one of the available configured paths. Conversely, if the cable to port 1 of the drive fails with [A, p1] as the primary path, two valid paths to the drive [A, p2] and [B, p2] are still available. Without DPF support, if a permanent path error occurs (because of HBA or cable failover, for example), the application fails. With DPF, if the permanent failure occurs with this configuration, two valid physical paths for the data are still available for transmitting data. The running application is not affected.

If the path that failed is restored, the device driver claims the path as available and uses it as a valid alternate path in most conditions. This action is dependent on Operating System and HBA behavior, not the IBM tape device driver behavior.

# Dual Host Bus Adapters (HBAs) to a single-port drive

Consider a multipath connection that consists of two HBAs connected through a fabric to a single- port device.

![](images/17f46a83191f70d08eedf4430093cc1fb04151fddeb1871f57c0152d1ac0fed4.jpg)  
Figure 3. Dual HBA and single-port drive

Figure 3. Dual HBA and single- port driveThis configuration supplies two physical paths to the same device. However, if the port or cable from the device fails, the automatic failover does not work. That connection is severed and a permanent path error occurs. If, however, the failure was with one of the HBAs or their cables, the automatic data path failover selects the other HBA. Then, the information continues through the alternate path. An example here is with the connections [A, p1] and [B, p1]. If [A, p1] is the primary path and a failure occurs with the HBA or HBA cable, then DPF automatically moves the connection to [B, p1] without affecting the application.

# Single Host Bus Adapters (HBA) to a multi-port drive

Consider a single path from the HBA through the fabric to a multi- port device.

![](images/6f526f22de099876e83c22cccddc445fabb6f657d1b0323bbf576df326faca7d.jpg)  
Figure 4. Single HBA and multi-port drive

Figure 4. Single HBA and multi- port driveThis configuration also provides a failover path unless the failure is with the HBA or the HBA's cable. At which point, the connection is severed and a permanent path error occurs. Whereas, if the failure occurs on the device side, an alternative path is still available for the information to go through which DPF automatically failovers to.

# Control path failover

Control path failoverThe following flowcharts outline the different types of configurations for control path failover (CPF). These configurations are presented in order of best practices as recommended by IBM.

# Dual Host Bus Adapters (HBAs) to multi-port drives

Consider a multipath connection that consists of two Host Bus Adapters (HBAs) connected through a fabric to the library by at least two drives.

![](images/5abae7041032b735665af516d2d57686e2c39758e1571f1c8c2e16e5b6ca47aa.jpg)  
Figure 5. Dual HBA and multi-port drives

As seen in Figure 5 on page 7, four available paths are available between the drive and the host system. These paths are

HBA A to drive 1 [A, d1]  HBA A to drive 2 [A, d2]  HBA B to drive 1 [B, d1]  HBA B to drive 2 [B, d2]

As with DPF, one path is the primary path and the other three are alternate paths. If [A, d1] is the primary path and if HBA A fails, two remaining valid paths ([B, d1] and [B, d2]) are still available. The CPF attempts to switch to one of the available configured paths. Conversely, if the cable to drive 1 or drive 1 fails with [A, d1] as the primary path, two valid paths to the drive ([A, d2] and [B, d2]) are available. Without CPF support, if a permanent path error occurs (because of HBA or cable failover, for example), the application fails. With CPF, if a permanent failure with this configuration occurs, two valid physical paths are available for the data to be transmitted. Also, the running application is not affected.

If the failed path is restored, the device driver claims the path as available and uses it as a valid alternate path in most conditions. This action is dependent on Operating System and HBA behavior, not the IBM tape device driver behavior.

Note: In the operating systems logs, reservation conflict information might appear, which is because of scsi2 reservations that are not cleared. However, the device driver continues to try any paths that are available to make the reservation conflict transparent to the operating system.

# Single Host Bus Adapter (HBA) to multi-port drives

Consider a single path from the HBA through the fabric to two drives in a library.

![](images/cb4f2a64621ae56c84bb06678434efcdfb2a5f71363db75aef067c388a082c68.jpg)  
Figure 6. Single HBA and multi-port drives

This configuration also provides a failover path unless the failure is with the HBA or the HBA's cable. At which point, the connection is severed and a permanent path error occurs. Whereas, if the failure occurs

with a drive or a drive's cable, an alternative path is still available for the information to go through which CPF automatically failovers to.

# Dynamic load balancing

The dynamic load balancing support optimizes resources for tape devices that have physical connections to multiple host bus adapters (HBA) in the same machine. When an application opens a device that has multiple configured HBA paths, the device driver determines which path has the HBA with the lowest usage. Then, it assigns that path to the application. When another application opens a different device with multiple HBA paths, the device driver again determines the path with the lowest HBA usage. Then, that path is assigned to the second application. The device driver updates the usage on the HBA assigned to the application when the device is closed. Dynamic load balancing uses all host bus adapters whenever possible and balance the load between them to optimize the resources in the machine.

For example, consider a machine with two host bus adapters, HBA1 and HBA2, with multiple tape drives attached. Each tape drive is connected to both HBA1 and HBA2. Initially, there are no tape drives currently in use. When the first application opens a tape drive for use, the device driver assigns the application to use HBA1. When a second application opens a tape drive for use, the device driver assigns the second application to use HBA2. A third application is assigned to HBA1 and a fourth application is assigned to HBA2. Two applications are assigned to HBA1 and two applications are assigned to HBA2.

If the first application finishes and closes the device, there is now one application with HBA1 and two applications with HBA2. When the next application opens a tape drive, it is assigned to HBA1, so again there are two applications with HBA1 and two applications with HBA2. Likewise, if the second application finishes and closes the device, HBA2 has one application that is assigned to it. The next application that opens a tape drive is assigned to HBA2.

The dynamic load balancing support is independent from the automatic failover support. Regardless of the path that is assigned initially for load balancing, if that path fails, the automatic failover support attempts recovery on the next available path.

# Dynamic Runtime Attributes

There are frequently field issues where customers must know which Initiator is holding a reservation in a drive or preventing the media from being unloaded. Also, they must correlate which drive special file name is used for the drive (such as zmt2). Sometimes this issue occurs over transport bridges and translators, losing any transport identifiers to help in this effort. LTO5, 3592 E07 (Jag 4) and later physical tape drives support attributes that are set to the drive dynamically by a host. This function is called Dynamic Runtime Attributes (DRA).

This feature is enabled by default. The attributes are set in the drive by the host during open, close, device reset, and data path change only. If there is a problem with sending the attributes to the drive, the error is ignored and not returned to the application.

There is no ioctl in the IBM tape drivers to retrieve the dynamic runtime attributes but there is a command on ITDT (See "runtimeinfo | qryruntimeinfo" on page 135). The attributes can also be retrieved through a pass through ioctl to issue Read Dynamic Runtime Attributes SCSI command (see applicable IBM Tape Drive SCSI Reference). See the host platform section for any special information that pertains to the driver that concerns DRA. If there is a question whether your driver level supports DRA, see the fixlist that comes with your driver to see whether it was added. Updates are also required with the drive firmware.

# Data encryption

# Tape and library requirements

For specific encryption support, refer to SSIC at IBM System Storage Interoperation Center (SSIC) The following three major elements comprise the tape drive encryption solution.

# The encryption-enabled tape drive

The 3592 Model E07 and newer model tape drives, and the LTO Ultrium 4 and newer Ultrium drives are encryption capable. To run hardware encryption, the tape drives must be encryption- enabled. Encryption can be enabled on the encryption- capable tape drives through the Tape Library Specialist Web interface. Refer to the appropriate section in the documentation for your library for information about how to enable encryption.

Note: FC 1604, Transparent LTO Encryption, is required to use library- managed encryption on LTO Ultrium 4 and newer tape drives. It is not required for application- managed encryption. Refer to the sections on each method of encryption for information.

# Encryption key management

Encryption key managementEncryption involves the use of several kinds of keys, in successive layers. How these keys are generated, maintained, controlled, and transmitted depends upon the operating environment where the encrypting tape drive is installed. Some data management applications, such as Tivoli® Storage Protect, can run key management. For environments without such applications or where application- agnostic encryption is wanted, IBM provides a key manager (such as the IBM Guardium Key Lifecycle Manager) to complete all necessary key management tasks.

# Encryption policy

The method that is used to implement encryption. It includes the rules that govern which volumes are encrypted and the mechanism for key selection. How and where these rules are set up depends on the operating environment.

The LTO Ultrium 6 and later encryption environment is complex and requires knowledge beyond that of product trained Service Support Representatives (SSRs). The Encryption function on tape drives (desktop, stand- alone, and within libraries) is configured and managed by the customer. In some instances, SSRs are required to enable encryption at a hardware level when service access or service password controlled access is required. Customer setup support is by Field Technical Sales Support (FTSS), customer documentation, and software support for encryption software problems. Customer 'how to' support is also provided with a support line contract.

In the open system environment, there are two methods of encryption management to choose from. These methods differ in where you choose to locate your encryption key manager application. Your operating environment determines which is the best for you, with the result that key management and the encryption policy engine might be in any one of the three environmental layers: application layer, system layer, and library layer.

# Application-managed tape encryption

This method is best where operating environments run an application already capable of generating and managing encryption policies and keys, such as IBM Storage Protect. Policies specifying when encryption is to be used are defined through the application interface. The policies and keys pass through the data path between the application layer and the Encryption is the result of interaction between the application and the encryption- enabled tape drive, and is transparent to the system and library layers.

Refer to "Planning for application- managed tape encryption" on page 10 for details on the hardware and software requirements for application- managed encryption. For details on setting up applicationmanaged tape encryption refer to the IBM Storage Protect documentation or for information, visit IBM Storage Protect documentation.

It is required to use the latest device drivers available. Refer to "Accessing documentation and software online" on page 75 for downloading drivers. In different operating system environments, refer to the applicable chapter for each operating system.

# Library-managed tape encryption

Library- managed tape encryptionThis method is best for encryption- capable tape drives in open attached IBM tape libraries. Scratch encryption policies that specify when to use encryption are set up through the IBM Tape Library Specialist Web interface. Policies are based on cartridge volume serial numbers. Key generation and management

are run by an encryption key manager. Policy control and keys pass through the library- to- drive interface, therefore encryption is transparent to the applications.

Refer to "Planning for library- managed tape encryption" on page 10 for details on the hardware and software requirements for library- managed encryption. For details on setting up library- managed encryption on encryption- capable tape drives, refer to the IBM Tape Library Operator's Guide for your library.

# Planning for application-managed tape encryption

Note: Contact your IBM representative for information about encryption on the IBM encryption- capable tape drive.

To run encryption on the encryption- capable tape drive, the following is required.

- Encryption-capable tape drive- Encryption configuration features:  
- Library code updates and Transparent LTO Encryption feature code for encryption-capable libraries  
- Tape drive code updates

# Application-managed tape encryption setup tasks

Any task that is not identified as an IBM service task is the responsibility of the customer.

1. Install, cable, and configure the encryption-capable tape drive (refer your IBM Tape Drive or Library Operator's Guide)2. Install appropriate IBM tape device driver level (Atape, for example).3. Set up encryption policies. Refer to the appropriate IBM Storage Protect documentation.4. Perform write/read operation to test encryption.5. Verify encryption of the test volume by Autonomic Management Engine (AME): issue QUERY VOLUME FORMAT=DETAILEDVerify that Drive Encryption Key Manager is set to IBM Storage Protect.

Verify that Drive Encryption Key Manager is set to IBM Storage Protect.

# Planning for library-managed tape encryption

Note: Contact your IBM representative for information about encryption on the IBM encryption- capable tape drive.

To complete encryption on the encryption- capable tape drive, the following items are required.

- Encryption-capable tape drive- Keystore (Refer to documentation on Guardium Key Lifecycle Manager (GKLM))- Encryption configuration features  
- Guardium Key Lifecycle Manager (GKLM)  
- Tape system library code updates and Transparent LTO Encryption feature code for encryption-capable libraries  
- Tape drive code updates

# Library-managed tape encryption tasks

Any task that is not identified as an IBM service task is the responsibility of the customer.

1. Install, verify, and configurea. Keystoreb. EKM (Refer to documentation on Security Key Lifecycle Manager (SKLM)) for information on both.

10 IBM Tape Device Drivers Installation and User's Guide

2. Install and cable the encryption-capable tape drive (IBM service task for TS1120 Tape Drive). 
3. Use IBM tape library specialist to enable the tape drive for library-managed tape encryption (refer to your IBM Tape Drive or Library Operator's Guide). 
4. Use library diagnostic functions to verify.

# Bulk rekey

For customers with Library- Managed Encryption with 3592 Enterprise tape drives and IBM tape and changer drivers that are running on open systems operating system (AIX, Linux, Windows), sample code for completing bulk rekey operations is available. The sample code packages are provided "as- is" with limited testing, and are provided to give customers guidance on bulk rekey operations.

For UNIX operating systems, a sample script (rekey_unix.sh) is provided and must be used with the tapeutil version that is bundled in the same package. For Windows operating systems, a sample c program (rekey_win.c) is provided. Both of these sample programs must be used with both the IBM tape and changer drivers. In addition, data cartridges must be in storage cells, not in I/O station cells or tape drives.

For information and to download the sample code packages, see http://www.ibm.com/support/fixcentral/.

# Encryption feature codes

To use system- managed and library- managed encryption, the Transparent LTO Encryption feature codes that are listed in Table 2 on page 11 are required for the associated IBM tape libraries with encryption- capable tape drives. If the drives in use are TS1120 tape drives, this feature code is not required for system- managed or library- managed encryption. If you are using application- managed encryption, no feature code is required on any encryption- capable tape drives.

<table><tr><td colspan="2">Table 2. Feature codes (encryption)</td></tr><tr><td>Tape library</td><td>Feature code</td></tr><tr><td>Diamondback</td><td>Standard, no FC required</td></tr><tr><td>TS4500</td><td>FC 1604</td></tr><tr><td>TS4300</td><td>FC 5900</td></tr></table>

# Recommended access order (RAO) open function

RAO was first introduced on IBM enterprise tape drives in the 3592- E07 (TS1140). RAO enables tape control applications to accelerate the retrieval of a certain number of files from a single tape thereby reducing the seek time between those files.

A feature of the LTO- 9 and later full- height drives is the ability to accept a list of User Data Segments and reorder those User Data Segments into a recommended access order that minimizes the locate portion of the time to read those User Data Segments. This sorted list is called a Recommended Access Order (RAO) list. A User Data Segment (UDS) is defined as a grouping of contiguous logical objects (i.e., logical blocks and filemarks) and is described by partition number, beginning logical object identifier, and ending logical object identifier.

The RAO implementation in LTO produces the best results for performance enhancement when there is little variability in block size or data compression ratio. When the variability in compression ratio or block sizes increase, the accuracy of the locate estimates may be reduced and any potential performance enhancements may be diminished.

# AIX Tape and Medium Changer device driver

This chapter describes the IBM AIX Enhanced Tape and Medium Changer Device Driver (Atape) for IBM tape devices.

# Purpose

PurposeThe IBM AIX Enhanced Tape and Medium Changer device driver is designed to take advantage of the features that are provided by the IBM tape drives and medium changer devices. The goal is to give applications access to the functions required for basic tape operations (such as backup and restore) and medium changer operations (such as mount and unmount the cartridges), and to the advanced functions needed by full tape management systems. Whenever possible, the driver is designed to take advantage of the device features transparent to the application.

# Data flow

The software that is described in this chapter covers the AIX Enhanced Device Driver (Atape device driver) and the interface between the application and the tape device. For data flow, refer to Figure 7 on page 12.

![](images/cb22d9e172305738979bf73c9d3cc40582f63eb2a70cbd346be7e32d50b55bd4.jpg)  
Figure 7. Data flow for AIX Device Driver (Atape)

# Product requirements

# Hardware requirements

Refer to "Hardware requirements" on page 1 the latest hardware that is supported by the Atape device driver.

# Software requirements

The AIX Enhanced device driver (Atape device driver) supports AIX 5L Version 5.3 and later releases on IBM POWER- based AIX servers.

For current software requirements, refer to the "Software requirements" on page 2.

# Installation and configuration instructions

The recommended procedure for installing a new version of the device driver is to uninstall the previous version.

Instructions for uninstalling the device driver are outlined in "Uninstalling" on page 15.

1. At the end of the installation procedure, the instalp facility automatically runs the AIX bosboot command to update the boot record with the newly installed Atape files. When the bosboot command completes, the following messages are displayed:

0503- 292 this update does not fully take effect until after a system reboot. installp: bosboot process completed.

This message refers to the updates to the boot record only. If the installation summary shows that the Atape driver was installed successfully, it is not necessary to reboot the machine currently.

If the installation summary shows that the installation failed, you must reboot the machine and attempt to install the Atape driver a second time.

2. During the Atape install, the following entries are entered into the two system files.

- /usr/lpp/bosinst/cdfsoptional.list to help the system image backup to DVD/CD media The entry list in /usr/lpp/bosinst/cdfsoptional.list:

/usr/lib/drivers/Atape /usr/lib/drivers/Atape Atape.driver /usr/lib/methods/cfgAtape /usr/lib/methods/cfgAtape Atape.driver /usr/lib/methods/ucfgAtape /usr/lib/methods/ucfgAtape Atape.driver /usr/lib/methods/defAtape /usr/lib/methods/defAtape Atape.driver /usr/lib/methods/ucdefAtape /usr/lib/methods/ucdefAtape Atape.driver /usr/lib/methods/chgAtape /usr/lib/methods/chgAtape Atape.driver

- /usr/lpp/bosinst/tape/tapefiles1 to create a bootable tape

The entry list in /usr/lpp/bosinst/tape/tapefiles1:

/usr/lib/drivers/Atape /usr/lib/methods/ucfgAtape /usr/lib/methods/cfgAtape /usr/lib/methods/ucfgAtape /usr/lib/methods/ucfgAtape /usr/lib/methods/ucfgAtape /usr/lib/methods/ucfgAtape /usr/lib/methods/ucfgAtape /usr/lib/methods/ucfgAtape /usr/lib/methods/chgAtape

The entries are removed from the files when Atape is uninstalled.

Attention: The entries might be lost when a user upgrades the AIX file set of bos.sysmgt.sysbr for System Backup and BOS Install Utilities after Atape installation. It is recommended that the user check whether the entries still exist and add the entries into the files if needed.

# Installation procedure

For information on obtaining the latest version of device drivers and the latest documentation, refer to "Accessing documentation and software online" on page 75.

# Preinstallation considerations

Before the installation starts, verify the following items:

1. The tape device is properly functioning, properly attached to the server, and is powered up. 
2. You logged on to the server on an account that has root authority. 
3. You have a command shell window open on the server to run the installation procedure. 
4. Make sure that the current path is defined in the command shell PATH variable. This definition can be accomplished in the Korn shell by using the following command:

EXPORT PATH=.\$PATH

5. If the tape device was configured previously by another device driver (not Atape), remove any existing device definitions for it. The following command is an example: rmdev -1 ost1 -d

# Installation procedure

Enter the following command to list the currently installed Atape.driver version:

1slpp - l Atape.driver

Enter the following command to install the Atape driver in the current directory. For example installp - acXd Atape.x.x.x.x Atape.driver

This command installs and commits the Atape driver on the system.

# Configuring Tape and Medium Changer devices

After the driver software is installed and a tape device is connected to the adapter, the device can be configured and made available for use. Access to the device is not provided until the device is configured.

Note: If the tape device was configured previously by another SCSI device driver, such as OST (Other SCSI Tape), issue the following command to remove the device definition before the following steps are completed.

rmdev - 1 [device]

Configure a tape device by using one of the following procedures.

- Enter the following command with no parameters.

cfgmgr

The command configures all devices automatically (including any new tape or medium changer devices).

- Power Off your subsystem and reboot the system to configure it automatically and make available any new tape or medium changer devices on the system.

# Configuring limitations

The subsequent limitations are applied for the Atape driver that runs on an AIX host.

Maximum supported number of tape devices 1024 Maximum supported number of HBA ports 32 Maximum supported number of paths for a device (DPF/ 16/16 CPF) Maximum LUN size per target for FC HBA\* 4095

Note: \*On AIX systems, the maximum LUN number is 4095. Since Atape supports up to 1024 devices, Atape configures a total of 1024 devices by using the range from LUN 0 - 4095. For instance, a device with LUN 4095 at a SCSI target address can be configured by Atape if the total number of devices on the system is less than 1024.

Every opened tape device uses a certain amount of resources. The user must also consider other resources such as physical memory and virtual space on the system before you attempt to reach the limits.

# Deconfiguring tape devices

Note: In the following examples, replace the letter n with the appropriate number for the chosen device.

Deconfigure the tape device by using one of the following procedures:

1. The first method leaves the tape device that is defined in the configuration database. It is similar to bringing the device offline (not in use).

Enter the following command to bring the /dev/rmtn tape device offline, but leave it defined in the device database.

2. The second method brings the tape device offline and removes its definition from the device database. Enter the following command.

rmdev - 1 rmtn - d

The device driver is not unloaded from the kernel until the last device is deconfigured.

# Deconfiguring Medium Changer devices

Note: In the following examples, replace the letter  $n$  with the appropriate number for the chosen device. Deconfigure the medium changer device by using one of the following procedures:

1. The first method leaves the device that is defined in the configuration database. It is similar to bringing the device offline.

Enter the following command to bring the /dev/smcn medium changer device offline, but leave it defined in the device database.

rmdev - 1 smcn

2. The second method brings the medium changer device offline and removes its definition from the device database.

Enter the following command.

rmdev - 1 smcn - d

The device driver is not unloaded from the kernel until the last device is deconfigured.

# Uninstalling

Attention: All devices that use the Atape driver must be closed and cannot be in use when Atape is uninstalled or the uninstall fails.

You can uninstall the Atape device driver by using the smit command menu to uninstall software and selecting Atape.driver or by using the following installp command

installp - u Atape.driver

# Tape drive, media, and device driver parameters

This chapter describes the parameters that control the operating modes of the AIX Enhanced Tape and Medium Changer Device Driver.

# Configuration parameters

The operating parameters for the tape drive and device driver are set and changed by configuration parameters. The installation defaults are provided for all parameters initially. The AIX smit command is used to set these parameters when a device is configured or to change these parameters. The AIX chdev command is used to change the configuration parameters.

The configuration parameters are used to set the operating mode of the tape drive and device driver when a device is opened. These parameters can be queried by an application. Some parameters can be temporarily changed during the open subroutine by an application. But, they are always restored to the configuration values when a device is closed. The configuration parameters are

- Alternate Pathing- Autoloading- Emulate autoloader (359x devices only)

- Block size- Buffered mode (359x devices only)- Compression- Fail degraded media (359x devices only)- Logical write protect (359x devices only)- Logging- Maximum size of the log file- New logical name- Read error recovery time (359x devices only)- Record space mode- Reservation key- Reservation support- Reservation type- Retain reservation- Rewind immediate- System encryption- System encryption for Write Commands- Trailer labels- SCSI status busy retry- iostat support for tape

# Alternate pathing

This parameter enables or disables the path failover support when a device is configured. See "Data Path failover and load balancing support for tape drives" on page 27 for a description of the path failover and failover support.

The installation default is no (path failover is not enabled).

# Autoloading

This parameter enables the autoloading feature of the device driver. It is used with the autoloading capability of the autoloader, ACF, ACL, or CSL installed on the tape device.

Note: The autoloading feature is not supported on the IBM 3584 UltraScalable tape library and the IBM 3583 Ultrium Scalable tape library with more than one IBM 3580 Ultrium tape drive installed.

Note: The autoloading feature is supported only on the following device types and configurations:

- IBM 3490E Models C11, C22, E01, E11, F01, and F11- IBM Enterprise Tape System 3590, Models B11, E11, and H11- IBM Magstar MP 3570 Models B01, C01, B11, and C11- IBM Magstar MP 3570 Models B02, B12, C02, and C12 (configured in split mode only)- IBM 7332 (all models)

Do not enable autoloading if one of the following conditions is true.

- The device is used by an application that provides library medium changer support for the IBM 3581 or IBM 3583.- The device is installed in a 3494 Enterprise Tape Library.- The device is used by an application with stack loader support.- The application is MKSYSB.

16 IBM Tape Device Drivers Installation and User's Guide

- The tapes that are read were not written with the autoloading feature.

Tapes that are created with AUTOLOAD=YES are not readable in configurations without Atape autoloade enabled, or on other UNIX operating systems, or on device types/models that are different from the backup device type/model.

If the parameter is set to On, then the tape stacker acts as one large virtual tape. During a read, write, or forward space file operation, no end of tape is detected by the application. When the end of tape is reached, the device driver automatically rewinds and unloads the tape, then loads the next tape. Then, it continues reading or writing the next tape. The following conditions are required to use this feature:

- The autoloading parameter must be set to On.- The cartridge stacker must be loaded with one or more tapes.- The ACF, ACL, or CSL must be set to Automatic, System, or Random mode.

This feature allows multivolume backups (with commands such as tar) without prompting for a volume change.

The installation default is Off (no autoloading).

# Emulate autoloader

This parameter controls how the device driver operates when the ACF on the IBM Enterprise Tape System 3590, the IBM Magstar MP tape device, or the IBM 3490E Model Fxx is set to Random mode. If this parameter is set to On and the ACF is in Random mode, the device driver emulates an autoloading tape drive. When an unload command is sent to the device driver to unload a tape, the tape is unloaded, returned to the magazine, and the next tape in the magazine is loaded automatically into the tape drive. If this parameter is set to Off, the normal unload operation occurs, and the tape remains in the drive.

The emulate autoloader parameter can be used for legacy applications that are written for the IBM 3490E Automated Cartridge Loader (ACL) when the IBM Enterprise Tape System 3590, the IBM Magstar MP 3570, or the IBM 3490 Model F autoloader is set to Random mode. This parameter eliminates the need to reconfigure the autoloader of the device Random or Automatic operation.

The installation default is Off (do not emulate autoloader).

Note: On IBM Magstar MP 3570 Models B02, C02, and C12, this feature is supported only when the two drives are configured in Split mode, or in Base mode with one drive that is configured and available to AIX. This feature does not work in Base mode if both drives are in the available state to AIX.

# Block size

This parameter specifies the block size that is used for read and write operations. A value of zero is the variable block size. Any other value is a fixed block size.

The installation default is zero (use variable length) except for the IBM 7332 4- mm Tape Cartridge Autoloader, for which the default is a fixed block size of 1024 bytes.

# Buffered mode

When a write command is processed, the data is either stored directly on the physical tape or buffered in the tape device. Buffering can increase the device performance.

The installation default is On (use Buffered mode).

# Compression

Hardware compression is implemented in the device hardware. This parameter turns the compression feature On and Off. If compression is enabled, then the effective performance can increase based on the compressibility of the data.

The installation default is On (use compression).

# Fail degraded media

This parameter controls whether the device driver fails a tape operation when degraded media is detected by the IBM Enterprise Tape System 3590. If a tape is loaded and the IBM 3590 cannot read the positioning information from the tape, the device driver is notified when the first command is sent to the tape drive. If this parameter is set to On, the device fails the command and returns a media error to the application. If this parameter is set to Off, the device driver does not fail the command.

Degraded media is a correctable condition that prevents the IBM Enterprise Tape System 3590 from running high speed Locate operations. A Locate command can take over 20 minutes, depending on the wanted position and the amount of data on the tape. This parameter is intended for use by real- time applications that cannot tolerate long Locate commands.

The installation default is Off (do not fail the tape operation if degraded media is detected).

# Logging

This parameter turns the volume information logging on and off. If logging is set to On, the statistical information about the device and media is saved in a log file when a tape is unloaded. If logging is set to Off, the information is not saved. This parameter has no effect on error logging because error logging is always enabled. For information, refer to "Device and volume information logging" on page 30.

The installation default is Off (no logging).

# Maximum size of the log file

This parameter specifies the number of entries that are made before the log file starts to wrap. Each entry is approximately 2 KB (2048 bytes). After the log file starts to wrap, the number of entries stays constant. Each time a new entry is made, the oldest entry is overlaid. For information, refer to "Device and volume information logging" on page 30.

The installation default is 500.

# New logical name

Setting this parameter changes the logical name of the device to a new name as specified. After the logical name is changed, the new logical name parameter is cleared. For information, refer to "Persistent Naming Support" on page 23.

There is no installation default value for this parameter.

# Read error recovery time

This parameter controls the read error recovery time for the IBM Enterprise Tape System 3590. If this parameter is set to On, the recovery time for read errors is limited to a maximum of 5 seconds. If this parameter is set to Off, full recovery is used by the device and can take up to 10 minutes. This parameter is intended for use by real- time applications that cannot tolerate long delays when data is read from the tape.

The installation default is Off (do not limit the read error recovery time).

# Record space mode

This parameter specifies how the device driver operates when a forward or backward space record operation encounters a filemark. The two modes of operation are SCSI and AIX.

The SCSI mode is the default mode of operation. When a forward or backward space record operation is issued to the driver and a filemark is encountered, the device driver returns - 1 and the errno variable is set to input/output error (EIO). The tape is left positioned after the filemark (the end- of- tape side of the filemark on the forward space and the beginning- of- tape side of the filemark on the backward space).

The AIX mode returns the same EIO errno value as the SCSI mode when a filemark is encountered except that the tape is left positioned before the filemark (the beginning- of- tape side of the filemark on the forward space and the end- of- tape side of the filemark on the backward space).

The installation default is SCSI mode.

# Reservation key

This parameter specifies the SCSI Persistent Reservation key that is used by the device driver when either the Reservation Type parameter is SCSI Persistent Reserve and the Alternate Pathing parameter is set to no or when the Alternate Pathing parameter is set to Yes.

The default for this attribute is blank (NULL).

If the Reservation Key parameter is specified as blank (NULL), then the device driver uses an internal unique key for all devices on the host they are configured on. Another AIX host that shares devices also have an internal unique key for all devices if the Reservation Key parameter was blank (NULL).

If the default is not used, then the Reservation Key value can be specified as either a 1- 8 character ASCII alphanumeric key or a 1- 16 hexadecimal key that has the format 0xkey. If fewer than 8 characters are used for an ASCII key (such as host1), the remaining characters are set to 0x00 (NULL). If less than a 16 hexadecimal key is used, the remaining bytes are set to 0x00.

Note: When a Reservation Key is specified on each host that shares a device, the key must be unique to each host.

# Reservation support

The parameter of reserve_support indicates that the Atape driver manages the reservation for the tape device when it is enabled. Atape reserves the tape device in open and releases it in close, and maintains the reservation in error recovery procedure (ERP).

Note: For the medium changer, this parameter is not applied when the Alternate Pathing (path failover) parameter is set to Yes. The device driver forces the setup to be disabled and the medium changer is not reserved in open when the Alternate Pathing parameter is set to Yes.

The installation default is Yes.

# Reservation type

This parameter specifies the SCSI Reservation type that is used by the device driver, either a SCSI Reserve 6 command or a SCSI Persistent Reserve command.

Note: This parameter is not used if the Alternate Pathing (path failover) parameter is set to Yes. The device driver uses SCSI Persistent Reserve when the Alternate Pathing parameter is set to Yes.

The installation default is SCSI Reserve 6.

# Retain reservation

When this parameter is set to 1, the device driver does not release the device reservation when the device is closed for the current open. Any subsequent opens and closes until the STIOCSETP IOCTL is issued with retain_reservation parameter set to 0. The device driver still reserves the device on open to make sure that the previous reservation is still valid.

The installation default is Off (the reservation is released in close).

# Rewind immediate

This parameter turns the immediate bit On and Off in rewind commands. If it is set to On, the rewind tape operation runs faster. However, the next command takes a long time to finish unless the rewind operation is physically complete. Setting this parameter reduces the amount of time that it takes to close a device for a Rewind on Close special file.

The installation default is Off (no rewind immediate) except for the IBM 7332 4- mm Tape Cartridge Autoloader, for which the default is On (rewind immediate).

# System encryption

This parameter specifies whether System- Managed Encryption must be used. For information, refer to "System- managed encryption" on page 29.

The installation default is No.

# System encryption for Write commands

This parameter controls if System- Managed Encryption is used for Write commands. For information, refer to "System- managed encryption" on page 29.

The installation default is Custom.

# Trailer labels

If this parameter is set to On, then writing a record past the early warning mark on the tape is allowed. The first write operation to detect EOM fails, and the errno variable is set to ENOSPC. No data is written during the operation. All subsequent write operations are allowed to continue until the physical end of the volume is reached and EIO is returned.

This parameter can also be selected by using one of three device special files that allow trailer- label processing. The special files are imtx.40, imtx.41, and imtx.60, where  $x$  is the name of the device (for example, imt0.40).

The installation default is Off (no trailer labels).

# SCSI status busy retry

Atape retries the SCSI command fail due to the SCSI status Busy when the parameter of busy_retry is set to On. Otherwise, Atape fails the SCSI command if it is set to Off.

The installation default is Off.

# iostat support for tape

The iostat command is used to monitor system input/output (I/O) devices. To work this system command for tape, Atape reports input and output statistics on each tape drive.

The installation default is On.

# Media parameters

The ability to set or change media parameters is a tape diagnostic and utility function, refer to "IBM Tape Diagnostic Tool (ITDT)" on page 73.

The media parameters can be queried and set by ITDT or the tape diagnostic and utility function by using the Query/Set Parameters option in the window.

These parameters cannot be set or changed by the configuration procedures. The media parameters are

- Capacity scaling- Logical write protect- Volume ID for logging- Archive mode unthread (AMU)

# Capacity scaling

This parameter sets the capacity or logical length of the current tape on IBM Enterprise Tape System 3590, IBM Enterprise Tape System 3592, or Magstar MP tape subsystems. By reducing the capacity of the tape, the tape drive can access data faster at the expense of data capacity.

Capacity scaling can be set at  $100\%$  for the entire tape (which is the default) or set at  $75\%$ $50\%$  ,or  $25\%$  of the tape or any device- specific hexadecimal value. For example, on IBM 3592, to change capacity scaling from a 300 GB format tape  $(100\%)$  to a 60 GB format tape, select the capacity scaling option. Then, select the option to enter a hexadecimal value and enter 35. Capacity scaling remains with the tape across mounts until it is changed.

# Note:

Note:1. The tape position must be at the start of the tape to change this parameter from its current value.2. Changing this parameter deletes any existing data on the tape.3. Attempting to set capacity scaling that is not supported by a device or the current loaded media always returns  $100\%$  and cannot be changed. For example, 60 GB media for the IBM 3592 cannot be capacity scaled and is always  $100\%$ .

# Logical write protect

This parameter sets or resets the logical write protect of the current tape on IBM Enterprise Tape System 3590, IBM Enterprise Tape System 3592, or Magstar MP tape subsystems. The three types of logical write- protect are associated- protect, persistent- protect, and write- once read- many (wORM)- protect.

Associated protect remains only while the current tape is mounted or associated with the tape drive. It is reset when the tape is unloaded or the tape drive is reset.

Persistent protect remains or persists with the tape across mounts until it is reset.

WORM protect also remains with the tape across mounts, but (unlike persistent protect) it cannot be reset on the tape. After a tape is WORM protected, it can never be written on again.

# Note:

Note:1. The tape position must be at the start of the tape to change this parameter from its current value.2. Attempting to set logical write protect that is not supported by a device or the current loaded media always returns "No" and cannot be changed.

# Volume ID for logging

Volume ID for loggingThis parameter is the volume ID of the current loaded tape. It is used in the log file entry (if volume logging is active) to identify the entry with a particular volume. The device driver sets the volume ID to UNKNOWN initially and when the tape is unloaded.

# Archive mode unthread (AMU)

Archive mode unthread (AMU)This parameter turns Archive mode unthread (AMU) On and Off. When it is set to On, Atape manages and works the AMU feather that rewinds the tape cartridge to the end of tape at a low tension for long- term storage. The AMU feature on the tape drive is enabled for 3592 tape drives and disabled for LTO tape drives by default. To enable or disable this feature on the tape drive, the parameter must be turned on in the Atape driver.

# Special files

Special filesWhen the driver is installed and a tape device is configured and available for use, access is provided through the special files. These special files, which consist of the standard AIX special files for tape devices (with other files unique to the Atape driver), are in the /dev directory.

# Special files for tape devices

Each tape device has a set of special files that provides access to the same physical drive but to different types of functions. As shown in Table 3 on page 22, in addition to the tape special files, a special file is provided for tape devices that allow access to the medium changer as a separate device.

Note: The asterisk  $(^{\star})$  represents a number that is assigned to a particular device (such as rmt0).

For tape drives with attached SCSI medium changer devices, the rmt\*.smc special file provides a separate path for commands that are issued to the medium changer. When this special file is opened, the application can view the medium changer as a separate SCSI device.

Both this special file and the rmt\* special file can be opened at the same time. The file descriptor that results from opening the rmt\*.smc special file does not support the following operations.

Read Write Open in Diagnostic mode Commands that are designed for a tape drive

If a tape drive has a Scsi medium changer device that is attached, then all operations (including the medium changer operations) are supported through the interface to the rmt\* special file. For detailed information, refer to Table 3 on page 22.

<table><tr><td colspan="6">Table 3. Special files for tape devices</td></tr><tr><td>Special file name</td><td>Rewind on Close (Note 1)</td><td>Retension on Open (Note 2)</td><td>Bytes per Inch (Note 3)</td><td>Trailer Label</td><td>Unload on Close</td></tr><tr><td>/dev/rmt*</td><td>Yes</td><td>No</td><td>N/A</td><td>No</td><td>No</td></tr><tr><td>/dev/rmt*.1</td><td>No</td><td>No</td><td>N/A</td><td>No</td><td>No</td></tr><tr><td>/dev/rmt*.2</td><td>Yes</td><td>Yes</td><td>N/A</td><td>No</td><td>No</td></tr><tr><td>/dev/rmt*.3</td><td>No</td><td>Yes</td><td>N/A</td><td>No</td><td>No</td></tr><tr><td>/dev/rmt*.4</td><td>Yes</td><td>No</td><td>N/A</td><td>No</td><td>No</td></tr><tr><td>/dev/rmt*.5</td><td>No</td><td>No</td><td>N/A</td><td>No</td><td>No</td></tr><tr><td>/dev/rmt*.6</td><td>Yes</td><td>Yes</td><td>N/A</td><td>No</td><td>No</td></tr><tr><td>/dev/rmt*.7</td><td>No</td><td>Yes</td><td>N/A</td><td>No</td><td>No</td></tr><tr><td>/dev/rmt*.10 (Note 4)</td><td>No</td><td>No</td><td>N/A</td><td>No</td><td>No</td></tr><tr><td>/dev/rmt*.20</td><td>Yes</td><td>No</td><td>N/A</td><td>No</td><td>Yes</td></tr><tr><td>/dev/rmt*.40</td><td>Yes</td><td>No</td><td>N/A</td><td>Yes</td><td>No</td></tr><tr><td>/dev/rmt*.41</td><td>No</td><td>No</td><td>N/A</td><td>Yes</td><td>No</td></tr><tr><td>/dev/rmt*.60</td><td>Yes</td><td>No</td><td>N/A</td><td>Yes</td><td>Yes</td></tr><tr><td>/dev/rmt*.null (Note 5)</td><td>Yes</td><td>No</td><td>N/A</td><td>No</td><td>No</td></tr><tr><td>/dev/rmt*.smc (Note 6)</td><td>N/A</td><td>N/A</td><td>N/A</td><td>N/A</td><td>N/A</td></tr><tr><td colspan="6">Table 3. Special files for tape devices (continued)</td></tr><tr><td>Special file name</td><td>Rewind on Close (Note 1)</td><td>Retension on Open (Note 2)</td><td>Bytes per Inch (Note 3)</td><td>Trailer Label</td><td>Unload on Close</td></tr></table>

# Note:

1. The Rewind on Close special files write filemarks under certain conditions before rewinding. 
2. The Retensions on Open special files rewind the tape on open only. Retensioning is not done because these tape products complete the retension operation automatically when needed. 
3. The Bytes per Inch options are ignored for the tape devices that are supported by this driver. The density selection is automatic. 
4. The rmt\*.10 file bypasses normal close processing, and the tape is left at the current position. 
5. The rmt\*.null file is a pseudo device similar to the /dev/null AIX special file. The input/output control (iootl) calls can be issued to this file without a real device that is attached to it, and the device driver returns a successful completion. Read and write system calls return the requested number of bytes. This file can be used for application development or debugging problems. 
6. The rmt\*.smc file can be opened independently of the other tape special files.

# Special files for Medium Changer devices

After the driver is installed and a medium changer device is configured and made available for use, access to the robotic device is provided through the smc\* special file in the /dev directory.

Table 4 on page 23 shows the attributes of the special file. The asterisk  $(^{\star})$  represents a number that is assigned to a particular device (such as smc0). The term smc is used for a SCSI medium changer device. The smc\* special file provides a path for issuing commands to control the medium changer robotic device. For information, refer to Table 4 on page 23.

<table><tr><td colspan="2">Table 4. Special files for Medium Changer devices</td></tr><tr><td>Special file name</td><td>Description</td></tr><tr><td>/dev/smc*</td><td>Access to the medium changer robotic device</td></tr><tr><td>/dev/smc*.null</td><td>Pseudo medium changer device</td></tr><tr><td colspan="2">Note: The smc*.null file is a pseudo device similar to the /dev/null AIX special file. The commands can be issued to this file without a real device that is attached to it, and the device driver returns a successful completion. This file can be used for application development or debugging problems.</td></tr></table>

The file descriptor that results from opening the smc special file does not support the following operations:

Read Write Commands that are designed for a tape device

# Persistent Naming Support

Persistent naming support is used to ensure that attached devices are always configured with the same logical name based on the SCSI ID, LUN ID, and host bus adapter (HBA), even when the system is rebooted.

When the AIX operating system is booted, the HBA runs a device discovery and assigns a default logical name to each device found in a sequential order. If there are three tape drives attached to a parallel SCSI

adapter, each with a LUN ID of 0 and a target address of 0, 1, and 2, the HBA initially configures them as Available with the following logical names.

<table><tr><td>rmt0</td><td>target 0, lun 0</td><td>Available</td></tr><tr><td>rmt1</td><td>target 1, lun 0</td><td>Available</td></tr><tr><td>rmt2</td><td>target 2, lun 0</td><td>Available</td></tr></table>

Run the following commands before the machine is rebooted.

<table><tr><td>- rmdev -dl rmt2</td></tr><tr><td>-rmdev -dl rmt2</td></tr></table>

On the next reboot, if the existing rmt1 target 1 device is powered off or not connected, the HBA initially configures two devices as Available with the following logical names:

<table><tr><td>rmt0</td><td>target 0, lun 0</td><td>Available</td></tr><tr><td>rmt1</td><td>target 2, lun 0</td><td>Available</td></tr></table>

If the previous rmt1 target 1 device is powered on after reboot and the cfgmgr command is run, the HBA configures the device as rmt2 instead of rmt1.

<table><tr><td>rmt2</td><td>target 1, lun 0</td><td>Available</td></tr></table>

This is one example, but there are other cases where the logical names of devices could change when the system is rebooted. For applications that need a consistent naming convention for all attached devices, it is accomplished with persistent naming support by defining a unique logical name (other than the AIX default names) that are associated with the specific SCSI ID, LUN ID, and HBA that the device is connected to.

# Changing the logical name after initial boot

The logical name of a device can be changed after an initial boot and configured. This procedure can be done by using the SMIT menu or the chdev command from a script or command line.

For example, a default rmt0 logical name for a tape drive can be changed to rmt- 0, tape0, or any descriptive name wanted. In this example, if the three tape drives are changed to rmt- 0, rmt- 1, and rmt- 2, and the system is then rebooted with rmt- 1 powered off, the HBA detects that unique names are predefined for the attached devices, and the HBA uses those names. In this case, the devices configure as follows:

<table><tr><td>rmt-0</td><td>target 0, lun 0</td><td>Available</td></tr><tr><td>rmt-1</td><td>target 1, lun 0</td><td>Defined</td></tr><tr><td>rmt-2</td><td>target 2, lun 0</td><td>Available</td></tr></table>

Since rmt- 1 is not detected by the HBA but is predefined at the SCSI ID and LUN ID, it remains in the defined state and is not configured for use. But, the next rmt- 2 tape drive configures as the same name at the same location after reboot.

# Changing the logical name with SMIT

To change the logical name by using SMIT, complete the following steps:

1. Run SMIT from a command line and select Devices.  
2. Select Tape Drive.  
3. Select Change/Show Characteristics of a Tape Drive.  
4. Select the logical device to be changed from the list displayed.  
5. In the New Logical Name field, enter a non-AIX default logical name.  
6. Press Enter to process the change.

# Changing the logical name with the chdev command

The logical name of a device can be changed by using the chdev command. For example, to change the logical name of the device from rmt0 to rmt- 0, run

chdev - l rmt0 - a new_name  $=$  rmt- 0

The output of this command displays rmto changed

# Note:

- When path failover is enabled, if you change the logical name for either a primary or alternate device, only the individual device name changes. Follow the naming convention whenever you run mksysb, bosboot:

- The prefix name of "rmt" cannot be changed.  
- A sequence number must be a positive integer. The smallest sequence number is 0.  
- The prefix name cannot contain non-numerical characters. For example, rmt1_rescu is not an acceptable prefix name.  
- When a device instance logical name is generated, the SMIT tool automatically assigns the next available sequence number (relative to a specific prefix name). The next available sequence number is defined as the smallest sequence number for a particular prefix name that is not yet allocated.

# Control Path failover support for tape libraries

Note: The library control path failover feature code must be installed before enabling the path failover support in the Atape device driver. Refer to "Automatic failover" on page 4 for what feature codes might be required for your machine type.

The Atape device driver path failover support configures multiple physical control paths to the same logical library within the device driver. It also provides automatic failover to an alternate control path when a permanent error occurs on one path. This support is transparent to the running application.

# Configuring and unconfiguring path failover support

Path failover support is not enabled automatically when the device driver is installed. It must be configured initially on each logical device after installation. When path failover support is enabled for a logical device, it remains set until the device is deleted or the support is unconfigured. The alternate path failover setting is retained even if the system is rebooted.

To enable or disable the support on a single logical device, use the SMIT menu to Change/Show Characteristics of a Tape Drive, select the logical device to change such as smc0, smc1, then select Yes or No for Enable Path Failover Support. The support can also be enabled or disabled by using the chdev command, for example,

chdev - l smc0 - aalt pathing=yes chdev - l smc0 - aalt pathing=yes chdev - l smc0 - aalt pathing=no chdev - l smc1 - aalt pathing=no

# Primary and alternative paths

When the device driver configures a logical device with path failover support enabled, the first device that is configured always becomes the primary path. On SCSI attached devices, - P is appended to the location field. On Fibre attached devices, - PRI is appended to the location field of the device.

When a second logical device is configured with path failover support enabled for the same physical device, it configures as an alternative path. On SCSI attached devices, - A is appended to the location field. On Fibre attached devices, - ALT is appended to the location field of the device.

A third logical device is also configured as an alternative path with either - A or - ALT appended, and so on. The device driver supports up to 16 physical paths for a single device.

The labeling of a logical device as either a primary or alternative path is for information only, to

1. Be able to identify the actual number of physical devices that are configured on the system and a specific logical device that is associated with them. Only one logical device is labeled as the primary path for each physical device. However, many (multiple) logical devices can be labeled as an alternative path for the same devices. 
2. Provide information about which logical devices configured on the system have path failover support enabled.

# Querying primary and alternative path configurations

You can see the primary and alternative path configuration for all devices with the 1sdev command. Two or more logical devices can be configured for a single physical device, but the first device that is configured is labeled the primary device. All other logical devices that are configured after the first device are labeled as alternative devices. To see this information, run the 1sdev - Cc tape command and look at the location field in the data. Run the following command,

1sdev - Cc tape | grep P

For example, you can easily determine how many physical devices are configured with path failover support.

Note: Show the primary and alternative path configuration for any device by using tape diagnostic and utility functions. Refer to "IBM Tape Diagnostic Tool (ITDT)" on page 73.

# Configuring and unconfiguring primary and alternative devices

Logical devices that are configured as alternative paths can be unconfigured and reconfigured at any time after the initial configuration is run. Unconfiguring an alternative path device removes that device from the primary device path list, removes the - A or - ALT appended to the location field, and changes the device to the Defined state. The primary and any other alternative devices are still available.

Likewise, configuring a new alternative path device or reconfiguring an existing one in the Defined state adds that device to the primary device path list, appends - A or - ALT to the location field, and makes the device available.

Logical devices that are configured as primary paths can also be unconfigured and reconfigured at any time after initial configuration is run. However, the operation is different for a primary device. When a primary device is unconfigured, the following events occur.

1. All alternative devices are unconfigured as described previously. 
2. The primary device is unconfigured. 
3. The -P or -PRI appended to the location field is removed. 
4. The device is changed to the Defined state. 
5. All alternative devices that were unconfigured are reconfigured. The first device that is reconfigured becomes the new primary device. All remaining alternative devices are reconfigured as alternative paths.

These methods can unconfigure and reconfigure physical devices on the system when device connections or addressing changes are made.

# Data Path failover and load balancing support for tape drives

# Note:

1. Some devices require a path failover feature code that is installed before the path failover support is enabled in the Atape device driver. Refer to "Automatic failover" on page 4 for what feature code might be required for your machine type. 
2. DPF keys do not need to be added if you are running the latest drive code on Ultrium 3 and Ultrium 4 drives. 
3. This function is not supported for devices that are attached through an IBM San Data Gateway or on the IBM Virtualization Engine TS7510. 
4. The AIX operating system supports only a static configuration of devices, which also applies to the Path Failover and Failover Support. When devices are initially configured at a specific SCSI ID and physical connection (drive port, host bus adapter, and switch number/port, if applicable) and in the Available state, changing the physical device address/connection without either rebooting or unconfiguring and reconfiguring the devices has unpredictable results and is not supported.

# Installing Data Path failover license key

Use the following command line script to query, add, or delete license keys for this feature before the path failover feature is enabled as described below. The key is a 16- digit hexadecimal value, for example, 1234567890a/bcdef.

All key values "A- F" must be entered in lowercase letters as "a- f."

Query installed keys: dpf_keys Install a license key: dpf_keys - a key Delete a license key: dpf_keys - d key

# Configuring and unconfiguring path failover support

Path failover support is not enabled automatically when the device driver is installed. It must be configured initially on each logical device after installation. When path failover support is enabled for a logical device, it remains set until the device is deleted or the support is unconfigured. The path failover setting is retained even if the system is rebooted.

Path failover support can be enabled on all configured devices at one time, or it can be enabled or disabled selectively by logical device. It might be desirable at times to configure some, but not all, logical paths to a device with the support enabled.

To enable the support globally on all currently configured devices, run the command

/usr/lpp/Atape/InstAtape - a

This action unconfigures all devices that have path failover set to No, and reconfigures all devices, setting path failover to Yes.

To enable or disable the support on a single logical device, use the SMIT menu to Change/Show Characteristics of a Tape Drive, then select Yes or No for Enable Path Failover Support. The support can also be enabled or disabled by using the chdev command, for example:

chdev - l rmtG - aalt_pathing=yes chdev - l rmtG - aalt_pathing=no

# Primary and alternative paths

When the device driver configures a logical device with path failover support enabled, the first device that is configured always becomes the primary path and PRI is appended to the location field of the device.

When a second logical device is configured with path failover support enabled for the same physical device, it configures as an alternative path and ALT is appended to the location field. A third logical device is configured as the next alternative path with ALT appended, and so on. The device driver supports up to 16 physical paths for a single device.

For example, if rmt0 is configured first, then rmt1, the 1sdev - Cc tape command output is similar to the following command.

rmt0 Available 20- 60- 01- PRI IBM 3590 Tape Drive and Medium Changer (FCP)  rmt1 Available 30- 68- 01- ALT IBM 3590 Tape Drive and Medium Changer (FCP)

If rmt1 is configured first, then rmt0, the command output is similar to the following.

rmt0 Available 20- 60- 01- ALT IBM 3590 Tape Drive and Medium Changer (FCP)  rmt1 Available 30- 68- 01- PRI IBM 3590 Tape Drive and Medium Changer (FCP)

The labeling of a logical device as either a primary or alternative path is for information only, to

1. Identify the actual number of physical devices that are configured on the system and a specific logical device that is associated with them. Only one logical device is labeled the primary path for each physical device. However, many (multiple) logical devices can be labeled as an alternative path for the same devices.  
2. Provide information about which logical devices configured on the system have path failover support enabled.

# Querying primary and alternative path configuration

You can see the primary and alternative path configuration for all devices with the 1sdev command. Two or more logical devices might be configured for a single physical device, but the first device that is configured is labeled the primary device. All other logical devices that are configured after the first device are labeled as alternative devices.

To see this information, run the 1sdev - Cc tape command and look at the location field in the data. By running 1sdev - Cc tape | grep PRI, for example, you can easily determine how many physical devices on the  $\mathsf{RS} / 6000^{\circ}$  or System p (also known as pSeries) server are configured with path failover support.

# Configuring and unconfiguring primary and alternative devices

Logical devices that are configured as alternative paths can be unconfigured and reconfigured at any time after the initial configuration is run. Unconfiguring an alternative path device removes that device from the primary device path list, removes the ALT appended to the location field, and changes the device to the Defined state. The primary and any other alternative devices are still available. Likewise, configuring a new alternative path device or reconfiguring an existing one in the Defined state adds that device to the primary device path list, appends ALT to the location field, and makes the device available.

Logical devices that are configured as primary paths can also be unconfigured and reconfigured at any time after initial configuration is run. However, the operation is different for a primary device. When a primary device is unconfigured, the following events occur.

1. All alternative devices are unconfigured as described previously.  
2. The primary device is unconfigured.  
3. The PRI appended to the location field is removed.  
4. The device is changed to the Defined state.  
5. All alternative devices that were unconfigured are reconfigured. The first device that is reconfigured becomes the new primary device. All remaining alternative devices are reconfigured as alternative paths.

These methods unconfigure and reconfigure physical devices on the system when device connections or addressing changes are made.

# System-managed encryption

# Device driver configuration

System- managed encryption can be set on a specific tape drive by using the standard SMIT panels to Change/Show Characteristics of a tape device or the command line chdev command. There are two new attributes added for encryption.

sys_encryption "yes/no" Use System Encryption FCP Proxy Manager wrt_encryption "off/on/custom" System Encryption for Write Commands at BOP

The sys_encryption attribute enables device driver system- managed encryption for a tape drive by setting the value to yes.

The wrt_encryption attribute controls whether the device driver can set the tape drive to encryption enabled for write commands. When set to off, the tape drive uses encryption for read operations; write operations do not use encryption. When set to on, the tape drive uses encryption for both read/write operations. When set to custom, the device driver does not modify current tape drive setting. The custom setting is intended for applications that use system- managed encryption to control write encryption without device driver intervention.

Note: If wrt_encryption is set to on, an application cannot open a tape drive by using the append mode.

# Querying tape drive configuration

Querying the tape drive configuration is a tape diagnostic and utility function. Refer to "IBM Tape Diagnostic Tool (ITDT)" on page 73.

# Testing data encryption configuration and connectivity

A data encryption test is available to validate the lbmekm.conf file server entries and test tape drive to server connectivity operations.

This test is a tape diagnostic and utility function. Refer to "IBM Tape Diagnostic Tool (ITDT)" on page 73.

# Error logging

Encryption errors are logged along with other tape operation errors by using the standard TAPE_ERR1 Template "Tape Operation Error" with associated sense data in the detail data.

An encryption failure is indicated when the asc/ascq in the sense data is EFxx or EXxx. Refer to the tape drive hardware reference for information on the asc/ascq being reported. The asc/ascq can be found in the first column of the second row in detail sense data. For example,

Detail Data SENSE DATA 0A00 0000 5A08 25FF 0000 00FF FF00 0000 0000 0000 F000 0600 0000 1458 0000 0000 EF11 FF00 D105 0000 0000 0191 0002 0000 0000 0A00 0000 0000 0000 0000 0000 0000 0000 0000 0000 0000 0000 0000 0000 0000 0000 0000 0000 0000 0000 0000 0200 2020 2000 0041 4A00 0000 0000 0000 0000 0000 0000 0000 0000 0000 0000 0000 0000 0000 0000 0000 0000 0000 0000 0000 2000 0000 0000 0000 0000 0000 0000 0000 0000 0000 0000 0000 0000 0000 0000 0000 0000 0000 0000 0000 2

# Field support information

When encryption failures require field support or development analysis, the following data must be provided for a problem on a specific tape drive from the machine (rmt1 for example) for the device driver. Tape drive dumps and EKM server logs might be needed in addition to this information.

# Problem determination

Problem determinationA set of tools is provided with the device driver to determine whether the device driver and the tape device are functioning correctly. The standard AIX interface is provided for problem determination.

# Dump support

Dump supportDump support is provided through the dump entry point in the driver. Refer to appropriate AIX manuals for a description of how to use the dump devices and how to read the dump data.

# Dump device commands

To list the current dump devices, enter the following command,

sysdumpdev - 1

To establish the rmt1 tape device as a secondary dump device, enter the following command,

sysdumpdev - s /dev/rmt1

To run a dump operation, use the sysdumpstart command. To send the dump data to the secondary dump device, enter the following command:

sysdumpstart - s

Note: This command stops the system. Use the sync command to ensure that the cache is flushed before the sysdumpstart - s command is issued.

To list the last dump data, enter the following command,

sysdumpdev - z

After the dump data is placed on the tape, copy it to a file on the disk before the crash command is used to process it. For example,

dd if=/dev/rmt1 of  $=$  tapedump1ibs  $= 4096$  obs  $= 512$  crash tapedump1

Note: The ibs value is the input block size.

If the block size of the tape device is larger than the block size sent during the dump process, the dump operation fails. Set the block size to zero on the tape device and experiment with the ibs value for the dd command.

# Device and volume information logging

An optional logging utility is provided to log the information about the device and the media. The information is extensive for some devices and limited for other devices. If set to On, the logging facility gathers all available information through the scs1 Log Sense command.

This process is a separate facility from error logging. Error logging is routed to the system error log. Device information logging is sent to a separate file.

The following parameters control this utility,

Logging Maximum size of the log file

Volume ID for logging

Refer to "Tape drive, media, and device driver parameters" on page 15 for a description of these parameters.

Each time the rewind and unload sequence occurs or the STIOc_LOG_SENSE ioctl command is issued, an entry is added to the log. Each time a new cartridge is loaded, the values in the device log buffers are reset with the Log Sense command. The log data is gathered on a per- volume basis.

# Log file

The data is logged in the /usr/adm/ras directory. The file name is dependent on each device; therefore, each device has a separate log. An example of the rmt1 device file is

/usr/adm/ras/Atape.rmt1. log

The files are in binary format. Each entry has a header followed by the raw Log Sense pages as defined for a particular device.

The first log page is always page  $0\times 00$  . This page, as defined in the SCSI- 2 ANSI specification, contains all pages that the device supports. Page  $0\times 00$  is followed by all pages that are specified in page  $0\times 00$  . The format of each following page is defined in the SCSI specification and the device manual.

# Tape log utility

A tape log utility is installed with the tapelog device driver that displays the contents of the log file in ASCII text. The log pages are shown as hexadecimal values in dump format.

The syntax for the tape log utility is tapelog - 1 Name [- d] or tapelog - f File [- d]

# Note:

1. Name is the logical name of the device (such as rmt0). 
2. File is the name of a log file (such as Atape.rmt0.log). 
3. The -d parameter, if used, deletes the log file for the specified device.

The contents of the log file are displayed as standard output. To save the log in a file, use the AIX redirection function. For example,

tapelog - 1 rmt0 > rmt0. log

# Reservation conflict logging

When the device driver receives a reservation conflict during open or after the device is opened, it logs a reservation conflict in the AIX error log. Before it logs the error, the device driver issues a Persistent Reserve In command to determine whether a SCSI Persistent Reservation is active on the reserving host to get the reserving host initiator WwPN (worldwide port name) and reserve key. If successful, the device driver logs this information as follows,

Reserving host key xxxxxxxx WwPN xxxxxxxx

Where the first xxxxxxxx is the actual reserve key, and the second xxxxxxxx is the reserving host initiator WwPN.

After the reserving host WwPN is initially logged, subsequent reservation conflicts from the same reserving host WwPN are not logged. This action prevents multiple entries in the error log until either the reserving host WwPN is different from the one initially logged or the device driver reserved the device and another reservation conflict occurs.

If the Persistent Reserve In command fails, the detail data contains the following entry with the errno from the failing Persistent Reserve In command.

Unable to obtain reserving host information, errno x

The possible errno values are

- ENOMEM 
- Device driver cannot obtain memory to run the command- EINVAL 
- Device has a Persistent Reservation but does not support the Persistent Reserve In service action- EBUSY 
- Device failed the command with reservation conflict and has an SCSI-2 Reserve active- EIO 
- Unknown I/O failure occurred

# Error logging

The device driver provides logging to the AIX system error log for various errors. The error log can be viewed for specific devices by using the Error Log Analysis utility that is provided with the tape drive service aids. Refer to "Error Log Analysis" on page 37. The error log can also be viewed by using the smit or the excerpt command.

# Error log templates

The error log templates the device driver uses follow the same format as the default AIX tape error log entries. Each error log entry is identified by an error label and contains detail data that is associated with the type of error. The following items describe the error labels and detail data for the templates that are used for logging tape device, media, and SCSI adapter- related errors in the AIX system error log.

# Error labels

Errors are logged with an associated error label and error ID. The error label indicates the basic type of error.

- TAPE_ERR1  Tape media error- TAPE_ERR2  Tape hardware error- TAPE_ERR4  SCSI Adapter detected error- TAPE_ERR5  Unknown error- RECOVERED_ERROR  Temporary tape hardware or media error- SIM_MIM_RECORD_3590  3590 Service/Media Information Message (Log Page X '31')- TAPE_SIM_MIM_RECORD  Tape drive Service/Media Information Message (Log Page X '31')- DEV_DUMP RETRIEVED  Device dump-retrieved- TAPE_DRIVE_CLEANING  Tape drive needs cleaning

- RESERVE_CONFLICT Reservation conflict

# Detail data

Detail data is logged with the associated error that identifies the cause of the error. Detail data for the SIM_MIM_RECORD_3590 or TAPE_SIM_MIM_RECORD entries contain the raw data from Log Sense Page 31. Refer to the hardware reference manual for the format of this entry. All other error log entries use the following format for detail data:

Detail Data SENSE DATA aabb xxxx cccd eeee eeee eeee eeee eeee ffgg hhxx ssss ssss ssss ssss ssss ssss ssss ssss ssss ssss ssss ssss ssss ssss ssss ssss ssss ssss ssss ssss ssss ssss ssss ssss ssss ssss ssss ssss ssss ssss ssss ssss ssss ssss ssss ssss ssss ssss ssss ssss ssss ssss ssss ssss ssss ssss ssss ssss ssss ssss sssss ssss ssss ssss ssss ssss ssss ssss ssss ssss ssss ssss ssss ssss ssss ssss ssss ssss ssss ssss ssss ssss ssss ssss ssss ssss ssss ssss ssss ssss ssss ssss ssss ssss ssss ssss ssss ssss ssss ssss ssss ssss ssss ssss ssss ssss ssss ssss ssss ssss s

aa Length of the command descriptor block (CDB). bb SCSI target address. xx Unused or reserved. cc Start of CDB, cc is the operation code (byte 0). dd Logical unit (byte 1) in the CDB. ee Bytes 2 - 1:2 in the CDB. ff Status validity field. If this field is 01, then a SCSI error was reported, and byte gg indicates the type of error. If this field is 02, then an adapter error was reported, and byte hh indicates the type of error.

# gg

This byte indicates the type of Scsi error that occurred.

- 02 CHECK CONDITION 
- Device reported a check condition.- 08 BUSY STATUS 
- Target is busy.- 18 RESERVATION CONFLICT 
- Target is reserved to another initiator.- 22 COMMAND TERMINATED 
- Device terminated the command.- 28 QUEUE FULL 
- Device command queue is full.

# hh

This byte indicates the type of adapter error that occurred. For parallel SCSI adapters, this byte is the general_card status code as defined in /usr/include/sys/scsi.h

- 01 HOST IO BUS ERROR 
- Host I/O bus error during data transfer.- 02 SCSI BUS FAULT 
- SCSI bus protocol or hardware error.- 04 COMMAND TIMEOUT 
- Command timed out before completion.- 08 NO DEVICE RESPONSE 
- Target did not respond to selection phase.- 10 ADAPTER HARDWARE FAILURE 
- Adapter indicated a hardware failure.- 20 ADAPTER SOFTWARE FAILURE 
- Adapter indicated a microcode failure.- 40 FUSE OR TERMINAL PWR 
- Blown terminator fuse or bad termination.- 80 SCSI BUS RESET 
- Adapter indicated that SCSI bus was reset.

For FCP or SAS adapters, this byte is the adapter_status code as defined in /usr/include/sys/scsi_buf.h

- 01 HOST IO BUS ERROR 
- Host I/O bus error during data transfer.- 02 TRANSPORT FAULT 
- Failure in the transport layer.- 03 COMMAND TIMEOUT 
- Command timed out before completion.- 04 NO DEVICE RESPONSE 
- Target did not respond to attempts to select it.- 05 ADAPTER HARDWARE FAILURE 
- Adapter indicated a hardware failure.- 06 ADAPTER SOFTWARE FAILURE 
- Adapter indicated a microcode failure.- 07 WW NAME CHANGE 
- Adapter detected a new worldwide name for the device.- 08 FUSE OR TERMINAL PWR 
- Blown terminator fuse or bad termination.- 09 TRANSPORT RESET 
- Adapter detected an external SCSI bus reset.- 0A TRANSPORT BUSY 
- The transport layer is busy.- 0B TRANSPORT DEAD 
- The transport layer is inoperative.

# ss

If byte gg indicates a check condition, the ss byte is the sense data from the device. Refer to the appropriate device reference manual for the specific format and content of these bytes.

# Automatic dump facility

The device driver provides an automatic dump facility for devices that support reading a dump and indicating when a dump is available in device sense data. Whenever a check condition occurs and the sense data indicates that a dump is available, the device driver reads the dump from the device and stores it in the /var/adm/ras directory. A maximum of five dumps for each device are stored in this directory as

Atape.rmtx.dump1  Atape.rmtx.dump2  Atape.rmtx.dump3

Note:  $X$  is the device number, for example, rmt0.

When the device is first configured, the dump name is set to dump1. If more than three dumps occur, the driver starts over at dump1; therefore the last three dumps are always kept.

# Dynamic Runtime Attributes

Field issues can occur where customers must know which initiator on a host is holding a reservation in a drive or preventing the media from being unloaded. Also, they must correlate which special file name is used for the drive (such as rmt2). Sometimes this issue occurs over transport bridges and translators, thus losing any transport identifiers to help this effort. LTO5, 3592 E07, 3592 E08, and later physical tape drives support attributes set to the drive dynamically by a host. This function is called Dynamic Runtime Attributes (DRA). Atape supports the feature, starting from 12.5.9.0.

The feature is enabled by default. The drive attributes on the host are set during the open, close, device reset, and data path change only. So, it does not have impact on the tape and system performance. The error is ignored and is not returned to application, when the information failed to send. However, a device attribute named as "host_attributes" is available to disable the DRA feature, when Atape runs with the virtual tape library for which the feature is not supported.

Run the lsattr command to display the attribute of host_attributes setup.

On LTO tape drive:

<table><tr><td># lsattr -El rmt1
host_attributes yes</td><td>Host Dynamic Runtime Attribute (LTO-5 and later only) True</td></tr><tr><td>On 3592 tape drive:</td><td></td></tr><tr><td># lsattr -El rmt4</td><td></td></tr></table>

To modify the attribute of "host_attribute", run the chdev command.

chdev - lmt1 - a host_attributes=no rmt1 changed

# Trace facility

The AIX trace facility is supported for the device driver. The trace event is identified with a hookword. The hookword that is used by the device driver is 326. The trace can be initiated at any time before an operation on a tape device.

Enter the following AIX command to start the trace.

trace - a - j 326

This command starts the trace in the background and collects only the trace events with the 326 hookword (Atape device driver).

Enter the following AIX command to stop the trace.

trcstop

This command stops the trace after the tape operations are run.

Enter the following AIX command to view the trace.

trcrt > trace.out

This command formats the trace output into a readable form and places it into a file for viewing.

# Atape System Trace (ATRC) utility

The atrc trace utility is also installed with the device driver to start, stop, and format a device driver trace. To start the trace, enter the atrc command. To stop and format the trace, enter the atrc command again. The trace is formatted to an atrc.out AIX file in the current directory.

# Component tracing

Later releases of AIX 5.3 and above support component tracing. Unlike system tracing that must be started and stopped, component tracing by default is on all the time and runs continually.

To determine whether component tracing is available, run the command ctctrl - q to display a list of supported components with their default settings. You must have root authority to run this command. Refer to the AIX ctctrl man page for a complete description of the ctctrl command and parameters.

To dump and format the current component trace to a file (for example, actrc.out) into the current directory, run the following commands.

ctctrl - D - c Atape  trcpt - 1 Atape - o actrc.out

The Atape component trace can also be retrieved from a system dump. This action eliminates the need to start the Atape system trace before a system dump or to re- create an AIX system dump when a system trace is not running. The AIX system dump is normally stored in the /var/adm/ras directory as a vmcore.x.BZ file, where x is a dump number 1, 2, and so on.

To retrieve and format the Atape component trace from a dump file (for example, vmcore.1. BZ) to a file (for example, actrc.dump) into the current directory, run the following commands.

dmpuncompress /var/adm/ras/vmcore.1. BZ  trcdead - c /var/adm/ras/vmcore.1

# Atape Component Trace (ACTRC) utility

The actrc component trace utility is also installed with the device driver to dump and format the current Atape component trace. To dump and format the component trace, run the command actrc. The trace is formatted to an actrc.out file in the current directory.

# Tape drive service aids

The service aids described in the following sections are accessible through the AIX diagnostic subsystem by using the AIX diag command and in the Task Selection menu by selecting IBM Tape Drive Service Aids. Refer to "Tape drive service aids details" on page 36.

Tape drive service aids are also available by using the "IBM Tape Diagnostic Tool (ITDT)" on page 73.

# Tape drive service aids details

The following service aid utilities are installed with the device driver:

Force Microcode Dump- Read Dump- Microcode Load- Error Log Analysis- Reset Drive- Create an FMR Tape

# Force Microcode Dump

This utility forces a dump operation on the tape drive. After the dump operation is completed, the dump data can be transferred from the tape drive by using the Read Dump utility.

To access this utility -

1. Open the Service Aids window. 
2. Select Force Microcode Dump from the IBM Tape Drive Service Aids window, then press Enter. 
3. Select the device from the IBM Tape Drive Selection window, then press Enter.

The Force Microcode Dump operation starts, and a window opens when the operation is completed.

# Read Dump

This utility transfers the dump data from the device to a file, a diskette, or a tape cartridge.

To access this utility -

1. Open the Service Aids window.

2. Select Read Dump from the IBM Tape Drive Service Aids window, then press Enter.

3. Select the device from the IBM Tape Drive Selection window, then press Enter.

4. Enter the destination file name or device in the Prompting for Destination window. The default destination is the /dev/rfd0 diskette drive. To transfer the dump data to a tape cartridge, enter the device name of the tape drive (for example, /dev/rmt0). To transfer the dump data to a file, enter the file name. Press F7 to commit.

Note: On certain terminal types, it might be necessary to press Esc and the number 7 instead of F7.

The Read Dump operation starts, and a window opens when the operation is completed.

# Microcode Load

Microcode LoadThis utility downloads microcode to the device from a file or a diskette (AIX format only).

Note: To download the microcode from a DOS diskette, you must first use the AIX dosread command to transfer the file from the DOS diskette to the AIX file. Then, you can use the Microcode Load utility to download the AIX file to the tape drive.

To access this utility -

1. Open the Service Aids window. 
2. Select Microcode Load from the IBM Tape Drive Service Aids window, then press Enter. 
3. Select the device from the IBM IBM Tape Drive Selection window, then press Enter.

4. Enter the source file name or device on the Prompting for Source File window. The default source is the /dev/rfd0 diskette drive. To load from a file, enter the file name. Press F7 to commit.

Note: On certain terminal types, it might be necessary to press Esc and the number 7 instead of F7.

5. If the current microcode on a tape drive is Federal Information Processing Standard (FIPS) code, then a window opens and displays the following message.

Warning: The drive is currently using FIPS code. Press Enter to continue with downloading new drive code.

If you do not want to download the new code, press either F3 to cancel or F10 to exit without downloading new code. Otherwise, press Enter to continue with the download code procedure.

The Microcode Load operation starts, and a window opens when the operation is completed.

# Error Log Analysis

Error Log AnalysisThis utility displays and analyzes the system error log entries for a specific tape drive and can be used for problem determination. The type of error, the SCSI command, and the sense data (if applicable) are displayed for each entry in the error log (one screen at a time).

To access this utility -

To access this utility - 1. Open the Service Aids window. 2. Select Error Log Analysis from the IBM Tape Drive Service Aids window, then press Enter. 3. Select the device from the IBM Tape Drive Selection window, then press Enter. 4. If entries are listed in the error log for the selected device, then the first entry is displayed. Press Enter to display the next entry. 5. After all entries are displayed, a window opens, and the operation is completed.

# Reset Drive

This utility resets the tape drive.

To access this utility -

Reset DriveThis utility resets the tape drive. To access this utility - 1. Open the Service Aids window. 2. Select Reset Drive from the IBM Tape Drive Service Aids window, then press Enter. 3. Select the device from the IBM IBM Tape Drive Selection window, then press Enter. The Reset Drive operation starts, and a window opens when the operation is completed.

# Create an FMR Tape

Create an FMR TapeThis utility creates a field microcode replacement (FMR) cartridge tape by using the loaded functional microcode in the tape drive.

To access this utility -

1. Open the Service Aids window.  
2. Select Create an FMR Tape from the IBM Tape Drive Service Aids window, then press Enter.  
3. Select the device from the IBM Tape Drive Selection window, then press Enter.  The Create an FMR Tape operation starts, and a window opens when the operation is completed.

# Performance considerations

This chapter describes the parameters and issues that can affect the perceived performance of the tape drive. In general, AIX applications that operate at a file level to move data between disk storage devices and tape do not use the full capabilities of a high end tape device. The goal of this discussion is to give an overview of the data path components that are involved in moving data between disk storage devices and tape. The following chapter describes basic techniques and common utilities in a specific environment that can be used to understand how a device is performing. Performance issues that are encountered by advanced application developers are beyond the scope of this document.

Refer to the hardware reference for the specific device for performance specifications. Refer to the application documentation for information on device- specific application configuration. Refer to the operating system documentation for information on disk storage device striping and other techniques for improving file system performance.

# Data path

The simplified model in Figure 8 on page 38 shows the components that are involved in the data path for moving data at a file level between disk storage devices and tape.

Performance analysis must be approached by determining which component of the data path impacts performance. Typically, a performance problem can be isolated by looking at one leg of the data path at a time. The goal of this analysis is to confirm that the tape data path is not impacting the performance adversely.

![](images/480058643b7c75fe46b17768c3d3115a743f6ff719852ff6d1f0196092b70517.jpg)  
Figure 8. Data path for AIX device driver (Atape)

# Common AIX utilities

The most commonly reported cause for poor tape performance is the use of small block sizes or the modification of the installation defaults for the tape device.

Note: The device parameters must not be changed from the defaults for most applications.

The following guidelines typically result in good tape path performance for use with AIX utilities:

1. Hardware compression must be enabled for maximum performance if the data sent to the device is uncompressed.  
2. The block_size parameter must be set to variable (block_size=0) and command or application parameters that are specified to a block size appropriate for the device.  
3. Block sizes of 128 KB or greater must be used to improve performance.

# AIX iostat utility for tape performance

AIX iostat utility for tape performanceIn releases of AIX 5.3 and earlier, the AIX iostat utility supports tape performance statistics in addition to other supported devices (such as disk). To determine whether the iostat utility supports the configured tape drives, run the command iostat - p. If the configured tape drives are supported, a list of configured tape drives are displayed with the statistics listed for each drive. Refer to the AIX iostat man page for a complete description of the iostat command and parameters. When the Data Path Failover feature is used, only the primary path for the tape drive is listed. The statistics apply to both the primary and alternative paths that are used.

# Before Support is called

Before Support is calledSystem performance tuning is not a support responsibility. If tests indicate that raw tape performance is below specifications, record the exact failing command. Then, collect the output from the commands in Table 5 on page 39 before support is contacted.

<table><tr><td colspan="2">Table 5. Error description</td></tr><tr><td>Information</td><td>Command</td></tr><tr><td>Configuration</td><td>lscfg -v</td></tr><tr><td>Device parameters</td><td>lsattr -E -l rmtN</td></tr><tr><td>Error log. Call hardware support if errors are found for TAPE_ERR* or SCSI* error labels.</td><td>errpt -a</td></tr><tr><td>Driver version</td><td>lspp -l Atape.driver</td></tr><tr><td>Trace of failing command</td><td>Refer to “Trace facility” on page 35</td></tr></table>

# Linux Tape and Medium Changer device driver

This chapter describes the IBM Linux Tape and Medium Changer device driver (lin_tape). For tape diagnostic and utility functions, refer to "IBM Tape Diagnostic Tool (ITDT)" on page 73.

# Purpose

PurposeThe lin_tape and medium changer device driver is designed to take advantage of the features that are provided by the IBM tape drives and medium changer devices. The goal is to give applications access to the functions required for basic tape operations (such as backup and restore) and medium changer operations (such as mount and unmount the cartridges), and also the advanced functions that are needed by full tape management systems. Whenever possible, the driver is designed to take advantage of the device features transparent to the application.

# Data flow

The software that is described in this chapter covers the Linux device driver (lin_tape device driver) and the interface between the application and the tape device.

![](images/16a99067675c78ff29c0288592b4e372dc0f1d79aa05c6cfac3d02f171e16100.jpg)  
Figure 9. Data flow for Linux device driver (lin_tape)

# Product requirements

# Hardware requirements

For current hardware requirements, refer to the "Hardware requirements" on page 1.

# Software requirements

Software requirementsFor current software requirements, refer to the "Software requirements" on page 2.

# Installation and Configuration instructions

The lin_tape device driver for Linux is provided in a source rpm package. The utility tools for lin_tape are supplied in binary rpm packages. Refer to "Accessing documentation and software online" on page 75. They are downloaded with the driver.

The following sections describe installation, configuration, uninstallation, and verification procedures for lin_tape and its utility tools. Refer to Linux documentation for tar command information and any Linux distribution that support rpm for rpm command information. You must have root authority to proceed with the installation of the driver. See the README file that can be downloaded with the driver at Fix Central. For information about downloading drivers, see "Accessing documentation and software online" on page 75.

This file contains the latest driver information and supersedes the information in this publication.

# Conventions used

In subsequent pages, you see file names with x.x.x in them. The x.x.x refers to the version of the driver, which changes as IBM releases new driver levels. Use the actual driver version numbers as you complete the instructions.

Commands that you are to type are indicated with a leading  $" > "$  character, which indicates the shell prompt.

Note: This procedure is run with tape diagnostic and utility functions. See "IBM Tape Diagnostic Tool (ITDT)" on page 73.

# Configuration limitations

Maximum supported number of tape 1024 devices Maximum supported number of HBA 16 (8 dual- port, 4 quad- port) ports Maximum supported number of paths 16/16 for a device (DPF/CPF) Maximum LUNs per system 256

Every attached tape or library device uses a certain amount of resources. The user must consider resources such as physical memory and virtual space on the system, which might further limit the number of devices that can be attached.

# Components created during installation

Components created during installationThe lin_tape package consists of the device driver and a number of associated files. Components that are created during lin_tape installation (from the rpm package) are listed in Table 6 on page 41.

<table><tr><td colspan="2">Table 6. Linux: Components created during lin_tape installation</td></tr><tr><td>Component</td><td>Description</td></tr><tr><td>/lib/modules/(Your system&#x27;s kernel name)/kernel/drivers/scsi/lin_tape.ko
/lib/modules/(Your system&#x27;s kernel name)/kernel/drivers/scsi/pfo.ko</td><td></td></tr></table>

<table><tr><td>&lt;ecel&gt;</td><td></td></tr><tr><td>&lt;fcel&gt;</td><td>&lt;nl&gt;</td></tr><tr><td>&lt;fcel&gt;</td><td>&lt;nl&gt;</td></tr><tr><td>&lt;fcel&gt;</td><td>&lt;nl&gt;</td></tr><tr><td>&lt;fcel&gt;</td><td>&lt;nl&gt;</td></tr><tr><td>&lt;fcel&gt;</td><td>&lt;nl&gt;</td></tr></table>

Note: On a Unified Extensible Firmware Interface secure boot enabled kernel, modules are required to be signed before they can be loaded. Refer to Table 6 on page 41 to identify the device driver modules and their location after installation. Proceed to sign the device driver modules by following the operating system instructions while keeping the modules at the same location.

# Installation procedure

If lin_tape is already installed on your system, refer to the "Updating procedure" on page 42. This section assumes that you are installing the lin_tape device driver onto a system where it is not currently installed.

If you are installing lin_tape on a system that is running Linux for  $S / 390^{\circ}$  or Linux for zSeries, ensure that the OpenFCP adapter device driver zfcp is loaded in the kernel. Refer to "Configuring Tape and Medium Changer devices on IBM System z models" on page 43 for how to configure and install zfcp.

Make sure that the  $C / C + +$  development and kernel development packages are installed on your system. To install the lin_tape driver with all the added value of the lin_taped daemon, complete the following steps.

1. Download the appropriate level of the source RPM package to a directory of your choice on the Linux kernel for which you want to install it.

2. Run rpmbuild --rebuild <filename>, where: <filename> is the name of the RPM file. A binary RPM package is created for your kernel from the source RPM package. For example,

>rpmbuild - - rebuild lin_tape- 1. x.x.x.0- 1. src.rpm

Note: For the current lin_tape driver, it is possible to enable path failover for st/sg interfaces. See Enabling path failover for st and sg interface for details. If path failover is enabled and you want to also enable it for st or sg devices, an extra flag that is named sfmp must be added at this step:

>rpmbuild - - rebuild - with sfmp lin_tape- 1. x.x.x.0- 1. src.rpm

3. Output from the build is printed to your screen. Near the end of the output, a line indicates the file name and location of your binary RPM package. For example, a line similar to the following is output to your screen:

Wrote: /root/rpmbuild/RPMS/s390x/lin_tape- 1. x.x.x.0- 1. s390x.rpm

4. To install the lin_tape driver from the binary package, run >rpm - ivh <filename> For example,

>rpm - ivh /root/rpmbuild/RPMS/s390x/lin_tape- 1. x.x.x.0- 1. s390x.rpm

Note: For Ubuntu, run alien - i - - scripts <filename> (starting with lin_tape version 3.0.33).

5. To install the lin_taped daemon, download it to your Linux file system and run rpm - ivh on the daemon RPM file. For example,

>rpm - ivh ../lin_taped- 1. x.x.x.0- rhel9. s390x.rpm

Note: For Ubuntu, run alien - i - - scripts <lin_taped*.rpm>.

# Updating procedure

If your current lin_tape device driver was installed from an rpm package previously, you can uninstall the driver first, then install the newer version. For example,

On SLES and RHEL:

>rpm - e lin_taped>rpm - e lin_tapeThen, follow the installation procedure in the previous section.

On Ubuntu:

apt purge lin- tapedapt purge lin- tape

Then, follow the installation procedure in the previous section.

Note: All tape devices that use the lin_tape device driver must be closed and cannot be in use when lin_tape is uninstalled.

# Querying the installed package

The query is supported for the lin_tape device driver rpm package and SLES and RHELonly.

The installed rpm package can be queried by running the following commands to show information that is associated with the package.

To show information about lin_tape -

>rpm - qi lin_tape

To show the file list, enter the command

>rpm - ql lin_tapeTo show the states of files in the package, for example, normal, not installed, or replaced - >rpm - qs lin_tapeTo query lin_tape package on Ubuntu: dpkg - query - - list lin - tape

# Configuring Tape and Medium Changer devices on Intel-compatible systems

Physically attach your tape and medium changer devices to your Linux server.

After the driver software is installed and a tape device is connected to the adapter, the device can be configured and made available for use. Access to the device is not provided until the device is configured.

If your system is attached to a tape library with the integrated router, before the QLogic driver is installed, set the host type of the router to solaris and make sure that the logical unit numbers of the control unit, medium changer, and the connected tape drives are contiguous. Otherwise, the QLogic device driver does not recognize all of the attached devices. To view the LUNs of attached devices, log on to the router and use the fcfShowDevs command. If the LUNs are not contiguous, use the mapCompressDatabase command to delete the invalid LUNs and make the valid LUNs contiguous.

When you run the lin_tape kernel module, it creates special files in the /dev directory.

# Configuring Tape and Medium Changer devices on IBM System p models

Follow the same instructions as documented in the previous section. You must configure the Emulex Linux device driver if you have Fibre Channel tape devices that are attached to your System p (also known as pSeries) system.

# Configuring Tape and Medium Changer devices on IBM System z models

The Fibre Channel topology that is supported for System  $z^{\circ}$  is point- to- point and fabric. Refer to the Linux on System z Fibre Channel documents for details on the supported configurations for Fibre Channel device attachment. The Linux Fibre Channel adapter device driver zfcp is available in the kernel that supports zSeries Fibre Channel Protocol. The zfcp device configuration methods in 2.6 (and higher) and 2.4 kernels are different. For 2.6 kernels and higher, refer to appropriate chapter in the Linux on System z document entitled Linux on System z: Device Drivers, Features, and Commands.

For 2.4 kernels, there are three ways to load the zfcp device driver to see the attached tape devices.

1. Create a /etc/zfcp.conf file and make a ram disk to statically attach tape devices into your system. You can use this method only if you have a persistent mapping in a SAN environment. Every time that you restart the system, the zfcp is automatically loaded and the tape devices can be seen from the system.

You must add the device map into this file. The following code is an example of zfcp.conf.

0x11c0 0x1:0x500576300402733 0x0:0x0000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000

The zfcp device driver uses the "map" module parameter to recognize a physically attached tape device. map takes the following format,

map="<devno><port scsi- id>:<wwpn><unit- scsi- lun>:<fcp- lun>;..."

Where devnoThe device number of the host bus adapter (56 bits, see /proc/subchannel5). It is 0xf1c0 or 0xf1c1 in the previous example.

# devno

# port scsi-id

port scsi- idLinux internal SCSI ID assigned to the Fibre Channel port of the SCSI target device (32- bit, must not be 0, must be a unique one- to- one mapping for each worldwide port name. It is 0x1 in the previous example.

# wwwn

wwwnWorldwide port name that identifies the Fibre Channel port of the SCSI target device (64- bit). It is 0x5005076300402733 in the previous example.

# unit scsi-lun

unit scsi- lunLinux internal SCSI Logical Unit Number (32- bit). It is 0x0 in the previous example.

# fcp-lun

Logical Unit Number that is associated with the scsi target device (64- bit). In the previous example, 0x0000000000000000 is the Logical Unit Number 0, and 0x0001000000000000 is the Logical Unit Number 1.

For tape attachment, each logical unit number must be associated with a unique devno. If you use the same devno numbers for several logical units, you can ensure that each <unit- scsi- lun> is unique. After /etc/zfcp.conf is created, run the following commands.

>mk_initro>zipl

Then restart the system. After it is booted up, your tape device must be shown in /proc/scsi/scsi file.

2. Modify the /etc/modules.conf file to add the zfcp module parameters; then run the depmod -A and modprobe zfcp command.

Note: Do not use this choice together with the first one, otherwise it causes conflicts.

The zfcp map in /etc/modules.conf always takes higher priority than the map in /etc/zfcp.conf.

The following example demonstrates the zfcp configuration in /etc/modules.conf.

options zfcp map=\ 0xf1c0 0x1:0x5005076300402733 0x0:0x0000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000 0xf1c1 0x1:0x5005076300402733 0x0:0x0001000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000

The map arguments are the same as the ones listed in for the /etc/zfcp.conf file.

After the /etc/modules.conf file is modified, save and close it. Then, run the following command.

>depmod - A >modprobe zfcp

This action installs the zfcp device driver and all of its prerequisite kernel modules. Now you can check the file /proc/scsi/scsi to see whether all of the attached tape devices are shown in this file. If not, then check the Fibre Channel connection, such as the fibre cables, or if the devices are powered on.

Then, run the following commands to install zfcp.

>zmodprobe zfcp

3. Run the moccprobe zfcp command first, then dynamically add a tape device into the system after you physically attach a Fibre Channel tape device to the switch.

If you physically attach a tape device on the switch and zfcp is already loaded, you do not need to restart the Linux system to add this entry in the /proc/scsi/scsi file. The zfcp device driver provides an "add_map" proc system entry under the directory /proc/scsi/zfcp to dynamically add the device into the system. For example, to add two logical units from the example in Step 2 into the system, you can issue the following commands.

> echo "0xf1c0 0x1:0x5005076300402733 0x0:0x0000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000x1c1 0x1:0x5005076300402733 0x0:0x0001000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000. /proc/scsi/zfcp/add_map > echo "scsi add- single- device 0 0 1 0" > /proc/scsi/scsi > echo "scsi add- single- device 1 0 1 1" > /proc/scsi/scsi

The scsi add- single- device takes four parameters, corresponding to the four parameters scsi, Channel, Id, and Lun in the /proc/scsi/scsi file. The value of scsi is 0 for the first devno, 1 for the second devno (if it is different from the first devno), and so on. The value of Channel can start from 0 for each different scsi value. The value of Id is the one you use for <unit scsi- lun> in the previous mapping. The value of Lun is the logical unit number of the target device, for example, the last number in the previous mapping. Currently, the zfcp device driver does not support dynamically removing the attached devices. If you must remove the tape devices from the system, do rmmod zfcp. Then, you can delete the entry in /etc/modules.conf and reload zfcp, or reload zfcp first and dynamically add the devices that you want. After you finished all the mapping, if you can see all of the attached tape devices in /proc/scsi/scsi, you successfully attached those devices to your system. Next, you can install the lin_tape device driver. Refer to the "Installation procedure" on page 41 section for the instructions on how to install lin_tape.

# Uninstallation procedure

Note: All tape devices that use the lin_tape driver must be closed and cannot be in use when lin_tape is uninstalled or the uninstall fails.

On SLES and RHEL:

Run the following command to uninstall lin_taped before you uninstall lin_tape:

> rpm - e lin_taped

Run the following command to uninstall lin_tape:

>rpm - e lin_tape

On Ubuntu:

<table><tr><td>apt purge lin-taped</td></tr><tr><td>apt purge lin-tape</td></tr></table>

# Tape drive, media, and device driver parameters

This chapter describes the parameters that control the operating modes of the IBM Linux Tape and Medium Changer device driver.

# Configuration parameters

The configuration parameters are used to set the operating mode of the tape drive and device driver when a device is opened. The installation defaults are provided for all parameters initially. These parameters are kept on reopen, but are always restored back to the default values when the lin_tape device driver is reinstalled.

Note: This procedure is completed with tape diagnostic and utility functions. See "IBM Tape Diagnostic Tool (ITDT)" on page 73.

The nonchangeable configuration parameters are

- Autoloading- Density code- Emulate autoloader- Hook word- Maximum block size- Minimum block size- Medium type- Read SILI bit- Record space mode- Volume ID for logging- Write protect

The changeable configuration parameters are

- Barcode length- Block size- Buffered mode- Capacity scaling- Compression- Disable auto drive dump- Disable SIM logging- Dynamic attributes- Logging- Logical write protect- Maximum SCSI transfer length- Read past filemark- Rewind immediate- Trace- Trailer labels- Busy Retry

# Nonchangeable parameters

The configuration parameters are used to set the operating mode of the tape drive and device driver when a device is opened. The nonchangeable parameters are detailed as follows.

# Autoloading

This parameter enables the autoloading feature of the device driver. It is disabled by default and cannot be changed.

# Capacity scaling

This parameter sets the capacity or logical length of the current tape. By reducing the capacity of the tape, the tape drive can access data faster at the expense of data capacity. Capacity scaling is not supported currently but might be supported in future releases of lin_tape.

46 IBM Tape Device Drivers Installation and User's Guide

# Density code

Density codeThis parameter is the density setting for the currently loaded tape. Some tape devices support multiple densities and report the current setting in this field. It cannot be changed by the application.

# Emulate autoloader

This parameter currently is not supported and is ignored.

# Hook word

Hook wordThis parameter is not supported in the lin_tape device driver.

# Logical write protect

This parameter sets or resets the logical write protect of the current tape. This feature is not supported currently but might be supported in future releases of the lin_tape.

# Maximum block size

This parameter is the maximum block size for the device.

# Minimum block size

This parameter is the minimum block size for the device.

# Medium type

Medium typeThis parameter is the media type of the current loaded tape. Some tape devices support multiple media types and different values are reported in this field.

# Read SILI bit

Read SILI bitSILI bit currently is not supported due to limitations associated with the Linux environment. SILI bit support can be enabled in future releases of the lin_tape.

# Record space mode

Record space modeThis parameter specifies how the device driver operates when a forward or backward space record operation encounters a filemark. Only the SCSI mode is supported by lin_tape. When a forward or backward space record operation is issued to the driver and a filemark is encountered, the device driver returns - 1 and the errno variable is set to input/output error (EIO). On the forward space operation, the tape is left- positioned after the filemark (the end of tape side of the filemark). On the backward space operation, the tape is positioned before the filemark (the beginning of tape side of the filemark).

# Volume ID for logging

Volume ID for loggingThis parameter is the volume ID of the currently loaded tape. The lin_tape device driver ignores this field.

# Write protect

Write protectThis parameter is set to TRUE if the currently mounted tape is logically or physically write protected.

# Changeable parameters

Changeable parametersThe configuration parameters are used to set the operating mode of the tape drive and device driver when a device is opened. The changeable parameters are detailed as follows.

# Barcode length

This parameter can be set to change the barcode length for a cartridge. For LTO cartridges the default is 8. It can be changed to 6 for LTO 1 and LTO 2 generation cartridges only. For 3592 cartridges the default is set at 6. It can be changed to 8. In the /etc/modprobe.conf.local file the following line must be added to reflect the desired change.

options lin_tape ibm3592_barcode  $= 8$  options lin_tape lto_barcode  $= 6$

# Block size

Block sizeThis parameter specifies the block size that is used for read and write operations. A value of zero means a variable block size. Any other value is a fixed block size. The installation default is zero (variable length block size). Refer to "Maximum SCSI transfer length" on page 49 for guidance.

# Buffered mode

Buffered modeThis parameter specifies whether read and write operations must be buffered by the tape device. The default (recommended) value is TRUE.

# Capacity scaling

Capacity scalingThis parameter sets the capacity or logical length of the current tape on Enterprise Tape System 3590 or 3592 tape subsystems. By reducing the capacity of the tape, the tape drive can access data faster at the expense of data capacity. Capacity scaling can be set at  $100\%$  for the entire tape (which is the default), or set at  $75\%$ $50\%$  or  $25\%$  of the 3590 tape cartridge and more available capacity scaling for the 3592 standard 300 GB rewritable data cartridge. Capacity scaling remains with the tape across mounts until it is changed.

# Note:

Note:1. The tape position must be at the start of the tape to change this parameter from its current value.2. Changing this parameter destroys any existing data on the tape.3. For 3592 media types, capacity scaling is supported only for the standard 300 GB rewritable data cartridge. Attempting to set capacity scaling that is not supported by a device or the current loaded media always returns  $100\%$  and cannot be changed. For example, the 60 GB (Economy Data) cartridge for the IBM 3592 cannot be capacity- scaled and is always  $100\%$ .

# Compression

Hardware compression is implemented in the device hardware. This parameter turns the hardware compression feature On and Off. If compression is enabled, the effective performance can increase, based on the compressibility of the data.

The installation default is On (use compression).

# Disable auto drive dump

Disable auto drive dumpThis parameter is provided in the lin_tape version 1.2.2 or later. It is set to FALSE by default. If it is FALSE and the lin_taped daemon is running and if an error occurs in the drive that creates a drive dump, the lin_tape device driver automatically retrieves the drive dump and saves it under the /var/log directory by default. You can specify another directory in the /etc/lin_taped.conf file. The dump is labeled with a .dmp extension on the file. Refer to "Configuring and running the lin_taped daemon" on page 60 for details.

# Disable SIM logging

This parameter is provided in the lin_tape version 1.2.2 or later. It is set to FALSE by default. If it is FALSE and the lin_taped daemon is running and SIM/MIM data is generated by the drive, the lin_tape device driver automatically retrieves the data and saves it in a formatted text file under the /var/log directory by default. You can specify another directory in the /etc/lin_taped.conf file. Refer to "Configuring and running the lin_taped daemon" on page 60 for details.This capacity is not applicable to IBM Ultrium tape drives.

This capacity is not applicable to IBM Ultrium tape drives.

# Dynamic attributes

Dynamic attributesThis parameter determines whether dynamic runtime attributes are attempted on open for supported drives. Default is 1 (oh) meaning that the driver automatically attempts to set dynamic runtime attributes on open. This parameter can be changed to 0 (off) in the configuration file before the lin_tape is loaded. It is recommended to keep on dynamic attributes unless it produces an unexpected problem in the environment.

# Logging (volume logging)

This parameter turns the volume information logging On or Off. With the lin_tape version 1.2.2 and later, the lin_tape device driver provides this support. It is set to On by default. If logging is On and the lin_taped daemon is running, the lin_tape device driver retrieves the full log sense data from the drive whenever a tape is unloaded, or the drive reaches a log threshold. The log file is saved in binary format under the directory /var/log by default. You can specify another directory in /etc/lin_taped.conf file. Refer to "Configuring and running the lin_taped daemon" on page 60 for details.

Note: This parameter is volume logging, which is different from error logging. lin_tape provides error logging whenever the lin_taped daemon is running. Refer to "Configuring and running the lin_taped daemon" on page 60 for details on error logging.

# Logical write protect

This parameter sets or resets the logical write protect of the current tape on Enterprise Tape System 3590 tape subsystems. The three types of logical write protect are associated protect, persistent protect, and write- once read- many (wORM) protect.

1. Associated protect remains only while the current tape is mounted or associated with the tape drive. It is reset when the tape is unloaded or the tape drive is reset. 
2. Persistent protect remains or persists with the tape across mounts until it is reset. 
3. WORM protect also remains with the tape across mounts, but unlike persistent protect it cannot be reset on the tape. After a tape is WORM protected, it can never be written on again.

Note: The tape position must be at the start of the tape to change this parameter from its current value.

# Maximum SCSI transfer length

Maximum SCSI transfer lengthIn the lin_tape drivers with level lower than 3.0.3, the maximum transfer length per device per SCSI command is 262144 bytes (256 KB) by default. Variable block read/write requests with transfer length greater than the maximum transfer length fails [errno: EINVAL]. When a fixed block size is defined, large write requests are subject to both the granularity of the block size and the maximum transfer length. For example, with a fixed block size of 80000 bytes and maximum transfer length of 262144, a write request for 400000 bytes (5 blocks of 80000 each) is written to tape in two transfers. The first transfer is 240000 bytes (3 blocks) and the second transfer is 160000 (the remaining two blocks). You can increase the maximum transfer length to enhance the data throughput. This procedure can be done with ITDT with the Query/Set Parameters option, or a customized STIOCSETP input/output control (ioctl) call. However, setting the transfer length greater than the default 256 KB does not guarantee a noticeable increase in data throughput. Maximum transfer length of 256 KB is highly recommended.

In lin_tape driver with level 3.0.5 or higher and the open source driver lin_tape, the maximum transfer length is defined as the minimum length that the host bus adapter and the tape drive can support. This number is greater than 256 KB. It cannot be changed by the STIOCSETP ioctl call any more.

# Read past filemark

If this parameter is set to true, when a fixed- length read operation encounters a filemark, it returns the number of bytes read before the filemark is encountered and positions the tape head after the filemark. If the read_past_filemark parameter is set to false, when the fixed- length read operation encounters a filemark, if data was read, the read function returns the number of bytes read, and positions the tape head before the filemark. If no data was read, then the read returns 0 bytes read and positions the tape head after the filemark.

This installation default is FALSE.

# Rewind immediate

This parameter sets the immediate bit for rewind commands. If it is set to On, the rewind tape operation runs faster, but the next command takes a long time to finish unless the physical rewind operation is complete. Setting this parameter reduces the amount of time it takes to close a device for a Rewind on Close special file.

The installation default is Off (no rewind immediate).

# Trace

This parameter turns the trace facility On or Off. With the lin_tape version 1.2.2 and later, the lin_tape device driver provides this support. It is set to On by default. If trace is On and the lin_taped daemon is running, the lin_tape device driver retrieves the trace from the driver if trace level is set to 1 or 2 in the / etc/lin_taped.conf file. The trace file is saved under the directory /var/log by default. You can specify another directory in /etc/lin_taped.conf file. Refer to "Configuring and running the lin_taped daemon" on page 60 for details.

# Trailer labels

If this parameter is set to On, then writing records past the early warning mark on the tape is allowed. The first write operation after detecting the early warning mark fails and the errno variable is set to ENOSPC. No data is written during the operation. All subsequent write operations are allowed to continue until the physical end of the volume is reached and errno EIO is returned.

If this parameter is set to Off, then writing records past the early warning mark is not allowed. Errno variable is set to ENOSPC.

The installation default is On (with trailer labels).

# Busy Retry

The parameter busy_retry determines how many times to retry a command when the device is busy. Default is 0 (off), and can be set up to 480 in the configuration file before the lin_tape is loaded.

# lin_tape_ignoreOEM

The parameter lin_tape_ignoreOEM stops OEM devices from connecting to the driver. Default is 0 (off), and can be set up to 1 in the configuration file before the lin_tape is loaded.

# Special files

After the driver is installed and a device is configured and made available for use, access is provided through the special files. These special files, which consist of the standard Linux special files for devices, are in the /dev directory.

# Special files for the tape device

Each tape device has a set of special files providing access to the same physical drive but providing different attributes. Table 7 on page 51 shows the attributes of the special files.

Note: The asterisk  $(^{\star})$  in IBMtape\* represents a number assigned to a particular device, such as IBMtapeo.

For tape drives with attached medium changer devices, the IBMchanger\* special file provides a separate path for issuing commands to the medium changer. When this special file is opened, the application can view the medium changer as a separate device. Both the tape and changer special file can be opened at the same time.

<table><tr><td colspan="2">Table 7. Linux: Special files for IBM tape devices</td></tr><tr><td>Special file name</td><td>Rewind on Close</td></tr><tr><td>/dev/IBMTape*</td><td>YES</td></tr><tr><td>/dev/IBMTape*n</td><td>NO</td></tr></table>

# Special files for the Medium Changer device

After the driver is installed and a medium changer device is configured and made available for use, access to the robotic device is provided through the IBMchanger special file in the/dev directory. The asterisk  $(^{\star})$  represents a number that is assigned to a particular device, such as IBMchanger0. The term IBMchanger is used for a SCSI medium changer device. The IBMchanger\* special file provides a path for issuing commands to control the medium changer robotic device.

The file descriptor that results from opening the IBMchanger special file does not support the following operations.

Read Write Open in Append mode Commands that are designed for a tape device

# Persistent naming support

Lin_tape Persistent Naming is implemented through the Linux udev utility. Udev is a service that monitors changes in system hardware configuration and completes actions that are based on what devices are attached to the Linux system. It can be configured to create symbolic links (persistent names) to a device based on attributes that a driver exports for that device. The persistent name can then be used as the device name to open and complete IO to a tape drive or medium changer. This action makes it possible to reference a static name, such as

/dev/lin_tape/by- id/lin_tape4801101

This name is always associated with the same physical device, rather than being required to reference the device name /dev/IBMtape0, which can change names and become /dev/IBMtapel after the driver is reinstalled.

Lin_tape exports several attributes that can be used as the basis to create persistent names. These attributes can be reported to the user through udevadm info on recent Linux kernels, or udevinfo on

older Linux kernels. The udevinfo and udevadm are udev management tools. These tools can query the udev database for device information and properties that are stored in the udev base for help creating udev rules.

Note: Udev, udevinfo, and udevadm are not implemented or maintained by the lin_tape driver. Refer to the man pages for udevadm or udevinfo for details on usage. These man pages are system specific and supersede all information in this document. For questions on these utilities, you must contact your Linux support representative.

An example is provided on udev for implementing a persistent name. The example must be customized to fit a user's needs and environment.

Note: Variations exist between kernels.

If a tape device is attached to the Linux system with worldwide port name 0x123456789ABCDEFGF0 with a current device name of /dev/IBMtape0, a user can run udevadm information to obtain information on exported attributes for this device. This procedure can be done as follows,

>udevadm info - - attribute- walk - - name /dev/IBMtap0

The output of this command includes something similar to

ATTRSserial_num  $\equiv =$  "123456789" ATTRSww_node_name  $\equiv =$  "0x123456789ABCDEFG1" ATTRSww_port_name  $\equiv =$  "0x123456789ABCDEFG0"

If you are using udevinfo, enter the previous command as >udevinfo - a - p `udevinfo - q path - n /dev/IBMtap0` or >udevinfo - a - p /sys/class/lin_tape/IBMtap0 (Both commands return the same information).

Also, on some kernels an attribute ATTRSxxx is replaced by SYSFSxxx. Furthermore, some kernels use a  $\equiv$  (single equal sign) to indicate an attribute match and also an assignment. Whereas, other kernels use a  $\equiv =$  (double equal sign) for a match and  $\equiv$  for assignment. Place the attribute from the attribute list into your rules file exactly as it appears in the attribute list, as described here.

The ww_port_name is in a rules file that assigns a symbolic link to a device that has the listed worldwide port name. The file typically is placed in /etc/udev/rules.d, but this location might be changed by the udev_rules directive in the /etc/udev/rules.conf file. In this example, a file is created that is called /etc/udev/rules.d/98- lin_tape.rules and write a single line to the file.

KERNEL  $\equiv =$  "IBMtapex",ATTRSww_port_name  $\equiv =$  "0x123456789ABCDEFG0", SYMLINK  $\equiv =$  "lin_tape/by- id/lin_tape4801101"

Assuming that the udev service is running and configured correctly, the user can install or reinstall lin_tape with modprobe, and the symbolic link is created in the /dev/lin_tape/by- id folder. One line must be added to the 98- lin_tape.rules file for each wanted symbolic link.

With lin_tape version 2.2.0, a parameter, persistent_n_device, is added to support persistent naming of no rewind devices. The default value is 0 (off). To enable this support, complete the following steps.

1. Modify the 98-lin_tape.rules file to differentiate standard devices and no rewind devices. For example,

KERNEL  $\equiv =$  "IBMtapex[0- 9]",ATTRserial_num  $\equiv =$  "1013000306", SYMLINK  $\equiv$  "lin_tape/by- id/IBMtap0" KERNEL  $\equiv =$  "IBMtapexn",ATTRserial_num  $\equiv =$  "1013000306", SYMLINK  $\equiv$  "lin_tape/by- id/IBMtap0n"

2. Stop lin_tape.

systemct1 stop lin_tape

3. Add the following line in your /etc/modprobe.conf or /etc/modprobe.conf.local file (or, if you are running RHEL 6 or higher, in your /etc/modprobe.d/lin_tape.conf file).

options lin_tape persistent_n_device=1

4. Start lin_tape.

systemctl start lin_tape

Note: Wait at least 10 seconds between stop and start in order for udev to correctly configure the devices.

5. Check that the devices are all correctly listed with the following command.

is - 1 /dev/lin_tape/by- id/

# Control Path failover support for tape libraries

Note: The library control path failover feature code must be installed before control path failover support is enabled in the Linux lin_tape device driver. Refer to"Automatic failover" on page 4 to determine which feature code is required for your machine type.

The Linux lin_tape device driver control path failover support configures multiple physical control paths to the same logical library within the device driver. It also provides automatic failover to an alternative control path when a permanent error occurs on one path. This support is transparent to the running application.

# Configuring and unconfiguring path failover support

Control path failover support is not enabled automatically when the device driver is installed. The Linux lin_tape device driver provides a driver parameter alternate pathing for you to enable the library control path failover. To enable the failover support in the lin_tape device driver software, you must do the following steps after the lin_tape rpm package is installed:

1. Stop lin_tape.

systemctl stop lin_tape

2. Add the following line in your /etc/modprobe.conf or /etc/modprobe.conf.local file (or, if you are running RHEL 6 or higher, in your /etc/modprobe.d/lin_tape.conf file)

options lin_tape alternate_pathing=1

3. Start lin_tape.

systemctl start lin_tape

You can check whether the lin_tape driver recognized multiple control paths for your library by reading the /proc/scsi/IBMchanger file.

cat/proc/scsi/IBMchanger

If your library lists "Primary" or "Alternate" under "FO Path", you successfully enabled the control path failover feature for your library. If "NA" is listed under "FO Path", then the control path failover is not enabled. After control path failover support is enabled, it remains set until the lin_tape driver is reloaded with the alternate pathing driver parameter set to OFF. The path failover setting is retained even if the system is rebooted. If you want to turn off the control path failover feature in the lin_tape device driver, you can complete the following steps.

1. Stop lin_tape.

systemctl stop lin_tape

2. Delete the following line in your /etc/modprobe.conf file.

options lin_tape alternate_pathing=1

3. Start lin_tape.

systemct1 start lin_tape

# Enabling path failover for the sg interface

Through a collaboration effort that is named join driver, control path failover is supported through the sg interface, by using the latest lin_tape driver version on RHEL over Intel only.

Note: It is important to review hardware and software requirements before path failover is enabled. See "Path failover and load balancing" on page 3.

To enable it, the lin_tape_as_sfmp parameter must be set at /etc/modprobe.d/lin_tape.conf as follows:

options lin_tape alternate_pathing=1 options lin_tape lin_tape_as_sfmp=1

If lin_tape is already installed, follow the "Uninstallation procedure" on page 45.

Follow the "Installation procedure" on page 41 by using - with sfmp at rpmbuild.

# Primary and alternative paths

When lin_tape is loaded into kernel memory, the first logical medium changer device that lin_tape sees in the system is the primary path for that medium changer. The other logical medium changers that lin_tape attached for the same medium changer are configured as alternative paths. The device driver supports up to 16 physical paths for a single device. The primary and alternative path information can be obtained by the following command.

cat /proc/scsi/IBMchanger An example of a /proc/scsi/IBMchanger file: lin_tape version:3.0.3 lin_tape major number:253

Table 8.Attached changer devices  

<table><tr><td>Number</td><td>Model</td><td>SN</td><td>HBA</td><td>FO Path</td></tr><tr><td>0</td><td>03584L22</td><td>IBM1234567</td><td>qla2xxx</td><td>Primary</td></tr><tr><td>1</td><td>03584L22</td><td>IBM1234567</td><td>qla2xxx</td><td>Alternate</td></tr><tr><td>2</td><td>03584L22</td><td>IBM1234567</td><td>qla2xxx</td><td>Alternate</td></tr></table>

The labeling of a logical device as either a primary or alternative path is for information only, to

Identify the actual number of physical devices that are configured on the system and a specific logical device that is associated with them. Only one logical device is labeled as the primary path for each physical device. However, multiple logical devices can be labeled as an alternative path for the same devices. Provide information about which logical devices configured on the system have path failover support enabled.

The numbers listed in Table 8 on page 54 are the ones used for IBMchanger special files at /dev directory (see Special Files for the Medium Changer device). An attempt to open a device file name not

listed at /dev directory will fail. Per file systems handling and due to path removal, usually a device file name is deleted after a device closes or before a device opens. Using Persistent naming support will maintain device file names listed under ls - l /dev LIN_tape/by- id/.

When lin_tape_as_sfmp is set, sg paths can be queried through pfo paths as follows:

cat /sys/class/pfo/\*/paths

Example output:

pfo10 sg=/dev/sg10 st=none sf=yes fo=no wwnn=00000013400140405 type=changer 2:0:1:1 up last 3:0:1:1

There is only one sg device file name per device that uses all the paths for this device.

# Querying primary and alternative path configuration

You can show the primary and alternative path configuration for all devices by reading the /proc/scsi/ IBMchanger file, as explained in "Primary and alternative paths" on page 54.

# Disabling and enabling primary and alternative paths

When you load the lin_tape device driver with the alternate_pathing parameter to be ON, by default, all the available paths for a physical device are enabled.

If it is necessary to disable a path and not run path failover (for example, because of maintenance), run commands to disable and then later enable the primary and alternative paths.

The commands to enable and disable primary and alternative paths are tape diagnostic and utility functions.

Note: See "IBM Tape Diagnostic Tool (ITDT)" on page 73.

# Data Path failover and load balancing support for tape drives

Data path failover support is not enabled automatically when the device driver is installed. The Linux lin_tape device driver provides a driver parameter alternate_pathing for you to enable the data path failover.

To enable the failover support in the lin_tape device driver software, you must complete the following steps after the lin_tape rpm package is installed.

>systemctl stop lin_tape

Add the following line in your file /etc/modprobe.d:/lin_tape.conf.

options lin_tape alternate_pathing  $= 1$

Save the file, then run the following commands.

>systemctl start lin_tape

You can check whether the lin_tape driver recognized multiple paths for your tape drive by reading the / proc/scsi/IBMtape file.

>cat /proc/scsi/IBMtape

If your tape drive lists Primary or Alternate under FO Path, you successfully enabled data path failover feature for your tape drive. If NA is listed under FO Path, the data path failover is not enabled. After the path failover support is enabled, it remains set until the lin_tape driver is reloaded with the alternate_pathing driver parameter set to OFF. The path failover setting is retained even if the system

is rebooted. If you want to turn off the data path failover feature in the lin_tape device driver, you can run the following steps.

>systemctl stop lin_tapeDelete the following line in your /etc/modules.conf file: options lin_tape alternate_pathing=1. >systemctl start lin_tape

# Enabling path failover for st and sg interface

Through a collaboration effort that is named join driver, control path failover is planned to be supported through st and sg interfaces by using the latest lin_tape driver version or RHEL over Intel only.

Important: Review hardware and software requirements before path failover is enabled. See "Path failover and load balancing" on page 3.

To enable it, the lin_tape_as_sfmp parameter must be set at /etc/modprobe.d/lin_tape.conf:

options lin_tape alternate_pathing=1 options lin_tape lin_tape_as_sfmp=1

If lin_tape is already installed, follow the "Uninstallation procedure" on page 45.

Follow "Installation procedure" on page 41 by using - with sfmp at rpmbuild.

# Primary and alternative paths

When the lin_tape device driver is loaded into kernel memory with path failover support enabled, the first logic device that lin_tape sees always becomes the primary path. The other logical devices that lin_tape sees are configured as the alternative paths. The device driver supports up to 16 physical paths for a single device.

The primary and alternative path information can be obtained by the following command.

>cat /proc/scsi/IBMtape

The following is an example of a /proc/scsi/IBMtape:

lin_tape version: 3.0.3  lin_tape major number: 253

Table 9.Attached tape devices  

<table><tr><td>Number</td><td>Model</td><td>SN</td><td>HBA</td><td>FO Path</td></tr><tr><td>0</td><td>03592</td><td>IBM1234567</td><td>qla2xxx</td><td>Primary</td></tr><tr><td>1</td><td>03592</td><td>IBM1234567</td><td>qla2xxx</td><td>Alternate</td></tr></table>

The labeling of a logical device as either a primary or alternative path is for information only and is used for following purpose:

- Identify the actual number of physical devices that are configured on the system and a specific logical device that is associated with them. Only one logical device is labeled the primary path for each physical device. However, many (multiple) logical devices can be labeled as an alternative path for the same device. 
- Provide information about which logical devices configured on the system have path failover support enabled.

The numbers listed in Table 9 on page 56 are the ones used for IBMchanger special files at /dev directory (see Special files for the tape device). An attempt to open a device file name not listed at /dev directory will fail. Per file systems handling and due to path removal, usually a device file name is deleted after a device closes or before a device opens. Using Persistent naming support will maintain device file names listed under ls - l /dev/lin_tape/by- id/.

When lin_tape_as_sfmp is set, st and sg paths can be queried through pfo paths as follows:

cat /sys/class/pfo/\*/paths

Example output:

pfo9 sg=/dev/sg9 st=/dev/st0 sf=yes fo=no wwnn=500507630f04be07 type=tape 2:0:1:0 up last - wwpn=500507630f44be07 3:0:1:0 - - - -

Only one st and one sg device file name per device can use all the paths for that device.

# Querying primary and alternative path configuration

You can show the primary and alternative path configuration for all devices by reading the /proc/scsi/ IBMtape file, as explained in "Primary and alternative paths" on page 56.

# Disabling and enabling primary and alternative paths

If it is necessary to disable a path and not run path failover (for example, because of maintenance), run commands to disable and then enable the primary and alternative paths.

The commands to enable and disable primary and alternative paths are tape diagnostic and utility functions.

Note: See "IBM Tape Diagnostic Tool (ITDT)" on page 73.

# Tape Reserve Type

This parameter causes lin_tape to issue SCSI- 3 persistent reserves to a tape drive whenever a reservation is attempted. Persistent reserves are automatically issued if data path failover is used, and therefore setting the parameter is unnecessary. This parameter can be set only when lin_tape is installed. To set it, add the following line to /etc/modprobe.conf or /etc/modprobe.conf.local (or, if you are running RHEL 6 or higher, to /etc/modprobe.d/lin_tape.conf):

options lin_tape tape_reserve_type=persistent

If alternate pathing is not enabled persistent reserve is not issued automatically, so this parameter is needed only if you want to use persistent reserve with alternate pathing disabled.

# Open source device driver - lin_tape

The lin_tape device driver is the new device driver for the Linux 2.6 kernels to replace the closed- source driver IBMtape. In most respects, it behaves the same as the closed- source IBMtape device driver. This section covers significant differences between the IBMtape driver and the lin_tape driver.

# Comparing IBMtape and lin_tape

Table 10 on page 58 compares the names for various components of the IBMtape and lin_tape device drivers.

<table><tr><td colspan="3">Table 10. Comparing IBMtape and lin_tape</td></tr><tr><td>Component</td><td>IBMtape</td><td>Lin_tape</td></tr><tr><td>Driver name</td><td>IBMtape</td><td>lin_tape</td></tr><tr><td>Module name</td><td>IBMtape.ko</td><td>lin_tape.ko</td></tr><tr><td>Special files</td><td>/dev/IBMtape0
/dev/IBMchanger0, and so on.</td><td>No change</td></tr><tr><td>Proc entry</td><td>/proc/scsi/IBMtape
/proc/scsi/IBMchanger</td><td>No change</td></tr><tr><td>Daemon name</td><td>IBM taped</td><td>lin_taped</td></tr><tr><td>Daemon configuration file</td><td>/etc/IBM taped.conf</td><td>/etc/lin_taped.conf</td></tr><tr><td>Daemon trace files</td><td>/var/log/IBMtape.trace
/var/log/IBMtape.errorlog</td><td>/var/log/lin_tape.trace
/var/log/lin_tape.errorlog</td></tr></table>

Lin_tape join driver installs pfo.ko, an extra lin_tape module that is idle for non- sftp configurations, and blacklist- pfo.conf at the location /etc/modprobe.d for pfo module load control.

# Installation

Installation of the lin_tape driver is the same as for the IBMtape driver, except that IBMtape must be replaced with lin_tape in the installation instructions. Refer to "Installation and Configuration instructions" on page 40 for details.

The lin_tape driver cannot be installed if the IBMtape driver is already installed. If the IBMtape driver is installed, first uninstall the IBMtape driver, and then install the lin_tape driver. With RHEL4 and SLES10, driver removal also requires a reboot of the server, since the IBMtape driver module is "permanent" in these distributions.

# Driver parameters and special device files

The driver parameters are not changed for the lin_tape driver. However, it is important that the module parameters, such as alternate_pathingand dpf_keys, must now be applied to the lin_tape module, instead of the IBMtape module. For example, in the /etc/modprobe.conf or /etc/modprobe.conf.local file (or, if you are running RHEL 6 or higher, the /etc/modprobe.d/lin_tape.conf file), add the following line for LTO library's path failover:

options lin_tape alternate_pathing  $= 1$  dpf_keys  $=$  "abcdefghijklmnop" abckdefghijklmnop is an example of a data path failover feature key.

The special device files for the lin_tape driver are the same as for the IBMtape driver. Refer to "Special files for the tape device" on page 51 and "Special files for the Medium Changer device" on page 51 for details on special device files.

# Taking devices offline and completing maintenance

Taking devices offline and completing maintenanceInput and output must be quiesced and all driver handles must be closed before a lin_tape device is taken offline. It is recommended to remove the lin_tape driver module and then the HBA driver module before maintenance is done or the physical topology of the tape drive or library environment is changed. Lin_tape can be removed by the following command at the shell prompt.

systemctl stop lin_tape

Likewise, the HBA module can be removed by the following command, where "HBA_driver" is your specific HBA driver.

rmmod <HBA_driver>

After the maintenance is complete and the environment is ready to issue input and output, the drivers must be reinstalled in reverse order. This procedure is typically done by systemctl start lin_tape

# Path failover support

Path failover support in lin_tape is the same. However, with the lin_tape driver, failover support is provided through the lin_taped daemon. If the lin_taped daemon is not running, neither control path failover nor data path failover is attempted. The lin_taped daemon is started automatically when the lin_tape driver is loaded.

To check whether the lin_taped daemon is running, run the following command.

lin_taped status

This command indicates whether the lin_taped daemon is running. If the /proc/scsi/IBMtape and /proc/scsi/IBMchange files indicate "NA" for "FO Path", this answer indicates that failover support for that device is not enabled. If all other settings are correct, but "FO Path" is incorrectly indicating "NA", confirm that the lin_taped daemon is running.

For details about path failover support, refer to "Control Path failover support for tape libraries" on page 53 and "Data Path failover and load balancing support for tape drives" on page 55.

# lin_taped daemon

lin_taped daemonThe lin_taped daemon uses the same command- line arguments as the IBMtaped daemon. The lin_taped configuration file is the same as the IBMtaped configuration file, but is renamed to lin_taped.conf. Refer to "Configuring and running the lin_taped daemon" on page 60 for information.

# Problem determination

Problem determinationA set of tools is provided with the device driver to determine whether the device driver and the tape device are functioning correctly.

# Tracing driver modules

By default, the driver prints minimal kernel trace messages to the system log at /var1Log/messages. The minimal information includes notification that a device is recognized or taken offline and also the most serious of error conditions. If a more verbose trace is wanted, the variable /sys/bus/scsi/drivers/ lin_tape/lin_tape_debug must contain the value 1. This procedure can be accomplished in one of two ways -

Add the following line to /etc/modprobe.conf, /etc/modprobe.conf.local, or /etc/modprobe.d/ lin_tape.conf:

options lin_tape lin_tape_debug=1

Then reinstall lin_tape. This action causes the lin_tape_debug variable to be set every time lin_tape is loaded.

- Issue the following command from the shell.

echo 1 > /sys/bus/scsi/drivers/lin_tape/lin_tape_debug

This action causes the lin_tape_debug variable to be set only until lin_tape is uninstalled or until the variable is set back to 0.

# Configuring and running the lin_taped daemon

Starting with lin_tape version 1.2.5, the lin_tape device driver provides an error diagnostic daemon (lin_taped) which provides the following capabilities:

1. Error logging and tracing2. When drive dumps, log sense data, or SIM/MIM error information is created by the tape drive, the daemon automatically retrieves that data and saves it to the hard disk drive on your Linux system.

3. Failover and load balancing

4. Encryption

Because lin_taped requires a minimal amount of system resource and because it provides these necessary diagnostic capabilities, IBM recommends that you leave the daemon always enabled.

# Installing lin_taped

lin_taped is automatically installed at/usr/bin/lin_taped when you install the lin_tape device driver with the rpm or tar package. Refer to"Installation and Configuration instructions" on page 40 for instructions on installing the lin_tape device driver.

# Configuring lin_taped

You can customize the operation of lin_taped by modifying its configuration file, which is at /etc/lin_taped.conf. The daemon reads only the configuration file when it starts; so if you modify the configuration file, stop the daemon, and restart it so that your modifications are recognized by the daemon.

# Tracing:

Three levels of tracing are supported for the lin_taped daemon. lin_taped tracing is a complement to, but is different from, tracing of the kernel module that is described in "Tracing driver modules" on page 59. The lin_taped tracing levels are defined as follows:

0 With tracing set to 0, lin_taped records minimal tracing.1 With tracing set to 1, lin_taped records information that is associated with each ioctl called. If a device error occurs and SCSI sense data is obtained from the device, a subset of that sense data is also recorded. The default setting for tracing.2 With tracing set to 2, lin_taped records tracing messages for each SCSI command. If a device error occurs and SCSI sense data is obtained form the device, all sense data is also recorded. This tracing level is used only when a specific problem is being diagnosed due to the potential for huge amounts of data that is generated.

Set the lin_tapeTrace variable in the /etc/lin_taped.conf file to 0, 1, or 2, depending on what level of tracing you want. If the lin_tapeTrace variable is set to an invalid number, the lin_taped daemon does not start.

Tracing information is written to a file named /var/log/lin_tape.trace, by default. Information is written into the file until it is 1 MB in size, by default. After 1 MB of information is written, the file is archived (using the Linux ar command) into file lin_tape.a in the same directory. In the archive, the file name is renamed to lin_tape.trace.timestamp, where timestamp reflects the time that the file was archived.

You can change the directory to which the tracing information is written or the default maximum size of the trace file by modifying settings in the lin_taped.conf file. Refer to the instructions in the lin_taped.conf file for details.

# Error logging:

lin_taped records certain error messages from the lin_tape device driver in a file named /var/log/lin_tape.errorlog, by default. Information is written into the file until it is 1 MB in size, by default. After 1 MB of trace information is written, the file is archived (with the Linux ar command) into file lin_tape.a in the same directory. In the archive, the file name is renamed to lin_tape.errorlog.timestamp, where timestamp reflects the time that the file was archived.

You can change the directory to which the error logging information is written or the default maximum size of the error log file by modifying settings in the lin_taped.conf file. Refer to the instructions in the lin_taped.conf file for details.

Whenever the lin_taped daemon is running, error logging is enabled if tracing is enabled. Following is an example an error log record.

<table><tr><td>IBM tape0--E6001</td><td>Tue Sep 10 14:04:57 2002</td></tr><tr><td>Scsi Path</td><td>03 00 00 00</td></tr><tr><td>CDB Command</td><td>01 00 00 00 00 00</td></tr><tr><td>Status Code</td><td>08 00 00 01</td></tr><tr><td>Sense Data</td><td>70 00 04 00 00 00 00 58 00 00 00 00 00 00 00 00 00 00 00 00 00 00 00 00 00 00 00 00 00 00 00 00 00 00 00 00 00 00 00 00 00 00</td></tr><tr><td>Description</td><td>Hardware Error</td></tr></table>

The first line indicates the tape device special file name and the device serial number, and the timestamp when the error message was recorded. "Scsi Path" is the SCSI path for this logical unit. It matches the order of the scsi/Channel/Id/Lun information in the /proc/scsi/scsi file. "CDB Command" is the command data block of the SCSI command. "Status Code" is the returned result from the Linux SCSI middle layer device driver (scsi_mod.o). The 4 bytes represent driver, byte, host, byte, msg, byte, and status_byte. "Sense Data" is the full SCSI sense data that is returned from the target. "Description" is a person- readable text string that is obtained by parsing the sense key field of the sense data.

The following circumstances are not logged in the lin_tape.errorlog file:

1. Sense key is 0, and the sense data indicates an overlength or an underlength read, or encountering a file mark or the end of data2. Sense key is 2, and the ASC/ASCQ indicates that the device is becoming ready3. Sense key is 6, indicating a unit attention4. Sense key is 8, and the ASC/ASCQ indicates the end of data

# Volume logging:

The lin_tape device driver retrieves the full log sense data from the tape drive whenever the drive reaches a log threshold, or a tape is unloaded from the drive, or the drive is reset through an application. This data is stored in binary in a file named lin_tape.timestamp.log, where: lin_tapen is the device special file (for example, lin_tape1, lin_tape2) and timestamp reflects the time that the file was created. Each time log sense data is obtained, it is written to a new file. Use the appropriate tape drive hardware reference manual to decode the log sense data.

The volume logging data is stored in the /var/log directory by default. You can specify another directory in the /etc/lin_taped.conf file.

There are two configuration parameters in the /etc/lin_taped.conf file that you can tailor to affect the number of log sense files that are kept on your system.

- lin_tapeMaxLogSenseFiles, which has a value of 0 or a positive decimal number.

- lin_tapeAutoLogSenseFileOverWrite, which has a value of 0 or 1.

By default, lin_tapeMaxLogSenseFiles is 0 and lin_tapeAutoLogSenseFileOverWrite is 1, which means that every time log sense data is created, it is written to a new file.

If lin_tapeMaxLogSenseFiles is 0, lin_tapeAutoLogSenseFileOverWrite is ignored, and each time log sense data is obtained, it is written to a new file.

If lin_tapeMaxLogSenseFiles is a positive number and lin_tapeAutoLogSenseFileOverWrite is 0, each time log sense data is created, lin_taped writes that data to a file until lin_tapeMaxLogSenseFiles is created. Then, lin_taped stops creating new files, even if new log sense data is produced.

If lin_tapeMaxLogSenseFiles is a positive number and lin_tapeAutoLogSenseFileOverWrite is 1, each time log sense data is created, lin_taped writes that data to a file until lin_tapeMaxLogSenseFiles is created. Then, when new log sense data is detected, lin_taped deletes the oldest log sense file and creates a new file with the newest log sense data. Thus, only the newest data is kept.

# Automatically retrieving a drive dump:

If a condition occurs in the drive such that a drive dump is created, lin_taped retrieves the drive dump and saves it in a file named lin_tapex.timestamp.dmp, wherelin_tapen is the device special file (for example, lin_tape1, lin_tape2) and timestamp reflects the time that the file was created. Each time a drive dump is obtained, it is written to a new file. The IBM service organization might request that you forward drive dumps to them for analysis.

The drive dumps are stored in the /var/log directory by default. You can specify another directory in the / etc/lin_taped.conf file.

There are two configuration parameters in the /etc/lin_taped.conf file that you can tailor to affect the number of drive dumps that are kept on your system.

- lin_tapeMaxDumpFiles, which can have a value of 0 or a positive decimal number.- lin_tapeAutoDriveDumpFileOverWrite, which can have a value of 0 or 1.

By default, lin_tapeMaxDumpFiles is 0 and lin_tapeAutoDriveDumpFileOverWrite is 1, which means that every time a drive dump is obtained, it is written to a new file.

If lin_tapeMaxDumpFiles is 0, lin_tapeAutoDriveDumpFileOverWrite is ignored, and each time a drive dump is obtained, it is written to a new file.

If lin_tapeMaxDumpFiles is a positive number and lin_tapeAutoDriveDumpFileOverWrite is 0, each time a dump is obtained, lin_taped writes that data to a file until lin_tapeMaxDumpFiles is created. Then, lin_taped stops creating new files, even if new drive dumps are produced.

If lin_tapeMaxDumpFiles is a positive number and lin_tapeAutoDriveDumpFileOverWrite is 1, each time a dump is obtained, lin_taped writes that data to a file until lin_tapeMaxDumpFiles is created. Then, when a new drive dump is detected, lin_taped deletes the oldest drive dump file and creates a new file with the newest drive dump data. Thus, only the newest data is kept.

# Automatically retrieving SIM/MIM data:

If a condition occurs in the drive such that a drive SIM/MIM data is created, lin_taped retrieves the data and save it in a file named lin_tapex.timestamp.simMim, where lin_tapen is the device special file (for example, lin_tape1, lin_tape2) and timestamp reflects the time that the file was created. Each time SIM/MIM data is obtained, it is written to a new file. The IBM service organization might request that you forward SIM/MIM data to them for analysis.

The SIM/MIM data is stored in the /var/log directory by default. You can specify another directory in the / etc/lin_taped.conf file.

There are two configuration parameters in the /etc/lin_taped.conf file that you can tailor to affect the number of SIM/MIM files that are kept on your system.

- lin_tapeMaxSimMimDataFiles, which can have a value of 0 or a positive decimal number.- lin_tapeAutoSimMimDataOverWrite, which can have a value of 0 or 1.

By default, lin_tapeMaxSimMimDataFiles is 0 and lin_tapeAutoSimMimDataOverWrite is 1, which means that every time SIM/MIM data is obtained, it is written to a new file.

If lin_tapeMaxSimMimDataFiles is 0, lin_tapeAutoSimMimDataOverWrite is ignored, and each time SIM/MIM data is obtained, it is written to a new file.

If lin_tapeMaxSimMimDataFiles is a positive number and lin_tapeAutoSimMimDataOverWrite is 0, each time SIM/MIM data is obtained, lin_taped writes that data to a file until lin_tapeMaxSimMimDataFiles is created. Then, lin_taped stops creating new files, even if new SIM/MIM data is created.

If lin_tapeMaxSimMimDataFiles is a positive number and lin_tapeAutoSimMimDataOverWrite is 1, each time SIM/MIM data is obtained, lin_taped writes that data to a file until lin_tapeMaxSimMimDataFiles is created. Then, when new SIM/MIM data is detected, lin_taped deletes the oldest SIM/MIM file and creates a new file with the newest SIM/MIM data. Thus, only the newest data is kept.

# Selective tracing:

Lin_tape provides facilities by which you can disable and enable tracing, error logging, auto- retrieving drive dumps, and auto- retrieving SIM/MIM data. You can selectively enable or disable them through an application program, which uses the STIOC_SETP ioctl. These settings persist until the device driver is restarted, or the host system is rebooted.

The parameters and their definitions are as follows -

# trace

This parameter is set to On by default, which enables lin_tape tracing of activities and error logging on a particular tape drive. Set this parameter to off to stop tracing and error logging.

# logging

This parameter is set to On by default and enables logging of log sense data. Setting this flag to Off suppresses volume logging for this device.

# disable_sim_logging

This parameter controls the logging of SIM/MIM data for a device. By default it is set to Off, which causes SIM/MIM data to be logged. Set this flag to On to suppress the logging of SIM/MIM records.

# disable_auto_drive_dump

This parameter controls the saving of drive dumps for a device. By default it is set to Off, which causes drive dumps to be saved. Set this flag to On to suppress the saving of drive dumps.

# Running lin_taped:

If you are running the lin_tape device driver, version 1.4.1 or higher, after installing lin_tape lin_taped starts running even if your system does not have a tape device attached. If you add a new tape device into your Linux system, lin_taped automatically creates a special file under the /dev directory. If you are running the lin_tape device driver, version 1.3. x or less, lin_taped does not automatically start if there is no tape device attached. After you attach a new tape device, you must start the lin_taped daemon.

You can start lin_taped from the command line. lin_taped takes zero or more of the parameters as listed in the following command.

lin_taped [start stop restart status]

# lin_taped or lin_taped start

Starts the daemon. If there is already a lin_taped running, the new one is aborted. (Use lin_taped restart if lin_taped is already running.)

# lin_taped stop

Terminates the daemon and frees all the resources that are associated with the daemon. When the daemon is stopped, no information is saved.

# lin_taped restart

Terminates the currently running daemon and starts a new one. The new daemon reads the /etc/lin_taped.conf file. This command is used after the /etc/lin_taped.conf file is modified while lin_taped is running.

# lin_taped status

Prints a message on stdout to indicate whether the daemon is running or not.

Note: If you run rmmod lin_tape command to remove the lin_tape device driver from the running kernel, you must stop the lin_taped daemon first; otherwise you get a Device or Resource Busy error.

# Reservation conflict logging

When the device driver receives a reservation conflict on a tape drive command, it logs the conflict to the kernel debug buffer (which is typically echoed to /var/log/messages). Before the error is logged, the device driver determines whether a SCSI Persistent Reservation is active on the target tape drive. If it is, it gets the reserving host initiator WwPN (worldwide port name). If successful, the device driver posts the message

lin_tape: reserve held by xxxxxxxx

to the debug buffer. To prevent multiple identical entries in the error log, subsequent reservation conflicts from the same reserving host WwPN are not logged.

# Devices not reported at /proc/scsi/IBMchanger or /proc/scsi/IBMtape

After lin_tape is installed, following the Installation and Configuration instructions, this command shows all tape devices and their device index:

cat /proc/scsi/IBM★

If any or some devices that are expected to show up are not there, confirm that they are attached to Linux SCSI layer by:

cat /proc/scsi/scsi

If the devices do not show up there, review your HBA configuration and device attachment. If they are there, make sure that they are supported devices at SSIC. See Hardware requirements.

If files /proc/scsi/IBMchanger and /proc/scsi/IBMtape do not exist, review Installation and Configuration instructions, and confirm lin_taped is running by using:

lin_taped status and also that lin_tape is correctly installed on SLES and RHEL by using:

rpm - qa lin_tape

On Ubuntu:

apt list - - installed | grep lin- tape

# Windows Tape and Medium Changer device driver

This chapter describes the hardware requirements, software requirements, and installation notes for the Microsoft Windows device drivers for IBM tape devices.

# Purpose

The Windows tape and medium changer device driver is designed to take advantage of the features that are provided by the IBM tape drives and medium changer devices. The goal is to give applications access to the functions required for basic tape operations (such as backup and restore) and medium changer operations (such as mount and unmount the cartridges), and to the advanced functions needed by full tape management systems. Whenever possible, the driver is designed to take advantage of the device features transparent to the application

# Data flow

Data flowThe software that is described here covers the Windows device driver and the interface between the application and the tape device.

![](images/e7986e26dfd0fde69a7836a2359ac81cea4c4aec81990c1b30aba873070b1f53.jpg)  
Figure 10 on page 65 illustrates a typical data flow process.

# Product requirements

# Hardware requirements

Refer to the "Hardware requirements" on page 1 for the latest hardware that is supported by the IBM tape device driver.

# Software requirements

For current software requirements, refer to the "Software requirements" on page 2.

Note: Limited support for customers who have Microsoft Windows Server 2016 extended support from Microsoft only.

# Installation and configuration instructions

This section includes instructions for installing and configuring the Windows tape and medium changer device driver on Windows Server 2019, 2022 and 2025.

The recommended procedure for installing a new version of the device driver is to uninstall the previous version (see "Uninstalling the device drivers" on page 68).

# Windows Server 2019, 2022 and 2025 instructions

This section describes how to install, remove, and uninstall the Windows tape and medium changer device drivers on Windows Server 2019, 2022 and 2025.

# Installation overview

The installation process consists of the following steps:

1. Verify that the hardware and software requirements are met.  
2. Install the host bus adapters and drivers.  
3. Shut down the system.  
4. Connect the tape and medium changer devices to the host bus adapters.  
5. Power on the tape and medium changer devices.  
6. Restart the system.  
7. Log on as Administrator.  
8. Install and configure the devices and device drivers with the installation application.

# Installation procedures

These procedures make the following assumptions:

: No other driver is installed that claims the tape and medium changer devices. : If you are updating the device driver from a Microsoft certified version to an uncertified version, you must first uninstall the certified driver. Refer to "Uninstalling the device drivers" on page 68. : The host bus adapter is installed, configured properly, and is running supported microcode and driver levels. : Device drivers are installed with Windows Server 2019, with V6.2.6.8 or later until V7.0.1.4; Windows Server 2022, with V7.0.1.1 or later; Windows Server 2025, with V7.0.1.5 or later.

Note: The latest driver level to include support for Windows Server 2016 is V7.0.0.8.

Note: The latest driver level to include support for Windows Server 2019 is V7.0.1.4.

Different registry keys are created to configure different parameters. They can be at System \CurrentControlSet\Services\ and the subkeys are created depending on the Windows Server version. With Windows 2016, they are:

- ibmcg2k16- ibmtp2k16- ibmtpbs2k16

Starting with Windows 2019 and later, subkeys are the same, they are:

- ibmcgbs- ibmtpbs

Starting with V7.0.1.5, subkeys are stored under System\CurrentControlSet\Services\ibmXXXX \Parameters.

Refer to this list when in doubt of the registry key's name and instructions that involve modifying the registry. Caution and a backup are advised due to the registry's nature.

Drivers are identified by the following conventions, where nnnn refers to a version of the driver. If there is more than one version, use the current one.

- Windows Server 2019 for extended 64-bit architectures (Intel EM64T and AMD64),

IBMTape.x64_w19_nnnn_WHQL_Cert.zip

Starting with V7.0.1.1 all the supported OS versions for each driver version share the same files.

IBMTape.nnnn- x64_WHQL_Cert.zip

To install the device drivers, follow this procedure.

1. Log on as Administrator.

2. Download the appropriate driver. Refer to"Accessing documentation and software online" on page 75.

3. Extract the driver package to a hard disk drive directory of your choice, other than the root directory.

4. Ensure that the tape and medium changer devices are connected to your host bus adapter and configured properly by locating the devices in Device Manager.

5. Double-click either install_exclusive.exe or install_nonexclusive.exe.

- install_exclusive.exe The driver issues automatic reserves on open. It also prevents multiple open handles from the host to a drive from existing at the same time, as is required by applications such

as Tivoli Storage Manager. This driver is also required for the failover feature to work as it uses persistent reservation (by default).

- install_nonexclusive.exe The driver permits open handles from the host to a drive to exist at the same time, as is required by applications such as Microsoft Data Protection Manager (DPM).

![](images/c0d7dfd28ba54791bb11565832c92960865e67c5bb989d2b2cf7db367566de3b.jpg)  
Figure 11. Installation application in Windows Explorer

# Note:

a. More installation features are available through the command prompt interface (CLI), which include

- Installing only the tape or medium changer device drivers (-t or 
-c)- Running in debug mode, which creates the file debug.txt in the driver package directory (-d)- Running in silent mode, which suppresses messages that require user intervention, but only with Microsoft-certified IBM drivers (-s)- Disabling DPF from installation (-f)- Enabling Persistent Reserve from installation if DPF is disabled (-p)- Disable Media Polling (-m)- Disable Dynamic Runtime Attributes (-a)- Exclude devices from being claimed by the driver (-e)- Install checked version of tape and changer device drivers (-x), available on V7.0.1.5 and later versions

To install the device drivers with any of these features, instead of double- clicking the installation executable file, open a command prompt window and cd to the driver package directory. For the usage information, type install_exclusive.exe - h or install_nonexclusive.exe - h at the prompt. b. If the Windows Found New Hardware Wizard begins during installation, cancel the wizard. The installation application completes the necessary steps.

6. To verify that the tape and medium changer devices and drivers are installed correctly, follow the instructions in "Verifying correct attachment of your devices" on page 191.

# Device removal or disable procedure

If you must remove a device, or if you are altering the hardware configuration, you must uninstall or disable the device first.

1. Right-click My Computer, select Manage to open the Computer Management Console, and click Device Manager.2. Right-click the device that you want to uninstall and select Uninstall ... If you want to disable the device without uninstalling it, you can select Disable.3. You are prompted to confirm the uninstallation. Click OK.4. In Device Manager, under System devices, right-click Changer Bus Enumerator and select Uninstall.5. In Device Manager, under System devices, right-click Tape Bus Enumerator and select Uninstall.

Note: This removal procedure removes the device from the device tree, but it does not uninstall the device driver files from your hard disk.

# Uninstalling the device drivers

To uninstall the device drivers from the system, which includes deleting the system files and deallocating other system resources, complete the following steps.

1. Quiesce all activity on the tape and medium changer. 
2. Double-click uninst.exe in the driver package.

Note: This action removes all the files in the system directories that were created during the installation of the device driver. It does not delete the compressed file or the files that were extracted from the compressed file. If you want to remove these files, you must delete them manually. For v.6.2.5.3 and later, it is not required to manually remove the devices at the Device Manager.

3. Restart the system.

# Configuring limitations

Configuring limitationsThe driver limitation for the supported number of tape devices is 1024. Every installed device uses a certain amount of resources. The user must also consider other resources, such as physical memory and virtual space on the system before you attempt to reach the limits. Also, be aware of Microsoft limitations. One known article is http://support.microsoft.com/kb/310072 (ID: 310072). Be aware of any Windows specific version limitations.

# Persistent Naming Support on Windows Server 2019, 2022 and 2025.

The Windows tape driver has an option for enabling device object names that persist across restarts of the operating system. For example, if your tape drive has the name \tape4801101 and the persistent naming option is used, then \tape4801101 is reserved for use by that device after an operating system restart.

Complete the following steps to enable this feature.

1. Add a DwORD value to the registry called PersistentNaming and assign it a value 1

On Windows Server 2016: HKEY_LOCAL_MACHINE\System\CurrentControlSet\Services \ibmtp2k16 On Windows Server 2019 and Windows Server 2022 (For IBM Tape 7.0.1.4 and before): HKEY_LOCAL_MACHINE\System\CurrentControlSet\Services\ibmtp On any supported Windows version starting on IBM Tape 7.0.1.5 : HKEY_LOCAL_MACHINE\System \CurrentControlSet\Services\ibmtp\Parameters

2. Restart your system. Then, the system writes information to the registry to associate the Worldwide Node Name from Inquiry p.  $0\times 83$  with the persistent name used by the operating system.

If the Worldwide Node Name is unavailable, or the drive is a virtual (that is, emulated) drive, then the device serial number is used rather than the Worldwide Node Name. If the PersistentNaming option is not specified in the registry, then your devices might not be able to claim the same device name after restart or driver initialization.

You can find registry subkeys with persistent naming information.

On Windows Server 2016: HKEY_LOCAL_MACHINE\System\CurrentControlSet\Services \ibmtpbs2k16 On Windows Server 2019 and Windows Server 2022(For IBM Tape 7.0.1.4 and before): HKEY_LOCAL_MACHINE\System\CurrentControlSet\Services\ibmtpbs On any supported Windows version starting on IBM Tape 7.0.1.5 :HKEY_LOCAL_MACHINE\System \CurrentControlSet\Services\ibmtpbs\Parameters

Alternately, you can use the Windows Device Manager to examine the device number to determine that persistent naming is enabled on your host. Persistent names contain tape device numbers that are based at 4801101 (which is the decimal equivalent of hexadecimal 0x49424D and ASCII "IBM").

If two physical paths exist to a drive and different Windows device names are required (which happens, for example, when two different HBAs are connected to the drive and Data Path failover is disabled), the first discovered path claims the persistent device name. Any subsequent paths that connect to the same device receive names according to the order in which they are discovered by the Windows Device Manager.

Note: Persistent naming is not set by default. For disabling it, set the PersistentNaming value to 0 and restart the system.

# Control Path failover support for tape libraries

To take advantage of Windows Control Path failover (CPF) support, the appropriate feature code must be installed. Refer to "Automatic failover" on page 4 for what feature code might be required for your machine type.

# Configuring and unconfiguring Control Path failover support

Control Path failover is enabled automatically when the device driver is installed by default (install_exclusive.exe). It can be disabled from installation with the - f CLI option. Or, it can be disabled or reenabled for the entire set of attached medium changers by modifying the registry.

1. Open the reg folder of the driver package. 
2. Double-click DisableCPF.reg or EnableCPF.reg. 
3. Reboot the system. This action is necessary for any registry modification to take effect.

# Querying primary and alternative path configuration

To check whether the control path failover is enabled in the device driver and show the primary and alternative paths, use the tape diagnostic and utility tool.

# Checking disablement of Control Path failover setting

If you disabled the control path failover in the device driver's setting by double- clicking the DisableCPF.reg file and rebooting your system, you can go into the registry by issuing the Windows regedit command to confirm that CPF is disabled. Look for FailoverDisabled exists and is set to 1

On Windows Server 2016: HKEY_LOCAL_MACHINE\SYSTEM\CurrentControlSet\Services \ibmcg2k16 On Windows Server 2019 and Windows Server 2022 (For V7.0.1.4 and before): HKEY_LOCAL_MACHINE \SYSTEM\CurrentControlSet\Services\ibmcg On any supported Windows version starting on IBM Tape 7.0.1.5 : HKEY_LOCAL_MACHINE\SYSTEM \CurrentControlSet\Services\ibmcg\Parameters

This line indicates that CPF is disabled in the driver. This setting takes effect only after your system is rebooted.

# Data Path failover support for tape drives

To take advantage of Windows Data Path failover (DPF) support, the appropriate feature code must be installed. Refer to "Automatic failover" on page 4 for what feature code might be required for your machine type.

# Configuring and unconfiguring Data Path failover support

Data Path failover is enabled automatically when the device driver is installed by default (instal1_exclusive.exe). It can be disabled from installation with the - f CLI option. Or, it can be disabled or reenabled for the entire set of attached drives or medium changers by modifying the registry.

1. Open the reg folder of the driver package. 
2. Double-click DisableDPF.reg or EnableDPF.reg. 
3. Reboot the system. This action is necessary for any registry modification to take effect.

For latest LTO generation 3 code and later LTO generation tape drives, a license key feature for the library hardware is required.

Note: For LTO generation 3 or lower, or for tape drives that require a data path license key on the host side to enable DPF, the device driver looks for a file that is called %system_root%:\IBM_DPF.txt for the key. %system_root% is the drive letter where Windows is installed, typically C (for example, C:\IBM_DPF.txt). The file contains the key on a single line, with no spaces and no other text on the line. If multiple keys are required, place each key in the file on its own line. The driver looks for this file at initialization. If the file contains a valid DPF license key, the DPF feature is enabled and any eligible devices have multi- path support.

# Reserve Type if DPF is disabled

If DPF is disabled, SCSI- 2 reserve is used by default to handle the reservation on tape drives. If Persistent Reserve is wanted rather than SCSI- 2 reserve, ReserveTypePersistent.reg enables it (ReserveTypeRegular.reg disables it and then SCSI- 2 reserve is used). Or, - p CLI option installation (only if - f was used to disable DPF) enables Persistent Reserve from installation.

Note: If DPF is not disabled, Persistent Reserve is used.

# Querying primary and alternative path configuration

To check whether the data path failover is enabled in the device driver and show the primary and alternative paths, you can use the tape diagnostic and utility tool.

# Checking disablement of Data Path failover setting

If you disabled the data path failover in device driver's setting by double- clicking the DisableDPF.reg file and rebooting your system, you can go into the registry by issuing the Windows regedit command to confirm that DPF is disabled. Look for FailoverDisabled exists and is set to 1

- On Windows Server 2016: HKEY_LOCAL_MACHINE\SYSTEM\CurrentControlSet\Services \ibmtp2k16- On Windows Server 2019 and On Windows Server 2022 (For V7.0.1.4 and before): HKEY_LOCAL_MACHINE\SYSTEM\CurrentControlSet\Services\ibmtp- On any supported Windows version starting on IBMTape 7.0.1.5: HKEY_LOCAL_MACHINE\SYSTEM \CurrentControlSet\Services\ibmtp\Parameters

This line indicates that DPF is disabled in the driver. This setting takes effect only after your system is rebooted.

If you enabled Persistent Reserve on DPF disabled, look for ReserveType exists and is set to 1- On Windows Server 2016: HKEY_LOCAL_MACHINE\SYSTEM\CurrentControlSet\Services \ibmtp2k16- On Windows Server 2019 and On Windows Server 2022 (For V7.0.1.4 and before): HKEY_LOCAL_MACHINE\SYSTEM\CurrentControlSet\Services\ibmtp- On any supported Windows version starting on IBMTape 7.0.1.5:HKEY_LOCAL_MACHINE\SYSTEM \CurrentControlSet\Services\ibmtp\Parameters

70 IBM Tape Device Drivers Installation and User's Guide

If you use ReserveTypeRegular.reg, then the ReserveType value is deleted.

# Problem determination

There is a debug version of the device driver that can be used if you encounter problems. The debug version of the driver issues DbgPrint messages at various places during device driver execution. To capture these messages, you must start a debugger or use a tool like Debug View from Sysinternals Suite, available from http://technet.microsoft.com/sysinternals/.

# Installing the debug version

# Prior V7.0.1.5 instructions

1. Quiesce all activity on the tape and medium changer. 
2. Exit all applications that are using the tape and medium changer devices. 
3. If the driver is installed, complete the Uninstall the devices procedure that is described in a previous section. 
4. Locate the Vchecked folder for the device driver level that you are running. This folder is in the highest level directory of the driver package. It contains checked versions of the tape and medium changer device drivers.

a. on Windows Server 2016. ibmtpxxyyy.sys, and ibmcgxxyyy.sys, where:

i)  $xx = \mathbf{f}$  for the filter driver, bs for the bus driver, or blank for the base driver ii)  $yyyy = 2k16$  . It also has the .inf and .cat files. 
b. on Windows Server 2019 and on Windows Server 2022. ibmtpxx.sys, and ibmcgxx.sys, where: i)  $xx = \mathbf{f}$  for the filter driver, bs for the bus driver, or blank for the base driver. It also has the .inf and .cat files.

5. Copy all the files from the \checked folder to the highest level directory of the driver package, overwriting the files in there. A previous backup of the files or having a copy of the original driver package is recommended.

6. Start Dbgview.exe to capture debug messages. Dbgview can be downloaded from the Microsoft website. Make sure that Capture Kernel, Enable Verbose Kernel Output, and Capture Events are checked at the Capture menu.

7. Complete the Installation procedure.

8. Issue commands to the driver. You see debug messages on Dbgview from IBMTpBus, IBMCgBus, tape, or mcd. For example, you might see IBMTpBus: ENT: tag output. If you do not see something similar, an error might have happened during the checked driver installation or no driver activity might have occurred.

# For V7.0.1.5 and later instructions

1. Quiesce all activity on the tape and medium changer. 
2. Exit all applications that are using the tape and medium changer devices. 
3. If the driver is installed, complete the Uninstall the devices procedure that is described in a previous section. 
4. Open a terminal and navigate to IBMTape.nnnn-x64_WHQL_Cert folder path, where the install-exclusive.exe is located. 
5. Execute the following command .\install-exclusive.exe -x; where the -x flag means the debug version of the drivers are going to be installed. 
6. Wait until the installation finishes.

7. Start Dbgview.exe to capture debug messages. Dbgview can be downloaded from the Microsoft website. Make sure that Capture Kernel, Enable Verbose Kernel Output, and Capture Events are checked at the Capture menu.

8. Complete the Installation procedure.

9. Issue commands to the driver. You see debug messages on Dbgview from IBMTpBus, IBMCgBus, tape, or mcd. For example, you might see IBMTpBus: ENT: tag output. If you do not see something similar, an error might have happened during the checked driver installation or no driver activity might have occurred.

# Restoring the non-Debug version

To restore the non- debug version of the driver, complete the following steps.

1. Quiesce all activity on the tape and medium changer devices.  
2. Exit all applications that are using the tape and medium changer devices.  
3. Uninstall the debug driver version, which can be accomplished by running uninstall.exe, within the driver's package folder.

Note: For v.6.2.5.3 and later, there is no need to manually remove the devices at Device Manager.

4. Restart the system.  
5. When the system is back, install the non-debug driver version.

# Reservation conflict logging

When the device driver receives a reservation conflict during open or after the device is opened, it logs a reservation conflict in the Windows eventlog. Before the error is logged, the device driver issues a Persistent Reserve In command. This action determines whether a SCSI Persistent Reservation is active on the reserving host to get the reserving host initiator WWPN (worldwide port name). If successful, the device driver logs this information as follows.

Reserving host key: kkkkkkkk WwPN: xxxxxxxx

Where kkkkkkkk is the actual reserve key and xxxxxxxx is the reserving host initiator WwPN.

After initially logging the reserving host WwPN, subsequent reservation conflicts from the same reserving host WwPN are not logged. This action prevents multiple entries in the error log until the reserving host WwPN is different from the one initially logged. Or, the device driver reserved the device and then another reservation conflict occurs.

# Max retry busy

Complete the following steps to enable this feature.

1. Add a DWORD value to the registry called MAXBusyRetry and assign it a value between 1 and 480 at:

- On Windows Server 2016: HKEY_LOCAL_MACHINE\SYSTEM\CurrentControlSet\Services\ibmtp2k16  
- On Windows Server 2019 and Windows Server 2022 (V7.0.1.4 and before): HKEY_LOCAL_MACHINE\SYSTEM\CurrentControlSet\Services\ibmtp  
- On any supported Windows version starting on V7.0.1.5: HKEY_LOCAL_MACHINE\SYSTEM\CurrentControlSet\Services\ibmtp\Parameters

2. Reboot your system. Then, when a device gets the device busy error, the command is retried up to MAXBusyRetry times.

# Checking if Data Path failover is enabled

Checking if Data Path failover is enabledIf the tape diagnostic utility is not available, you can confirm that Data Path failover is enabled by installing the Debug version of the device driver. Refer to "Problem determination" on page 71. While the Debug device driver is installing, look for the message: tape.CreateTapeDeviceObject: DPF INSTALLED.

# Checking if Control Path failover is enabled

Checking if Control Path failover is enabledIf the tape diagnostic utility is not available, you can confirm that Control Path failover is enabled by installing the Debug version of the device driver. Refer to "Problem determination" on page 71. While the Debug device driver is installing, look for the message: FtIValidateCpfKey and confirm that the status is True.

# IBM Tape Diagnostic Tool (ITDT)

This chapter describes the IBM Tape Diagnostic Tool.

# Purpose

The IBM Tape Diagnostic Tool (ITDT) is available in two versions:

Standard Edition (ITDT- SE)- The command line version. Graphical Edition (ITDT- GE) - The GUI version for the following platforms: - Microsoft Windows operating systems - Linux operating systems

Both versions provide the user with a single diagnostic program for tapeutil applications. Both SE and GE contain tapeutil functions with SE also providing scripting capability.

Note: The term tapeutil is a synonym for the tool that is delivered with the device driver. For example, this tool is named tapeutil on UNIX operating systems; it is named nutil on Microsoft Windows operating systems.

ITDT supports IBM tape and library devices claimed by the IBM Tape Device Driver and devices that use the generic Operating System driver. By default the IBM Tape Device Driver is used but only if the IBM Tape Device Driver is not installed or the device is not claimed by the IBM Tape Device Driver. The generic Operating System driver is used when available.

The available advanced operations that are provided by the IBM Tape Diagnostic Tool are completed on tape drives and tape libraries. By using this function, you can complete maintenance tasks and run diagnostic tasks to determine tape drive issues. This action reduces product downtime and increases productivity.

The IBM Tape Diagnostic Tool is designed to

Run quick or extended diagnostic tests on tape drives. Start tape library self- test operations. Retrieve dumps from tape drives and libraries. Run a firmware update on tape drives or libraries. Online Check for Firmware updates for tape drives or libraries (ITDT- GE). Test the performance of the environment by completely writing a cartridge and measuring performance. Verify tape drive compression. Measure system performance. Retrieve and display cartridge usage information.

- Verify the encryption environment. This test is used to verify whether data on the cartridge was written encrypted.- Scan the system to discover all supported tape and library devices.- Run a connection test (scan). This test is used to verify that all devices are attached properly.- Run a standard test to check whether the tape device is defective and output a pass/fail result.Note: When this test is completed, all data on the cartridge is overwritten.- Run a full write function. This function writes the entire cartridge, overwriting all previous data with a selectable block size that contains either compressible or incompressible data and then outputs performance data.Note: When this test is completed, all data on the cartridge is overwritten.- Run a system test. Write different block sizes with compressible and incompressible data and then outputs performance data.Note: When this test is completed, all data on the cartridge is overwritten.- Run a tape usage function to retrieve statistical data and error counters.- Run HD-P functions like discovery.- Physical Copy/Data migration and verification of cartridges.- Log and Dump File analysis.ITDT-SE provides the most important functions of the previous tapeutil tools. As an extension of the current tapeutil variants, the set of operations and functions available with ITDT-SE is identical across all supported operating systems (unless a function is not available on a particular system).Dedicated device drivers for tapes and libraries can be installed on the target system and an application is installed that uses the tape/library devices. When this configuration exists, ITDT-SE can coexist with the application so that when the application disables the device internally, ITDT-SE can run the diagnostic tests on that device.

ITDT- SE provides the most important functions of the previous tapeutil tools. As an extension of the current tapeutil variants, the set of operations and functions available with ITDT- SE is identical across all supported operating systems (unless a function is not available on a particular system).

Dedicated device drivers for tapes and libraries can be installed on the target system and an application is installed that uses the tape/library devices. When this configuration exists, ITDT- SE can coexist with the application so that when the application disables the device internally, ITDT- SE can run the diagnostic tests on that device.

# Accessing ITDT

IBM maintains the latest levels of the ITDT tool chain and documentation on the Internet at http://www.ibm.com/support/fixcentral.

One option to access ITDT is through "Accessing documentation and software online" on page 75. When an IBM driver is downloaded, there is a corequisite to download ITDT.

This portal gives access to the download area where the following procedure guides you to the correct download:

1. In the Product Group menu, select Storage Systems.2. In the Product Family menu, select Tape Systems.3. In the Product Type menu, select Tape device drivers and software.4. In the Product menu, select Tape Diagnostic Tool (ITDT).5. Select your platform and press Continue.

# Accessing documentation and software online

IBM maintains the latest levels of System Storage tape drive and library device drivers and documentation on the Internet. Fix Central is a portal where you can download the latest version of drivers for most of the IBM tape products. In this download area, you find menus that can guide you to find what you need.

To access to tape Device Drivers or software downloads, click http://www- 933. ibm.com/ support/fixcentral/?productGroup0  $\equiv$  System%20Storage&productGroup1  $\equiv$  ibm/ Storage_Tape&productGroup2  $\equiv$  Tape%20drivers%20and%20software&productGroup3  $\equiv$  ibm/ Storage_Tape/Tape%20device%20drivers. Choose your platform, then click Continue. Alternately, you can follow these steps.

1. Access the Fix Central URL at http://www.ibm.com/support/fixcentral. 
2. In the Product Group menu, select System Storage. 
3. From the System Storage menu, select Tape systems. 
4. In the Tape systems menu, select Tape drivers and software. 
5. In the Select from Tape drivers and software menu, select Tape device drivers. You are also able to get tools such as ITDT. 
6. The Platform menu displays. Select the platform that you are looking for, then click Continue to view what is available for the selected platform. 
7. In the next screen, there is a list of the latest Tape device drivers or Tape Diagnostic tools versions. Select the package that you need, then click Continue.

# Note:

- The latest driver has the highest number extension. Numeric sequence numbers are in each level of the device and library driver. So, for example, for AIX the number is Atape.1.1.7.5.0.bin. As newer levels of a driver are released, a higher numeric sequence is assigned. When a driver is downloaded, ITDT also appears as a recommended download if you selected the option of corequisites and prerequisite. As of January 31, 2012, each IBM client that accesses Fix Central (whether through their employees or other authorized representatives) is required to have an individual IBM ID to download fixes (some exemptions might apply). The registration is quick and simple and provides users with a customized experience to better serve their needs.

8. After you click Continue, you can choose the way that you want to download the fix or fixes. You can either use your browser (HTTP), the Download Director (http://www6.software.ibm.com/dldirector/ doc/DDfaq_en.html#Q_A1), or bulk FTP option. The bulk FTP option is a web service where you can download a package with the FTP command or other download commands like WGET.

# Note:

- To use the Download Director, ensure that the Java™ Runtime Environment is installed on your computer. For information, see http://www6.software.ibm.com/dldirector/doc/DDfaq_en.html. Leave the check box for prerequisites and co-requisites selected. Click Continue.- When you select the download option before Continue is pressed, you can change the way that you want to download the fix or fixes from the Change download options link at the top of the page.

9. Sign in, then click Submit.

10. Click I agree on the Terms and Conditions screen to continue.

11. A list of files and downloadable files displays, including links to Documentation, Programming guides, User guides, and readme files.

Note: Some plain text files might require you to right- click, then select Save Link As... to download the file.

Table 11 on page 76 documents each driver by name and description.

<table><tr><td colspan="2">Table 11. Driver descriptions</td></tr><tr><td>Driver</td><td>Description</td></tr><tr><td>Atape.n.n.n.n.bin</td><td>AIX Device Driver (Atape)</td></tr><tr><td>/lin_tape_source-lin_taped/lin_tape-x.x.x.x-x.src.rpm.bin</td><td>Linux Device Driver (lin_tape) source code</td></tr><tr><td>/lin_tape_source-lin_taped/lin_taped-x.x.x-dist.arch.rpm.bin</td><td>Linux lin_taped daemon program</td></tr><tr><td>/IBMtape.n.n.n.n.bin</td><td>Solaris Device Driver (IBMtape)</td></tr><tr><td>/IBM Tape.arch_wXX_nnnn.zip</td><td>Windows Server 20XX Driver on arch (x86, x64) where XX denotes the version (such as, 2008, 2012, 2019)</td></tr></table>

# Note:

1. Valid for Windows 2008 and Windows 2008 r2

2. dist indicates a Linux distribution. arch indicates a machine architecture (for example, i386, ia64, s390).

3. The n.n.n. or n.n.n strings are replaced with digits to reflect the version of each driver.

- For details on supported tape attachment, refer to the System Storage Interoperation Center website 
- http://www.ibm.com/systems/support/storage/config/ssic/.

- Information concerning supported Fibre Channel host bus adapters (HBAs) and associated HBA device drivers, firmware, and BIOS levels can be obtained from the System Storage Interoperation Center website 
- http://www.ibm.com/systems/support/storage/config/ssic/.

- The IBM_Tape_Driver_IUG.pdf file contains the current version of the IBM Tape Device Drivers: Installation and User's Guide, which can be found here: http://www-01.ibm.com/support/docview.wss?rs=577&iid=ssgIS7002972. You can also use the links that are located under Related Information to get quick access to the download section for the tape device drivers.

- The IBM_Tape_Driver_PROGREF.pdf file contains the current version of the IBM Tape Device Drivers: Programming Reference, which can be found here: http://www-01.ibm.com/support/docview.wss?uid=ssg1S7003032.

- For the current information for the device driver you are using, consult the readme file (not files) included in the download of your device driver.

Use the links listed below for quick access to the download section for every tape Device Driver platform.

- AIX. Path: Storage Systems > Tape Systems > Tape device drivers and software > Tape device drivers > AIX, or http://www-933.ibm.com/support/fixcentral/swg/selectFixes?parent=ibm-ST~Tapedevicedriversandsoftware&product=ibm/Storage_Tape/Tape+device+drivers&release=1.0&platform=AIX&function=all.

- Linux. Path: Storage Systems > Tape Systems > Tape device drivers and software > Tape device drivers > Linux, or http://www-933.ibm.com/support/fixcentral/swg/selectFixes?parent=ibm-ST~Tapedevicedriversandsoftware&product=ibm/Storage_Tape/Tape+device+drivers&release=1.0&platform=Linux&function=all.

- Solaris. Path: Storage Systems > Tape Systems > Tape device drivers and software > Tape device drivers > Solaris, or http://www-933.ibm.com/support/fixcentral/swg/selectFixes?

parent=ibm\~ST\~Tapedevicedriversandsoftware&product=ibm/Storage_Tape/Tape+device +drivers&release=1.0&platform  $\equiv$  Solaris&function  $\equiv$  all.

Windows.Path:Storage Systems  $\gimel$  Tape Systems  $\gimel$  Tape device drivers and software  $\gimel$  Tape device drivers  $\gimel$  Windows, or http://www- 933. ibm.com/support/fixcentral/swg/selectFixes? parent=ibm\~ST\~Tapedevicedriversandsoftware&product=ibm/Storage_Tape/Tape+device +drivers&release=1.0&platform  $\equiv$  Windows&function  $\equiv$  all.

Windows WHQL.Path:Storage Systems  $\gimel$  Tape Systems  $\gimel$  Tape device drivers and software  $\gimel$  Tape device drivers  $\gimel$  Windows WHQL, or http://www- 933. ibm.com/support/fixcentral/swg/ selectFixes?parent=ibm\~ST\~Tapedevicedriversandsoftware&product=ibm/Storage_Tape/Tape+device +drivers&release=1.0&platform  $\equiv$  Windows+WHQL&function  $\equiv$  all.

# Supported systems

ITDT is developed to support various versions of different platforms and tape products. For the latest support, refer to "Introduction" on page 1 and Inter- operation Center website product support at http://www.ibm.com/systems/support/storage/config/ssic/.

# IBM Tape Diagnostic Tool - Standard Edition

# Installing ITDT - Standard Edition

This section describes the installation procedures for the Standard Edition of ITDT in various operating systems.

Before the IBM Tape Diagnostic Tool Standard Edition (ITDT- SE) is used with the IBM Tape Device Driver, we recommend upgrading to the latest available IBM Tape Device Driver level.

The [U] Tapeutil option works only if the IBM Tape Device Driver is installed. The - - force- generic- dd startup option to bypass the IBM Tape Device Driver has no effect on the [U] Tapeutil option commands. They still use the IBM Tape Device Driver if it is installed.

In a System Managed Encryption setup, the Encryption test [E] always exits with NO DRIVER SPECIAL FILE when ITDT- SE is started with - - force- generic- dd.

# Installing ITDT-SE on AIX operating systems

ITDT- SE on AIX operating systems has two installation modes and can be installed by one of the following methods.

1. Installation in the AIX software repository:

a. Download tape.itdt.<version>.bff  
b. Change to the download folder and run the imutoc command.  
c. To install the file, run the following command.

installp - acd tape.itdt.<version>.bff all Or smitty install

2. Installation independently.

a. Download install_itdt_se_<OS>.<version> to a directory of your choice.  - install_itdt_se_Aix_<version> is for AIX<version> operating systems.  
b. Run the following command to make install_itdt_se_<OS>.<version> executable.

chmod 700 install_itdt_se_Aix_<version>

3. To run without parameters, run the following command.

<table><tr><td>install_itdt_se_Aix_&lt;version&gt;</td></tr><tr><td>Or</td></tr><tr><td>./install_itdt_se_Aix_&lt;version&gt;</td></tr></table>


</version>

depending on your operating system.

Note: ITDT- SE can be used only by a user with root access rights.

# Installing ITDT-SE on Microsoft Windows operating systems

To install ITDT- SE on Microsoft Windows operating systems, complete the following steps.

Note: ITDT- SE can be used only by a user with administrator rights.

1. Download install_itdt_se_WindowsX86_64_<version>.exe to a directory of your choice. 
2. Run the following command:

install_itdt_se_WindowsX86_64_<version>.exe

# Installing ITDT-SE on IBM System i

ITDT's installation package for IBM System i is a single file for a command- line based installation either in the Qshell or BASH environment. For this reason, the file needs to be manually transferred to the system i host.

# Procedure

To install ITDT- SE on IBM System i, complete the following steps.

Remember: ITDT- SE can be used only by a user with QSECOFR access rights.

1. Download install_itdt_se_System_i_version to a directory of your choice. 
2. Transfer the installation file to a folder within the IBM System i host IFS file system, such as /tmp. 
3. Connect to the IBM System i host by using any terminal emulation program. 
4. Run the QSH command and change the directory to the folder where the ITDT installation program is stored. See #unique_150/unique_150_Connect_42_fig_jmv_qvh_ltb on page 78 for reference.

![](images/6862d8f63594162ca7b66aee07f034627cdaf0969b00f2908c9efbf791d1b7bb.jpg)  
Figure 12. QSH command entry

5. Run the following command to make install _itdt_se_System_i_version an executable file: chmod 700 install_itdt_se_<OS>_version

6. Run the installation file.

Tip: You can also use the following parameters as needed:

- --help displays help message.- 
--verbose displays verbose information.- 
--uninstall uninstalls ITDT.- 
--version displays version information.

# Results

After successful installation, the ITDT program is located in the following folder: /home/ITDT.

# Installing ITDT-SE on other supported operating systems

To install ITDT- SE on other supported operating systems, complete the following steps.

Note: ITDT- SE can be used only by a user with root access rights, except for the Mac OS, which requires the user to have the minimum of read/write access to the device file.

1. Download install_itdt_se_<OS>_<version> to a directory of your choice.

- install_itdt_se_Aix_<version> is for AIX operating systems- install_itdt_se_Linuxx86_64_<version> is for Linux operating systems on X86_64 hardware- install_itdt_se_Linuxpowerpc64_<version> is for Linux operating systems on pSeries- install_itdt_se_Linuxpowerpc64le_<version> is for Linux operating systems on pSeries Little Endian.- install_itdt_se_Linuxx390x_<version> is for Linux operating systems on zSeries- install_itdt_se_MacOS_<version> is for Mac OS operating systems.

2. Run the following command to make install _itdt_se_<OS>_<version> executable.

chmod 700 install_itdt_se_<OS>_<version>

3. To run without parameters, run the following command

install_itdt_se_<OS>_<version>

or

./install_itdt_se_<OS>_<version>

depending on your operating system.

Optionally you can use these parameters:

- -h [--help] Show help message- 
-v [--verbose] Print the extracted files- 
-p [--path] Path for extraction (default is current directory)- 
-t [--test] Show package content

# Starting ITDT - Standard Edition

This section describes the startup procedures for the Standard Edition of ITDT in various operating systems.

# Starting ITDT-SE on Solaris operating systems

1. If the IBM Tape Device Driver is not used, if it is not installed, or you want to be able to run - forcegeneric-dd, configure the sgen driver.

A script sgen_solaris_conf.sh is included in the ITDT- SE package. This script allows you to configure the sgen generic SCSI driver that is shipped with Solaris.

ITDT- SE requires that the sgen device driver is configured so that the devices you want to work with can be found.

Note: For system security reasons, always reset the sgen device driver settings after you finish working with ITDT- SE, by using the sgen_solaris_conf.sh script.

To configure the sgen driver, start the sgen_solaris_conf.sh script with root access.

The following command- line options are available on the sGEN driver configuration screen:

1. Check driver: This option checks if the driver /kernel/drv/sgen is available. 
2. List driver settings: This option shows the current activated devices. 
3. New driver configuration: This option shows the screen that is used to create a new driver configuration (see Step 2). 
4. Stop sgen driver: This option stops the driver (that is, rem_drv sgen). 
5. Start sgen driver: This option stops and starts the sgen driver. 
6. Exit program: This option closes the shell script.

2. Enter option 3 to create a new driver configuration.

The following command- line options are available on the New Configuration screen. Use these options to configure the sgen driver:

1. List targets: This option shows the targets in current configuration. 
2. Define device types: This option defines drive and changer types. 
3. or 5. Add targets: This option adds targets to the list.

Note: Option 3 allows for the addition of individual devices one at a time. Option 5 allows for the addition of a range of devices, eliminating the need to add many devices one by one.

4. or 6. Remove targets: This option removes targets from the list.

Note: Option 4 allows for the removal of individual devices one at a time. Option 6 allows for the removal of a range of devices, eliminating the need to remove many devices one by one.

7. Save configuration: This option saves the modifications. 
8. Back to Main Menu: This option returns to the main menu.

3. After the sgen driver is configured, enter command-line option 8 to go back to the main menu.

4. On the SGEN driver configuration screen, enter command-line option 5. This option starts the sgen driver. New devices are found by using the definitions that are completed in Step 2.

5. After the new devices are found, enter option 6 to exit the sgen_solaris_conf.sh script.

Note: For Fibre Channel Host Bus Adapters (HBAs), special handling is required. Attached devices must be configured with their WwPN in the sgen.conf file. This task must be done manually. It is not completed by using the sgen_solaris_conf.sh script.

The following code is an example how to add those devices:

Run the command "cfgadm - al" to get the WwPN number(s). c4:5005076302401924 fC- private connected configured tape connected configured Add the ww- PN number(s) into the sgen.conf file. name="sgen" parent="fp" target=0 lun=0 fc- port- wwn="5005076302401924" name="sgen" parent="fp" target=0 lun=1 fc- port- wwn="5005076302401924"

If you have finished the editing, the sgen driver has to be restarted. Please enter "update_drv sgen".

The Read Attribute command is 16- byte CDB. On Solaris 10, IBMtape can detect what maximum CDB length is supported from HBA attributes and set the supported CDB. For Solaris 9, CDB16 must be enabled for ITDT- SE to work correctly. This procedure can be done by adding the entry of

cdb16_support=1

at the first line in

/usr/kernel/crv/IBMtape.conf

and reload the IBMtape driver again.

To start ITDT- SE, run the following command:

./itdt

At first start, read the User License Agreement.

Press Enter to scroll the license screens forward or b followed by Enter to go back. Type i if you agree to the terms of license or q followed by Enter to quit the application.

During the initial program start, the input and output directories are created.

Input directory: default directory for firmware files during Firmware Update Output directory: directory that contains the result files, dump files, and log files after tests are run ITDT- SE does not make changes outside the installation directory.

# Starting ITDT-SE on Windows operating systems

At first program start, the license text is displayed and the input and output directories are created. Start ITDT by running the following command from the directory "ITDT" that was created during the installation (confirm to run with administrator rights):

itdt.exe

Read the User License Agreement.

Press Enter to scroll the license screens forward or b followed by Enter to go back. Type i, if you agree to the terms of license or q followed by Enter to quit the application.

ITDT- SE does not create any registry entries or make changes outside the installation directory.

During installation a subdirectory "ITDT" was created which contains the ITDT program file and 2 subdirectories initially:

"License" directory with license files in different languages. "Scripts" directory for extra scripts.

When the program is run for the first time, two more subdirectories are created in the "ITDT" folder:

"Input" directory for firmware files to be uploaded to devices. "Output" directory for generated log and dump files.

To remove ITDT from your system, erase the ITDT directory. Any log and dump files in the subdirectories are also erased when you do so.

# Starting ITDT-SE on i5/OS operating systems

To use ITDT- SE to update firmware or pull dumps from a tape drive inside a tape library, make sure that the drives are varied online in STANDALONE MODE by completing the following steps.

1. Issue the command WRKMLBSTS. Identify the library and drives you want to work with. Note their names (for example, TAPMLB01, TAP01, TAP02).

2. Deallocate the corresponding drives by using option 6.

3. Vary OFF the TAPMLB by using option 2.

4. Enter the following command:

WRKCFGSTS *DEV TAP*

Identify the drives that were noted in Step 1 (for example, TAPMLB01, TAP01, TAP02) and vary them ON by using option 1.

5. Start the iSeries Qshell environment with the following command:

QSH

6. Change to the folder /home/ITDT with the following command:

cd /home/ITDT

7. Start ITDT with the following command.

./itdt

8. Update Firmware and pull dumps. See "Firmware Update" on page 94 and "Dump" on page 93.

9. When firmware updates and dumps are complete, enter the following command:

WRKCFGSTS *DEV TAP*

10. Vary off the TAPs that you worked with by using option 2.

11. Issue the command WRKMLBSTS. Identify the library and drives you worked with.

12. Vary on the TAPMLB by using option 1.

13. Press F5 to refresh the screen. The TAPs belonging to the TAPMLB shows up.

14. Allocate the TAPs back to the TAPMLB by using option 4 or 5.

# Starting ITDT-SE on other supported operating systems

To start ITDT- SE, run the following command

./itdt

At first start, read the User License Agreement:

Press Enter to scroll the license screens forward or b followed by Enter to go back. Type i, if you agree to the terms of license or q followed by Enter to quit the application.

During the initial program start, the input and output directories are created.

Input directory: default directory for firmware files during Firmware Update Output directory: directory that contains the result files, dump files, and log files after tests are run ITDT- SE does not make changes outside the installation directory.

# Generic Operating System driver with ITDT-SE

When the IBM Tape Device Driver cannot be used, ITDT- SE can be forced to use generic Operating System driver devices with the parameter - force- generic- dd.

The parameter is supported in the interactive and scripting mode of ITDT- SE.

For information, go to:

"Standard Edition - Program options" on page 117

82 IBM Tape Device Drivers Installation and User's Guide

- "Device file names 
- device addressing" on page 106- "Scan" on page 89

# Standard Edition - known issues and limitations

This section describes the known issues and limitations of the ITDT- SE program.

# AIX operating systems

The following are the known scan limitations:

- Only devices that have the device state "available".

For FC and SAS devices, ID and LUN greater than 999 are not displayed; they are shown as ###.

When logged in through telnet, backspace might not work - an escape sequence is inserted and the input is ignored after Enter is pressed.

# HP-UX operating systems

Verify that the following patches are installed before ITDT- SE is started.

- PA-Risc: At least these patches:    
- Id and linker tools cumulative patch    
- libc cumulative patch- Itanium/IA-64: All regular patches and the following patches:    
- VxVM 3.5~IA.014 Command Patch    
- VxVM 3.5~IA.014 Kernel Cumulative Patch    
- Aries cumulative patch    
- linker + fdp cumulative patch

Note: ITDT- SE is emulated by Aries (a binary emulator that transparently emulates 32- bit and 64- bit HP- UX PA- RISC applications on HP- UX IA- 64 machines).

On HP- UX11. i/3 systems, tape libraries that are operated through the drive's control path (no control path failover) might disappear from the Device List after a [F] Firmware Update on the controlling drive. It is recommended to complete repeated [S] Scan operations to make the library reappear in the device list.

# Linux operating systems

ITDT- SE on Linux requires glibc 2.2.5 or later.

Note: On an SLES9 s390x (64 bit) configuration, you might experience a SCSI CMD TIMEOUT when the [T] option is run with the IBM Tape Device Driver.

For SUSE SLES9 on zSeries, ensure that the kernel update SUSE- SA:2007:035 is installed.

# Solaris operating systems

Rescan might take 6- 8 minutes, depending on the numbers of host adapters and devices attached.

The known scan limitations: SCSI ID 0- 255, LUN 0- 10.

If the IBM Tape Device Driver is not installed on Solaris 10, tape devices might not be found during scan although they are configured in sgen.conf. When this event occurs, complete the following steps to configure the devices:

1. Check the current driver bindings for IBM tape drives and changers by entering the following commands:

<table><tr><td># egrep &quot;scsiclass,01&quot; /etc/driver_aliases</td><td>(for drives)</td></tr><tr><td># egrep &quot;scsiclass,08&quot; /etc/driver_aliases</td><td>(for changers)</td></tr></table>

2. Modify the /etc/driver_alias file to comment all lines not starting with sgen and containing identification of your drives and changers. Examples:

st "scsiclass,01" (all tape drives) #st "scsiclass,01. vIBM.pULT3580- TD4" (IBM tape drive model ULT3580- TD4) #st "scsiclass,08" #st "scsiclass,08. vIBM.p3573- TL" (IBM changer model 3573- TL)

3. Check that the configured drives are not configured for st driver by entering the following command:

cfgadm - al

If the tape drive is claimed by st device driver, an entry with cxx: rmt/y, is displayed, for example:

c11: rmt/8 tape connected configured unknown

4. Add sgen driver aliases with one of the following commands:

update drv - a - i "scsiclass,01. vIBM.pULT3580- HH4" sgen (adds sgen alias for IBM drive, model ULT3580- HH4) # update drv - a - i "scsiclass,01" sgen (adds sgen alias for all drives attached to the system) # update drv - a - i "scsiclass,08. vIBM.pULT3581- TA2" sgen (adds sgen alias for IBM changer, model ULT3581- TA2) # update drv - a - i "scsiclass,08" sgen (adds sgen alias for all changers attached to the system)

5. Check that the drives and changers are now configured with the following command:

cfgadm - al

6. If the drives or changers are not listed in the output of 'cfgadm - al', reboot the system and verify the list of configured devices with the command:

cfgadm - al

# Windows operating systems

After a firmware update, devices might disappear. This issue is a known Windows problem.

Repeated Scan operations can help to rediscover the device.

When applications are turned on Windows while ITDT- SE is running, an extra ESC character might appear on the input line. When this issue occurs, the input is ignored after Enter is pressed.

If you are using Adaptec SCSI Host Bus adapters, ensure that you are using the latest Adaptec Host Bus Adapter Drivers instead of the drivers that are shipped with the Windows operating system.

On Microsoft Windows systems where the maximum transfer size is limited to less than  $64~\mathrm{kB}$ , the Dump and Firmware Update operations do not work.

# i5/OS operating systems

The Tape Drive must be varied online. If the tape drive is operated through a tape library, the library must be varied offline. See "Starting ITDT- SE on i5/OS operating systems" on page 81 for details.

As the library is varied offline, the Encryption Test does not deliver decrypted data in a Library Managed Encryption environment.

IO adapters without a dedicated IOP do not support commands with 16 bytes. These adapters are called IOP- less and can be identified with the command: WRKHDWRSC *STG. Adapters where the CMBxxx and DCxxx resource names are the same (like both 5774) are IOP- less. If it is IOP, then the CMBxxx is a different type from the DCxxx.

Therefore, the following ITDT commands and test sequences can fail on IOP- less adapters attached devices.

Encryption Verification Test Physical Copy with more than one partition Erase command Change Partition command Read/write - attribute command

ITDT- SE on i5/OS V5R4 requires the following PTF installed.

- PTF: SI25023 Release: 540 Abstract: OSP-MEDIA-TAPE-THREADS-MSGCEE0200-T/QTAHRMGR QTARDCAP FAILS

The [U] Tapeutil option is not available for i5/OS with this release as all the underlying operations require the IBM Tape Device Driver to be installed.

FC 5912 SAS HBA support is only for POWER6 and V6R1 configurations that are attached to LTO Gen 4 HH tape drives (No support for LTO 3 HH SAS).

# All supported operating systems

This section describes the known issues and limitations of the ITDT- SE program on all other supported operating systems.

Prevent/Allow Medium Removal is missing as a Tape Drive option. But, it can still be completed by using the [56] Prevent/Allow Medium Removal option for tape libraries while the tape device is opened.

# User Interface issues

If you press the arrow keys on most UNIX operating system consoles, the input is ignored after Enter is pressed.

When the Tab key is pressed as an input string for field data, the user interface is corrupted.

Make sure that field input does not start with a number followed by space and extra text. This input is interpreted as an entry to a specific row in the field. To avoid this issue, use an underscore character (_) instead of the space character.

# Command timeout

There is no instant operation termination upon SCSI command timeout; for example, when the SCSI cable is unplugged after POST A is started.

When a command timeout condition occurs, ITDT might still continue to complete more operations (like unmounting the cartridge) instead of instantly terminating with a timeout condition.

# TS3310 tape libraries

Library Firmware Update with ITDT- SE and ITDT- GE is not supported by the TS3310 libraries. Update the firmware by using the Web User Interface for those libraries.

# Standard Edition - Start menu commands

After program startup, ITDT- SE displays the start screen menu.

![](images/c3ede3a426f76b6d0bcf9889a90b0960bc0afd7acf16eebb1f630cebe271c431.jpg)  
Figure 13.Start screen menu

The following commands are available on the start screen menu:

Figure 13. Start screen menuThe following commands are available on the start screen menu:- [S] Scan for tape drives and enter Diagnostic/Maintenance ModeOpens the screen for the Scan function (refer to "Standard Edition - Scan menu commands" on page 86).- [H] HelpHelp starts and displays the available online help.- [Q] Quit programQuits the program.- [U] TapeutilOpens the screen for the Tapeutil operation commands. These commands are the standardized tapeutil functions with most of the options available that were available with the previous tapeutil functions (refer to "Standard Edition - Tapeutil menu commands" on page 106).- [A] Add Device ManuallyOpens the screen for specifying a device manually instead of using the Scan function.- [P] PreferencesOpens the dialog where default program settings can be defined and altered.

# Standard Edition - Scan menu commands

When ITDT- SE is used after S is entered on the start screen, the Scan function starts and displays the first device list screen.

![](images/fa8781eb6ad30820b24d87be1924d1d73c404c5928018e6cc9298cffa27479e7.jpg)  
Figure 14. Scan screen menu

![](images/a270b4f65e5083541fc6ca51d1bfe01f2d9b54279fbbe0ac14310afc2c25ebfa.jpg)  
Figure 15. Scan menu

To select a device, enter a number from the leftmost column, then click Enter.

Entering the M command returns to the Start Screen menu. Entering the V command toggles between displaying the physical device address and the driver name.

![](images/c30feb4d916abd04d5ab6084e2802f73dc00697d79cf1c7bc7ad27f95a9f21e4.jpg)  
Figure 16. Scan screen second menu

This screen contains the S, T, D, F, Y, J, and A commands. Entering the O command displays the second device list screen. The second screen contains the W, U, K, L, I, E, C, and R commands.

The following commands are described:

- S 
- "Scan" on page 89- T 
- "Health Test" on page 91- D 
- "Dump" on page 93- F 
- "Firmware Update" on page 94- Y 
- "System Test" on page 97- J 
- "Eject Cartridge" on page 98- A 
- "Cleaning Statistics" on page 99

O- "Other Functions" on page 99

![](images/7d0d723d3c99df67eedf5668d535f19d4c0f9b64336078c08dfc700653f7cb10.jpg)  
Figure 17. More scan options

![](images/ccb7258432a7ef50f8a570b5dc14291a2a7a086b932ad090ed3250e9b59bd72c.jpg)  
Figure 18. More scan options

W- "Full Write" on page 99- U- "Tape Usage" on page 100- K- "Check LTFS Readiness" on page 101- L- "Library Test" on page 102- I- "Library Media Screening" on page 102- E- "Encryption" on page 102- C- "Configure TCP/IP" on page 104- R- "Return" on page 106

# Scan

ScanThe Scan function [S] is used to discover all supported tape and library devices that are attached to the computer system so that they can be selected for the subsequent ITDT- SE operations. The scan function also serves as a connection test that can be used to verify correct attachment of the devices.

Make sure that no other program is accessing the devices that are used by ITDT- SE. For example, stop the backup jobs that are accessing the devices when ITDT- SE is used, or if not sure, stop the entire backup application.

After ITDT- SE is started, type S followed by Enter to activate the scan function.

Depending on the operating system and the number of attached drives, the scan can take several minutes. See "Standard Edition - known issues and limitations" on page 83 for details.

During the scan operation, a bar in the lower left edge of the screen shows that the scan operation is still in progress.

When the scan is finished, the first device list screen is displayed.

![](images/37662e99c2370d673225a298c63ca3652cc1bbdc30874c0e3e3bbdb5bf83d499.jpg)  
Figure 19. Device List screen

![](images/fb7fc156f5ab83aa72633a794af5a4416583eafa2dc0264f3933142f2ef3a0cc.jpg)  
Figure 20. Device List screen

The first device list screen shows all detected devices and the connection information (host adapter number, bus number, SCSI/FCP ID, and LUN or driver name) along with product data (Model name, Unit Serial number, Microcode revision). For drives that are attached to a library, the Changer column shows the serial number of the changer the drive is attached to.

Scrollable data is indicated by "VVVVVVVV" in the bottom border of the table. To view the non- displayed entries, type + and press Enter.

Note: For fast down scrolling, type + followed by a space and the number of lines to scroll down, then press Enter. Alternately, type N and press Enter to scroll down one page.

To scroll back, use - instead of +.

Note: For fast up (backward) scrolling, type - followed by a space and the number of lines to scroll up. Press Enter, or type P and press Enter to scroll up one page.

If no devices appear or if devices are missing in the list, make sure that

- ITDT-SE is running with administrator/root rights.- The devices are properly attached and powered on.- Linux: The devices must be attached at boot time.- i5/OS: Only tape drives are detected.- Solaris, when no IBM tape device driver is in use:    
- Ensure that sgen is correctly configured.

Ensure that sgen is correctly configured.

file /kernel/drv/sgen.conf

is correctly configured (see "Starting ITDT- SE on Solaris operating systems" on page 80).

- Solaris 10, see (see "Standard Edition 
- known issues and limitations" on page 83)

- More than 10 devices displayed 
- scroll down the Device List.

ITDT- SE uses the IBM Tape Device Driver for its operations. If no IBM Tape Device Driver is installed, the generic device driver for the operating system is used instead. On Microsoft Windows, any Tape Device Driver that is installed is used.

If you must bypass the IBM Tape Device Driver for diagnostic purposes, start ITDT- SE with the following command.

itdt - force- generic- dd

Note: For operating system- specific information on how to use this command, see the corresponding Initial Startup sections.

When the wanted device is displayed, select the device for test. Only one device can be selected at a time.

# Health Test

The Health Test function [T] checks if the tape device is defective and outputs a pass/fail result.

Attention: The health test function erases user data on the cartridge that is used for the test.

For the library or autoloader test, the Library Test [L] must be selected.

# Note:

1. The test can take from 15 minutes up to 2 hours.

2. The test runs only on tape drives, not on autoloaders or libraries.

To complete the test function, it is recommended that a new or rarely used cartridge is used. Scaled (capacity- reduced) cartridges must not be used to test the device.

To test tape drives within a library, the library must be in online mode.

1. Start ITDT-SE, then type S and press Enter to scan for the devices.

2. Select the device that you want to test by entering its number and press Enter.

3. Type T followed by Enter to activate the test.

If no cartridge is inserted, ITDT- SE prompts to insert a cartridge. Either insert a cartridge and press Enter or stop the test by entering C followed by Enter.

Note: If ITDT- SE detects data on the cartridge, the Device Test screen displays a message (as shown in Figure 21 on page 92.

![](images/edbe08b61ad88810fe4d380563f9ee32978aa2dd11e351eb3342192fda464fa6.jpg)  
Figure 21. Data Delete question

Type Y followed by Enter to continue the test if you are sure that data on the cartridge can be overwritten. If you are unsure, type N followed by Enter to stop the test.

During the test, the program shows a progress indicator in the form of a bar of number signs (#) (#) that shows the progress of a single subtest and also a description of that subtest. The user might stop the test by selecting the [A] Abort option (exception: POST A).

During the test, a progress indicator (#) is shown on the test screen. Messages from the test steps are shown in the Status field [2].

![](images/43da1fcbb1f9c676fff2e3a6f12f118b21f385db115d4c8d729db6074f7c2a51.jpg)  
Figure 22. Test running

The test sequence contains the following steps.

1. Initialize Device  
2. Read Thermal Sensor (might get skipped)  
3. Mount Medium  
4. [Medium Qualification] - only if the previous step indicated this requirement

5. Load/Write/Unload/Read/Verify  
6. POST A  
7. Performance Test (run 2 times if first run failed with performance failure)  
8. Unmount Medium  
9. Read Thermal Sensor (might get skipped)  
10. Get FSC  
11. Get Logs

The test can be stopped by typing A followed by Enter at any time except during the POST test, which is not interruptible.

Note: It might take some time until the test stops.

![](images/214696e617cd6ffdd407301f5785df50a142edb6ebbab05c1133cf232dd9454e.jpg)  
Figure 23. Test results

When all subtests are finished, ITDT- SE shows a screen that displays the attachment and device information as in the first device list screen. It also shows the test result and failure information in the code field. The screen also shows the output files that were generated during the test run. The files might be requested by the IBM Support Center.

If you want to use other ITDT- SE functions, type R followed by Enter to return to the first device list screen. Otherwise, type Q followed by Enter to exit the program.

# Dump

Complete the following steps to start the Dump [D] process.

1. Start ITDT-SE, then type S and press Enter to scan for the devices.  
2. Select the device that you want to retrieve a dump from by entering its number and pressing Enter.

3. Type D and press Enter to start the dump retrieval for the selected device. The ongoing dump process is completed (it takes less than 1 minute).

![](images/7f07a49eb6d35de8deb57fcdd42ad140cef6ce9aa1d0feb546cc585fffa54de7.jpg)  
Figure 24. Dump

When the dump process is completed on a tape library or autoloader other than the 3584/TS3500/ TS4500, the Dump function stores 1 log file in the output folder of the program (\*.blz). For the 3584/ TS3500/TS4500, a dump file (\*.a) is stored in the output folder.

Note: When the Dump function is completed for tape libraries or autoloaders other than the 3584/ TS3500/TS4500, the log file contains only Log Sense and Mode Sense pages, while a Drive or 3584/ TS3500/TS4500 dump contains much more diagnostic information.

Retrieve the files from the ITDT- SE output subdirectory that was created during the installation. The following are examples of the directory:

- Example output directory (Windows): c:\ITDT\output- Example output directory (UNIX): /home/user/ITDT/output- Example output directory (i5/OS): /home/ITDT/output (On the IFS) use FTP or the System i Navigator to transfer the file

If you want to use other ITDT- SE functions, type R followed by Enter to return to the device list; otherwise, type Q followed by Enter to exit the program.

# Firmware Update

The Firmware Update [F] upgrades the firmware of tape drives and tape libraries.

![](images/6e13181811c5d3b00661c4ca4e0f8c1b05e77e421624c4925c9f32abb0f4bcfa.jpg)  
Figure 25. Firmware Update screen

# Note:

The following site is available for the latest firmware files: https://www.ibm.com/support/fixcentral.

1. Press the button Select Product  
2. Product Group: System Storage  
3. Tape systems  
4. Tape drives or Tape auto loaders and libraries  
5. Select your product accordingly  
6. Press Continue  
7. Select the related fix and download the file(s).

Download the files to the ITDT- SE input subdirectory that was created during the installation. The following are examples of the directory:

Example input directory (Windows): c:\ITDT\input  Example input directory (Unix): /home/user/ITDT/input  Example input directory (i5/0s): /home/ITDT/input (on the IFS) use FTP or the i- Series Navigator to transfer the file

To do a Firmware Update, complete the following steps:

1. Start ITDT-SE, then type S and press Enter to scan for the devices.  
2. Select the device that you want to update by typing the number of the device and pressing Enter.  
3. Type F and press Enter to display the Firmware Update screen.

4. To select the needed firmware update, complete one of the following steps:

If the downloaded firmware file is listed in the Content field of the Firmware Update screen, type the corresponding line number and press Enter. . If the firmware file is stored in a directory other than FW Dir, type F followed by a space and the fully qualified path to the directory that contains the firmware file, then press Enter.

For example, enter the following to change the firmware directory (UNIX):

f /home/user/firmware

If no files are displayed in the Content field, check the Dir OK field on the right side of the screen. It indicates true if the directory exists, false otherwise.

: If the content of the displayed FW Dir changed, type D and press Enter to refresh the directory content.

Note: The selected file name is reset to the first item (#0) after the Refresh function is used.

If the displayed directory contains more files than the files shown, type  $^+$  and press Enter to scroll down the list. For fast down scrolling type  $^+$  followed by a space and the number of lines to scroll down then press Enter. To scroll back, use - instead of  $^+$

Scrollable data is indicated by "VVVVVVVVVVVVVVVV".

![](images/634da6bfc8b68eabf7ce6e556fac3d12a34a99f1c6faad82aa377680c37311df.jpg)  
Figure 26. Scrollable Data screen

5. After the firmware file is selected, type C and press Enter to continue.

6. Before the firmware update is started, make sure the file that is displayed in the FW File field is the correct file.

If the correct file is displayed, proceed to the next step. If the correct file is not displayed, type C and press Enter to change the selected firmware file. Go to Step 4.

Note: The selected file name is reset to the first item in the list when you return to that dialog from the Start Update dialog.

7. If you decide to run the firmware update, type S and press Enter to start the firmware update. During the firmware update, a firmware update progress screen is displayed.

Attention: Once started, do not interrupt the firmware update.

The firmware update usually takes 3- 5 minutes, but it can take up to 45 minutes for libraries. If you decide not to run the firmware update, type R and press Enter to return to the Device List.

Note: If ITDT- SE detects FIPS- certified drive firmware, it displays a warning dialog. Before you continue, ensure that you use FIPS- certified firmware to update the drive.

8. After completion, the Status field on the lower right side indicates PASSED if the firmware was updated successfully and FAILED otherwise.

Type R and press Enter to return to the Device List.

# System Test

The System Test [Y] is a short test that completes the following steps:

Reveals system performance bottlenecks. Compressible data throughput values can reveal bandwidth limitations that are caused by the system, cabling, or HBA. Measures performance variations across the different block sizes to find the ideal block size for the system configuration.

The System Test runs only on tape drives, not on autoloaders or libraries. To complete a System Test on tape drives within a library, the library must be in online mode.

1. Start ITDT-SE, type S, and press Enter to scan for the devices.

2. Type Y and press Enter to start the System Test.

ITDT- SE then switches to the System Test screen. If no cartridge is inserted, ITDT- SE prompts to insert a cartridge. Either insert a cartridge and press Enter or stop the test by typing C followed by Enter.

Note: If ITDT- SE detects data on the cartridge, it shows the System Test screen, and displays the following message.

Cartridge not empty!

Type Y followed by Enter to continue the test if you are sure that data on the cartridge can be overwritten. If you are unsure, type N followed by Enter to stop the test.

The System Test is completed as follows:

a. System Test determines the amount of data to write for each supported blocksize (a percentage of the cartridge is written for each blocksize) 
b. The test determines the maximum supported blocksize of the system - maximum is 8 MiB (generic 1 MiB). 
c. Next, the System Test writes the calculated size with supported block sizes in powers of two down to 64 kB (at maximum 8192, 4096, 2048, 1024, 512, 256, 128, 64), first with incompressible data, then with compressible data. Five different block sizes are written.

d. At the end of the test, a summary screen is displayed.

![](images/85ff27e3656d4bf13f4312f1d7547d03f68e63e426f1fc06fde43dcc61fdb2b5.jpg)  
Figure 27. System Test results

"Compressible  $=$  Yes" means that the data written was just zeros so that the data is compressed by the drive with a maximum compression ratio. "Compressible  $= \mathbb{N}_0"$  means that a data pattern was written that the drive almost cannot compress at all. If the compression ratio is 1, the drive was not able to compress the data (equivalent to 1:1 compression ratio). If the compression ratio is 94.0, the drive was able to do 94:1 compression, meaning that 94 bytes in the original data is compressed to 1 byte on the medium. 100.0 means 100 bytes is compressed down to 1 byte on the medium.

The System Test can be stopped by typing A followed by Enter at any time.

Note: It can take some time until the System Test stops.

If you want to use other ITDT- SE functions, type R followed by Enter to return to the device list. Otherwise, press Q followed by Enter to exit the program.

# Eject Cartridge

The Eject Cartridge [J] function unloads a cartridge from a tape drive.

1. Start ITDT-SE, then type S and press Enter to scan for the devices. 
2. Select the device that you want to unload a cartridge from by entering its number and pressing Enter. 
3. Type J and press Enter to unload a cartridge.

# Cleaning Statistics

Cleaning Statistics [A] retrieves statistical data about cleaning actions. Some devices do not support this function.

1. Start ITDT-SE, type S, and press Enter to scan for the devices.  
2. Select the device that you want to retrieve cleaning statistics from by entering its number and pressing Enter.  
3. Type A and press Enter to start the cleaning statistics retrieval for the selected device.

# Other Functions

Other Functions [O] - type O followed by Enter to display a screen with the following commands:

W- "Full Write" on page 99 U- "Tape Usage" on page 100 K- "Check LTFS Readiness" on page 101 L- "Library Test" on page 102 I- "Library Media Screening" on page 102 E- "Encryption" on page 102 C- "Configure TCP/IP" on page 104 R- "Return" on page 106

# Full Write

The Full Write [W] function writes the entire cartridge with a specified block size either with compressible or incompressible data and output performance data.

Attention: The Full Write function erases data on the cartridge that is used for the test.

# Note:

1. The Full Write function takes approximately 2 hours when incompressible data is written, less time for compressible data.  
2. The Full Write function runs only on tape drives, not on autoloaders or libraries.

The Full Write test can be used to

Demonstrate that the drive can write the full amount of data on a cartridge. Identify system issues with compression.

Drive data compression is always turned on during the full write. When run with compressible data, the output shows the compression rate. If the compression rate is higher than 1.0 but the system does not appear to be able to compress data on the cartridge, check the device driver and software settings to see whether they disable compression.

1. After ITDT-SE is started, type S followed by Enter to activate the device scan.

2. Select the device that you want to write to by entering its number and press Enter.

3. Type W and press Enter to start the full write.

ITDT- SE then switches to the Full Write screen. If no cartridge is inserted, ITDT- SE prompts to insert a cartridge. Either insert a cartridge and press Enter or stop the test by typing C followed by Enter.

Note: If ITDT- SE detects data on the cartridge, it shows the Full Write screen, and displays the following message:

Cartridge not empty!

Type Y followed by Enter to continue the test if you are sure that data on the cartridge can be overwritten. If you are unsure, type N followed by Enter to stop the test.

4. The system prompts for entry of a transfer size between 32 KB and the maximum block size that is supported by the system (maximum value is 8 MiB). This action is a check for the type of supported block size that is completed. Enter the appropriate values for your system.

Note: Values of 16 KB and 32 KB are not tested in cases where the capability of a system supports higher block sizes.

5. Select the type of data to write, either [C] Compressible or [I] Incompressible.

During the full write, the program shows a progress indicator in form of a bar of number signs (#) that shows the progress of the full write.

The full write can be stopped by typing A followed by Enter at any time.

Note: It can take some time until the full write stops.

If all write operations are finished, ITDT- SE shows a screen that displays the compression ratio (1) and the write performance (shown in 2 as the Data Rate) for the selected block size. If an error occurred during the full write, data is only written partially.

"Compressible = Yes" means that the data written was just zeros so that the data is compressed by the drive with a maximum compression ratio. "Compressible = No" means that a data pattern was written that the drive almost cannot compress at all. If the compression ratio is 1, the drive was not able to compress the data (equivalent to 1:1 compression ratio). If the compression ratio is 94.0, the drive was able to do 94:1 compression, meaning that 94 bytes in the original data is compressed to 1 byte on the medium. 100.0 means 100 bytes is compressed down to 1 byte on the medium.

If you want to use other ITDT- SE functions, type R followed by Enter to return to option 4 the device list. Otherwise, type Q followed by Enter to exit the program.

![](images/9cea775197a9e57f00bc606639a8ccd2f55d7334a0c9edfd42af3f8948023781.jpg)  
Figure 28. Full Write results

# Tape Usage

The Tape Usage [U] function retrieves statistical data and error counters from a cartridge.

![](images/4ac21eed90b0672d1bc6c231566dd094a212bcd689411f3842eb751ffeb72298.jpg)  
Figure 29. Tape Usage screen

1. After ITDT-SE is started, type S followed by Enter to activate the device scan.

2. Select the device that you want to test by entering its number and press Enter.

3. Type U followed by Enter to start the tape usage log retrieval. ITDT-SE then switches to the tape usage screen. If no cartridge is inserted, ITDT-SE prompts to insert a cartridge. Either insert a cartridge and press Enter or stop the test by entering C followed by Enter.

During the gel logs operation, the program shows a progress indicator in form of a bar of number signs (#) that shows the progress of a single suboperation and a description of that operation.

When all suboperations are finished, ITDT- SE shows a Tape Usage completion screen. The Status field on the lower right side indicates PASSED if the log retrieval completed successfully and ABORTED otherwise.

# Check LTFS Readiness

The Check LTFS Readiness test analyzes the operating system and tape drive environment to ensure that the IBM linear tape file system can be installed. This test checks the operating system version, the tape device driver version, the tape drive firmware, and the LTFS HBA requirements.

Note: The tape drive firmware must be at least version: C7RC (for LTO 5), C974 (for LTO 6), and 36A5 (for TS1140 and TS1150). The LTFS Readiness Check requires an empty data cartridge. The maximum transfer size is detected at the beginning of the test. If it is under 512kB, a warning is displayed. If it is under 256kB, the test fails.

The LTFS Readiness Check can return with result FAILED and one of the following error codes.

<table><tr><td colspan="2">Table 12. Codes and root causes</td></tr><tr><td>Code</td><td>Root causes</td></tr><tr><td>LTFS CDB LENGTH</td><td>Unsupported SCSI 16 Byte command.</td></tr><tr><td>LTFS CARTRIDGE TYPE</td><td>Cartridge Density Code is not supported.</td></tr><tr><td>LFTS XFER SIZE</td><td>Supported block size is too less.</td></tr><tr><td>LTFS PARTITION SUPPORT</td><td>Partitioning commands are not supported.</td></tr><tr><td>LTFS FW LEVEL</td><td>Unsupported Tape Device firmware level.</td></tr><tr><td>LTFS DATA TRANSFER SIZE</td><td>Data Integrity Test failed.</td></tr><tr><td colspan="2">Table 12. Codes and root causes (continued)</td></tr><tr><td>Code</td><td>Root causes</td></tr><tr><td>LTFS DD VERSION</td><td>IBM Tape Device Driver version is not supported.</td></tr><tr><td>LTFS OPERATING SYSTEM</td><td>Operating system is not supported.</td></tr></table>

# Library Test

The Library Test [L] starts and monitors the library- internal self- test. This test runs only on libraries and autoloaders, not on tape drives.

1. Start ITDT-SE, type S, and press Enter to scan for the devices.

2. Type 0 and press Enter to display the second device list screen.

3. On the second device list screen, type L and press Enter to start the Library Test.

A Device Test screen is displayed and a functionality test on the tape library is completed.

At the end of the test, a results screen is displayed.

The Library Test can be stopped by typing A followed by Enter at any time.

Note: It can take some time until the Library Test stops.

If you want to use other ITDT- SE functions, type R followed by Enter to return to the device list; otherwise press Q followed by Enter to exit the program.

# Library Media Screening

Library Media Screening [L] generates dumps for each drive and cartridge within a library. It runs only on libraries (except TS3500/TS4500) and auto- loaders, not on tape drives.

First, the test tries to read dump files from each drive that is installed from the library. After that, the customer must select one drive for loading the cartridges.

All cartridges of the I/O and storage slots are moved - one after the other - from their source to the selected drive. A dump is taken and moved back to the source address.

In the result screen, the dumps taken and the count of dumps are displayed.

# Encryption

The Encryption [E] function is used to verify whether data on the cartridge was written encrypted. It reads both decrypted and raw data from the cartridge into two separate files on disk. The user can then verify that the data differs to ensure that encryption worked.

The Encryption function does not provide a Write - Read test.

The Encryption function is supported only on encryption enabled drives. It requires that an encryption infrastructure, including the Encryption Key Manager (EKM), is properly set up. An encrypted data cartridge must be used.

The Encryption function is supported for the following encryption environments:

- System Managed: IBM tape device driver must be installed and in use by ITDT to read decrypted data- Library Managed- Application Managed: Only raw encrypted data is read (result file *.ENC)

Note: On i5/OS, media changers and media changer operations are not supported by this release of ITDT- SE. To test a tape drive inside a library, the tape drive must be varied online and the tape library must be varied offline (see "Starting ITDT- SE on i5/OS operating systems" on page 81 for details). As the library is varied offline, the Encryption function does not deliver decrypted data in a Library Managed Encryption environment.

1. After ITDT-SE is started, type S followed by Enter to activate the device scan.

2. Select the device that you want to test by entering its number and press Enter.

3. Type E and press Enter to start the encryption test. ITDT-SE then switches to the Encryption Verification screen. On this screen, the system requires the entry of the number of the start record and the amount of data (in KB) to be read.

4. Type S followed by a space and the start record number, then press Enter to enter the start record number. Type L followed by a blank and the data length, then press Enter to enter the data length, maximum 100000 KB.

![](images/c68ef3d41da68648bfe48782d78979d642efef270b8f49e7fc8e533c75473fd4.jpg)  
Figure 30. Encryption Start screen

5. If you entered the values correctly, press Enter to start the encryption.

During the encryption, the program shows a progress indicator in form of a bar of number signs (#) that shows the progress of a single subtest and information about that subtest.

The Encryption function can be stopped by typing A followed by Enter at any time.

Note: It can take some time before the Encryption function stops.

If all encryption operations are finished, ITDT- SE shows a screen that displays the Status field on the lower left side that indicates PASSED if the encrypted test completed successfully and ABORTED otherwise.

The screen also shows the output files that were generated during the Encryption function:

file serial# .n.DEC contains the raw encrypted data file serial# .n.DEC contains the decrypted data

Table 13 on page 103 defines the abort codes.

<table><tr><td colspan="2">Table 13. Abort code definitions</td></tr><tr><td>ABORT CODE</td><td>ROOT CAUSE</td></tr><tr><td>LOCATE FAILED</td><td>Start position as requested by the user was not reached</td></tr><tr><td>MEDIUM NOT ENCRYPTED</td><td>ITDT detected medium as non-encrypted</td></tr><tr><td>NO DRIVER SPECIAL FILE</td><td>System-Managed environment, but generic device file is used instead of IBM device driver special file</td></tr><tr><td>DRIVE ENCRYPTION DISABLED</td><td>Mode Sense detected disabled drive encryption</td></tr><tr><td colspan="2">Table 13. Abort code definitions (continued)</td></tr><tr><td>ABORT CODE</td><td>ROOT CAUSE</td></tr><tr><td>UNEXPECTED DATA</td><td>• Set Raw read mode failed
• One of the commands failed</td></tr><tr><td>END OF MEDIUM</td><td>End of medium that is encountered before the specified amount of data was read</td></tr><tr><td>END OF DATA</td><td>End of data that is encountered before the specified amount of data was read</td></tr><tr><td>READ FAILED</td><td></td></tr><tr><td>ENCRYPTION ERROR</td><td></td></tr><tr><td>INVALID PARAMETER</td><td>User entered data length of 0 kB</td></tr><tr><td>FILE IO ERROR</td><td>The hard drive that ITDT is installed on might have run out of space.</td></tr></table>

If you want to use other ITDT- SE functions, type R followed by Enter to return to the device list. Otherwise, type Q followed by Enter to exit the program.

# Configure TCP/IP

Configure TCP/IP [C] configures the Ethernet settings of LTO 5, TS1140, and later drives. For those drives, the current settings are read and displayed and can be changed.

Note: LTO drives have one port with 2 sockets and TS1140 and later drives have two ports and 4 sockets can be configured. Configuring the Ethernet must not be done in a TS3500/TS4500. Although the ports can be configured, it is ineffective.

1. Start ITDT-SE, type S, and press Enter to scan for the devices.

2. Select a device from the list (just the ones that are listed are supported) by entering the number, then press Enter.

3. Type O and press Enter to display the second device list screen.

4. On the second device list screen, type C and press Enter to open the TCP IP screen.</li>

ITDT- SE switches to the TCP IP screen, reads the data configuration and displays it in a table.

![](images/b752df5aba3642ca4a00cdf597c818c8c0536cf17b3df3701f9620a734ce4262.jpg)  
Figure 31. TCP/IP screen: Query configuration

Select the Port/Socket that you want to alter by entering its number (0. .1 for  $\mathsf{L}\mathsf{T}\mathsf{O}\mathsf{5}+$  and 0. .3 for  $\mathsf{TS1140 + }$  and press Enter.

To alter the values for the selected Port/Socket type S and press Enter. The current values are loaded.

![](images/1f2b506550b76afc14f5704bdf6dc76e569df43ca36a70df79138ba5a182f190.jpg)  
Figure 32. TCP/IP screen: Read data

Each parameter can be set by entering the number (1 - 3) and a following value.

If you want to enable DHCP, enter 1 1 and press Enter. The value of the DHCP field is refreshed with the value entered.

To change the IP address, enter 2 and the fixed IP Address you want to set followed by pressing Enter. The value of the IP Adress field is refreshed with the value entered.

For the subnet mask length, enter 3 and the Subnet Mask Length you want to set followed by pressing Enter. The value of the Subnet Mask Length field is refreshed with the value entered.

![](images/3ce074f15394485570bcafb0b6b927a5ce4be9a37e4b859ae0c01984a0ca4b63.jpg)  
Figure 33. TCP/IP screen: Altered data

Regular field values:

[1] DHCP enabled: 0/1 (false/true)  [2] Address IPv4 Regular IPv4 address  [3] Address IPv6: Regular IPv6 address  [3] Subnet Mask Length V4: 0. ..23  Subnet Mask Length V6: 0. ..127

The values are applied to the drive by entering A and pressing Enter. ITDT- SE configures the drive and the current addresses are shown.

![](images/8f2ceb089c8803a50af3160f11b2b115d1f8bc76f94e1e32eea498e2a0d09a17.jpg)  
Figure 34. TCP/IP screen: Applied values

If you want to use other ITDT- SE functions, type R, then press Enter to return to the device list. Or, press Q and Enter to exit the program.

# Return

Return [R] - type R followed by Enter to go back to the first device list screen.

# Standard Edition - Tapeutil menu commands

When the user runs the U command on the ITDT- SE start screen, the Tapeutil operation screen is displayed.

Note: On any screen, to start a command, press the shortkey displayed in brackets [], followed by Enter.

The following commands are described in this section:

# [1] Open a Device

When you select the Open a Device command [1]:

1. ITDT checks if a device is already opened.  
2. You are prompted for a device special file name.  
3. You are prompted for an open mode (rw, ro, wo, append).  
4. ITDT opens the device that you selected.

Note: Always use the Read Only mode when you are working with write- protected media.

The combination of open cmd with parameter - - force- generic- dd is not supported.

# Device file names - device addressing

ITDT supports generic and Device Driver claimed devices. This section shows examples for device names (addressing) of all supported platforms. The used abbreviations stand for:

host Number of the host adapter (SCSI, FC, SAS)  bus Number of the bus from the host adapter  target Target Number of the device

Note: The correct IDs are reported in the ITDT Control Center after a scan or with the scripting function "scan".

<table><tr><td colspan="4">Table 14. Device addressing</td></tr><tr><td></td><td>IBM Tape Device Driver</td><td>Generic IDs separated with blanks</td><td>Generic (alternative, as a result from &quot;ltdt scan&quot;)</td></tr><tr><td>IBM AIX</td><td>/dev/rmtX.Y
/dev/smcX</td><td rowspan="5">&lt;host&gt;&lt;host&gt;&lt;lun&gt;</td><td rowspan="5"></td></tr><tr><td>Linux</td><td>/dev/IBMtapeX
/dev/
IBMchangerX</td></tr><tr><td>Microsoft Windows</td><td>\.\.\tape0
\.\.\changer0</td></tr><tr><td>Oracle Solaris</td><td>/dev/rmtXsmc
/dev/smc/Xchng</td></tr><tr><td>HP-UX</td><td>/dev/rmt/Xmnb
/dev/rmt/Xchng</td></tr><tr><td>Apple Mac</td><td>-</td><td>&lt;host&gt;&lt;host&gt;&lt;lun&gt;</td><td>H&lt;host&gt;-B&lt;bus&gt;-T&lt;target&gt;-L&lt;un&gt;</td></tr><tr><td>IBM &#x27;i&#x27;</td><td>-</td><td>-</td><td>Device Name; for example, TAP01</td></tr></table>


</un></lun></host></lun></host></lun></un></lun></host></lun></un></lun></un></lun></lun></lun></lun></lun></lun></lun></lun></lun></lun></lun></lun></lun></lun></lun></lun></lun></lun></lun></lun></lun></lun></lun></lun></lun></lun></lun></lun></lun></lun></lun></lun></lun></lul></lun></lun></lun></lun></lun></lun></lun></lun></lun></lun></lun></lun></lun></lun></lun></lun></lun></lun></lun></lun></lun></lun></lun></lun></lun></lun></lun></lun></lun></lun></lun></lun></lun>

Example for Linux (IBM Tape Driver devices):

./itdt scan Scanning SCSI Bus #0 /dev/IBMchanger2 [3573- TL]- [C.50] S/N:00L4U78D6118_LL0 H3- B0- T5- L1 (IBM- Device) #1 /dev/IBMchange2 [ULT3580- TD5]- [BBNE] S/N:1168001104 H3- B0- T5- L0 Changer: 00L4U78D6118_LL0 (IBM- Device) Exit with code: 0

Example for Linux (generic Operating System devices):

./itdt - force- generic- dd scan Scanning SCSI Bus #0 /dev/sg7 - [3573- TL]- [C.50] S/N:00L4U78D6118_LL0 H3- B0- T2- L1 (Generic- Device) #1 /dev/sg8 - [ULT3580- TD5]- [BBNE] S/N:1168001104 H3- B0- T5- L0 Changer: 00L4U78D6118_LL0 (Generic- Device) Exit with code: 0

To open the tape device by using the IBM Tape Device Driver, use the /dev/IBMtape3 as device file name. By using H3- B0- T5- L0 or 3 0 5 0 as the device name, it is opened with the generic Operating System device driver.

# [2] Close a Device

When you select the Close a Device command [2]:

1. ITDT checks if the device is already closed.  
2. ITDT closes the device.

# [3] Inquiry

When you select the Inquiry command [3]:

1. You are prompted for page code.  
2. ITDT then displays a decoded format of a hexadecimal dump and prints a hexadecimal dump of the inquiry data.

The equivalent ITDT scripting command is inquiry

# [4] Test Unit Ready

When you select the Test Unit Ready (TUR) command [4], ITDT issues the Test Unit Ready command to the device.

The equivalent ITDT scripting command is tur

# [5] Reserve Device

When you select the Reserve Device command [5], ITDT issues a reserve command to the device. The equivalent ITDT scripting command is reserve

# [6] Release Device

When you select the Release Device command [6], ITDT issues a release command to the device. The equivalent ITDT scripting command is release

# [7] Request Sense

When you select the Request Sense command [7]:

1. ITDT issues a Request Sense command.  
2. ITDT then displays a decoded format of hexadecimal dump sense data and prints hexadecimal dump sense data.

The equivalent ITDT scripting command is requestsense

# [8] Log Sense

When you select the Log Sense command [8]:

1. You are prompted for Log Sense Page.  
2. ITDT issues a log sense command.  
3. ITDT completes a hexadecimal dump page. The equivalent ITDT scripting command is logsense

# [9] Mode Sense

When you select the Mode Sense command [9]:

1. You are prompted for Mode Sense Page.  
2. ITDT issues mode sense command.  
3. ITDT completes a hexadecimal dump page. The equivalent ITDT scripting command is modesense

# [10] Query Driver Ver. (Version)

When you select the Query Driver Version command [10]:

1. ITDT issues the required command to print IBM Tape Device device driver  
2. ITDT prints the driver version. The equivalent ITDT scripting command is qryvers

# [11] Display All Paths

When you select the Display All Paths command [11]:

All configured SCSI paths for this device are queried. Such as logical parent, SCSI IDs, and the status of the SCSI paths for the primary path and all alternative paths that are configured.

The equivalent ITDT scripting command is qrypath

Note: The command is only supported for devices that are using the IBM Tape Device Driver.

# [12] Query Runtime Info

When you select the Query Runtime Info command [12]:

1. ITDT issues the required command to get the runtime info.  
2. ITDT prints out the Dynamic Runtime Attribute Values.

The equivalent ITDT scripting command is runtimeinfo

Note: The command is only supported for tape devices.

# [20] Rewind

When you select the Rewind command [20], ITDT rewinds a loaded cartridge.

The equivalent ITDT scripting command is rewind

Note: The command is only supported for tape devices.

# [21] Forward Space File Marks

When you select the Forward Space File Marks command [21]:

1. You are prompted for the number of file marks to space forward.  
2. ITDT is spacing the number of specified file marks in the forward direction.

The equivalent ITDT scripting command is fsf

Note: The command is only supported for tape devices and requires a loaded cartridge.

# [22] Backward Space File Marks

When you select the Backward Space File Marks command [22]:

1. You are prompted for the number of file marks.  
2. ITDT is spacing the number of specified file marks in the backward direction.

The equivalent ITDT scripting command is bsf

Note: The command is only supported for tape devices and requires a loaded cartridge.

# [23] Forward Space Records

When you select the Forward Space Records command [23]:

1. You are prompted for the number of records to space forward.  
2. ITDT is spacing the number of specified data records in the forward direction.

The equivalent ITDT scripting command is fsr

Note: The command is only supported for tape devices and requires a loaded cartridge.

# [24] Backward Space Records

When you select the Backward Space Records command [24]:

1. You are prompted for the number of records to space backward.  
2. ITDT is spacing the number of specified data records in the backward direction.

The equivalent ITDT scripting command is bsr

Note: The command is only supported for tape devices and requires a loaded cartridge.

# [25] Space to End of Data

When you select the Space to End of Data (EOD) command [25], ITDT is spacing from the current position to the end of data.

The equivalent ITDT scripting command is seod

Note: The command is only supported for tape devices and requires a loaded cartridge.

# [26] Read and Write Tests

When you select the Read and Write Tests command [26]:

: You are prompted for block size (If you press Enter, the default block size is 10240 bytes). Note: If the block size is zero, variable mode is used. With a fixed block size, a data amount of (block size \* blocks) is transferred with a single operation. This process can get rejected if the total amount exceeds the transfer size the system can handle. : You are prompted for the number of blocks per read/write (If you press Enter, the default number of blocks is 20). : You are prompted for the number of repetitions (If you press Enter, the default number of repetitions is 1).

You can then select one of the following options:

Read data from tape (to run Read- only test) : Write data to tape (to run Write- only test) : Write/Read/Verify (to run Read and Write test)

ITDT runs the selected test. Then, it displays the transfer size and block size that is used for this test, the number of records read/written, and the total bytes transferred.

The equivalent ITDT scripting command is rwtest

CAUTION: The command is overwriting the content of the loaded cartridge! Any data is lost.

Note: The command is only supported for tape devices and requires a loaded cartridge.

# [27] Read or Write Files

When you select the Read or Write Files command [27]:

: You are prompted to specify the file name of the source file (for Write test) or the destination file (for Read test). : You are prompted for the number of records to be read.

You can then select one of the following options:

Read File from Tape: ITDT reads a file from tape and stores data into a file with the specified file name. : Write File to Tape: ITDT reads data from file with the specified file name and writes data to tape.

110 IBM Tape Device Drivers Installation and User's Guide

ITDT displays the number of records read/written, the transfer size, and the total bytes transferred.

The equivalent ITDT scripting commands are write and read

![](images/9a5857698aa22b323d8f61c09873c9b8981e43dbe0a1498adf7ad22f061d4f3d.jpg)

CAUTION: The command write is overwriting the content of the loaded cartridge! Any data is lost.

Note: The command is only supported for tape devices and requires a loaded cartridge.

# [28] Erase

When you select the Erase command [28], ITDT erases the loaded cartridge. All data on cartridge will be overwritten!

For more information, refer to the ITDT Scripting Command at "erase" on page 129.

![](images/7ed18bafc086d3d67cd320bcd9b05a3fe896d24b9c60603f8992a4dbb4a53f42.jpg)

CAUTION: The command is overwriting the content of the loaded cartridge! Any data is lost.

Note: The command is only supported for tape devices and requires a loaded cartridge.

# [29] Load Tape

When you select the Load Tape command [29], ITDT issues the load tape command. The equivalent ITDT scripting command is load

Note: The command is only supported for tape devices.

# [30] Unload Tape

When you select the Unload Tape command [30], ITDT issues the unload tape command. The equivalent ITDT scripting command is bsr

Note: The command is only supported for tape devices and requires a loaded cartridge.

# [31] Write File Marks

When you select the Write File Marks command [31]:

1. You are prompted for the number of file marks to write. 
2. ITDT writes the number of file marks to the current position.

The equivalent ITDT scripting command is weof

![](images/c34b5bb853a0b3b8b95cf34f64d12fae49e1114bebb9b25227aeb347c3ee561c.jpg)

CAUTION: The command is overwriting the content of the loaded cartridge! Any data is lost.

Note: The command is only supported for tape devices and requires a loaded cartridge.

# [32] Synchronize Buffers

When you select the Synchronize Buffers command [32], ITDT flushes the buffer to the cartridge. The equivalent ITDT scripting command is sync

![](images/e8d20bdc16b9a799aa99407c9eb3447019578f12c26c2b79a882d345b83519f6.jpg)

CAUTION: The command is overwriting the content of the loaded cartridge! Any data is lost.

Note: The command is only supported for tape devices and requires a loaded cartridge.

# [33] Query/Set Parameter

When you select the Query/Set Parameter command [33]:

1. ITDT displays non-changeable parameters. Note: The list of non-changeable parameters is operating system specific.

2. ITDT displays changeable parameters.

Note: The list of changeable parameters is operating system specific. For a list of changeable parameters, refer to Table 15 on page 137.

3. You are prompted for parameter to change.

4. ITDT requests prompt for parameter value (if required).

5. ITDT requests safety prompt (if required).

6. ITDT issues the ioctl command.

The equivalent ITDT scripting command querying the parameters is getparms

The equivalent ITDT scripting command setting a parameter is setparm

# [34] Query/Set Tape Position

When you select the Query/Set Tape Position command [34]:

1. ITDT displays the current position  
2. You are prompted for a new position to set.  
3. This subcommand issues the SCSI Locate command to the device to set the tape position.

The equivalent ITDT scripting command querying the parameters is setpos

Note: The command is only supported for tape devices and requires a loaded cartridge.

# [35] Query Encryption Status

When you select the Query Encryption Status command [35]:

ITDT queries the tape drive encryption settings and displays the encryption state.

The equivalent ITDT scripting command is gyrencryption

Note: The command is only supported for tape devices.

# [36] Display Message

When you select the Display Message command [36]:

1. You are prompted for the text of message 0 (8 characters or less).  
2. You are prompted for the text of message 1 (8 characters or less).  
3. You are prompted for message type (msg0, msg1, flash0, flash1, alt).  
4. ITDT displays a message on the display panel.

Not all drives have a display. The 3592 drive is the only one that has display message capability. It is the only one with a display that is more than one character long. Eight is the limit of the characters on a display screen.

The equivalent ITDT scripting command is display

Note: The command is only supported for tape devices.

# [37] Report Density Supp (Support)

When you select the Report Density Support command [37]:

1. ITDT prints report status text for all supported media.  
2. ITDT issues a report density command to retrieve all supported media.

3. ITDT prints all requested reports. Data is printed in a decoded way. Scroll the screen to print each one of the following information:

Density name Assigning organization Description Primary density code Secondary density code Write OK Duplicate Default Bits per MM Media Width Tracks Capacity (megabytes).

4. ITDT prints report status text for current media 
5. ITDT issues a report density command to retrieve current media 
6. ITDT prints report data in a decoded way. The equivalent ITDT scripting command is density

Note: The command is only supported for tape devices and requires a loaded cartridge.

# [38] Test Encryp. Path (Test Encryption Key Path/Setup)

When you select the Test Encryption Key Path/Setup command [38]:

Note: Not supported for the HP- UX operating system.

[38] Test Encryp. Path (Test Encryption Key Path/Setup)When you select the Test Encryption Key Path/Setup command [38]:Note: Not supported for the HP- UX operating system.1. ITDT prints status message that server configuration and connections are tested2. ITDT issues the Encryption Diagnostics ioctl command, Ping Diag3. ITDT prints number of servers available or error message4. ITDT issues the Encryption Diagnostics ioctl command, Basic Encryption Diag5. ITDT prints completion code or error message6. ITDT issues the Encryption Diagnostics ioctl command, Full Encryption Diag7. ITDT prints completion code or error messageThe equivalent ITDT scripting command is ekmtest

# [50] Element Information

[50] Element InformationWhen you select the Element Information command [50]:1. ITDT issues the mode sense command

2. ITDT displays the

Number of transport elements First transport element address Number of storage slots First storage slot address Number of I/O elements First I/O element address Number of drives First drive address

The equivalent ITDT scripting command is elementinfo

Note: The command is only supported for tape library devices.

# [51] Position to Element

When you select the Position to Element command [51]:

1. You are prompted for destination address2. This subcommand issues the SCSI Position to Element command by using the destination address specified.

The equivalent ITDT scripting command is position

Note: The command is only supported for tape library devices.

# [52] Element Inventory

When you select the Element Inventory command [52]:

This subcommand issues the SCSI Read Element Status command for each element type and displays the element status information

Note: ITDT displays decoded element inventory information. Type n followed by Return to show the next page of information.

The equivalent ITDT scripting command is inventory

Note: The command is only supported for tape library devices.

# [53] Exchange Medium

When you select the Exchange Medium command [53]:

1. You are prompted for source address.2. You are prompted for first destination address.3. You are prompted for second destination address.4. ITDT issues the 2 SCSI MOVE commands to exchange the cartridges.

The equivalent ITDT scripting command is exchange

Note: The command is only supported for tape library devices.

# [54] Move Medium

When you select the Move Medium command [54]:

1. You are prompted for source address.2. You are prompted for destination address.3. ITDT issues the MOVE command to move the cartridge from source address to destination address.

The equivalent ITDT scripting command is move

114 IBM Tape Device Drivers Installation and User's Guide

Note: The command is only supported for tape library devices.

# [55] Initialize Element Status

When you select the Initialize Element Status command [55]:

1. ITDT prints the command summary.  
2. ITDT issues the SCSI INITIALIZE ELEMENT STATUS command. The equivalent ITDT scripting command is audit

Note: The command is only supported for tape library devices.

# [56] Prevent/Allow Medium Removal

When you select the Prevent/Allow Medium Removal command [56]:

1. You are prompted to select (1) for Prevent Removal, or (0) for Allow Removal. The equivalent ITDT scripting command is prevent and allow

Note: The command is only supported for tape library devices.

# [57] Initialize Element Status Range

When you select the Initialize Element Status Range command [57]:

1. You are prompted for the first slot address.  
2. You are prompted for the number of slots.  
3. TDT issues the SCSI INITIALIZE ELEMENT STATUS WITH RANGE command.

The equivalent ITDT scripting command is audit

Note: The command is only supported for tape library devices.

# [58] Read Device IDs

When you select the Read Device IDs command [58], ITDT retrieves the device ID information for all available drives and displays the information. Type n followed by Enter to show the next page.

The equivalent ITDT scripting command is devoid

Note: The command is only supported for tape library devices.

# [59] Read Cartridge Location

When you select the Read Cartridge Location command [59]:

1. You are prompted for the address of the first slot.  
2. You are prompted for number of elements.  
3. ITDT verifies that the specified address range is valid, otherwise prints error message and exit.  
4. ITDT issues the Read Cartridge Location ioctl command.  
5. ITDT issues the Element Info ioctl command.  
6. ITDT verifies that the address range is valid; otherwise, print the error message and exit.  
7. If no slots are found in Element Info data, print the error message and exit.  
8. ITDT issues the Read Cartridge Location ioctl command.  
9. ITDT prints decoded storage element information, Type n followed by Enter to show next page.

The equivalent ITDT scripting command is cartridgelocation

Note: The command is only supported for tape library devices.

# [70]Dump/Force Dump/Dump

When you select the Dump/Force Dump/Dump command [70]:

ITDT retrieves the dump. ITDT issues the Force Dump command. ITDT retrieves second dump. ITDT displays the name of stored dump files and the output directory where they are stored. The dump file names start with the serial number of the device.

The equivalent ITDT scripting command is dump

Note: The command is only supported for tape devices.

# [71] Firmware Update

# Note:

The following site is available for the latest firmware files: https://www.ibm.com/support/fixcentral.

1. Press the button Select Product 
2. Product Group: System Storage 
3. Tape systems 
4. Tape drives or Tape auto loaders and libraries 
5. Select your product accordingly 
6. Press Continue 
7. Select the related fix and download the file(s).

When you select the Firmware Update command [71]:

1. ITDT displays the firmware update dialog:

If the downloaded firmware file is listed in the Content field of the Firmware Update screen, type the corresponding line number and press Enter. If the firmware file is stored in a directory other than FW Dir, type F followed by a space and the fully qualified path to the directory that contains the firmware file, then press Enter.

For example, enter the following to change the firmware directory (UNIX):

f /home/user/firmware

If no files are displayed in the Content field, check the Dir OK field on the right side of the screen. It indicates true if the directory exists, false otherwise.

If the content of the displayed FW Dir changed, type D and press Enter to refresh the directory content.

Note: The selected file name is reset to the first item (#0) after the Refresh function is used.

If the displayed directory contains more files than the files shown, type  $^+$  and press Enter to scroll down the list. For fast down scrolling type  $^+$  followed by a space and the number of lines to scroll down then press Enter. To scroll back, use - instead of  $^+$

Scrollable data is indicated by "VVVVVVVVVVVVVVVV".

2. After the firmware file is selected, type C and press Enter to continue.

3. Before the firmware update is started, make sure the file that is displayed in the FW File field is the correct file.

If the correct file is displayed, proceed to the next step. If the correct file is not displayed, type C and press Enter to change the selected firmware file. Go to Step 4. Note: The selected file name is reset to the first item in the list when you return to that dialog from the Start Update dialog.

4. If you decide to run the firmware update, type S and press Enter to start the firmware update.

During the firmware update, a firmware update progress screen is displayed.

Attention: Once started, do not interrupt the firmware update.

The firmware update usually takes 3- 5 minutes, but it can take up to 45 minutes for libraries. If you decide not to run the firmware update, type R and press Enter to return to the Device List.

Note: If ITDT- SE detects FIPS- certified drive firmware, it displays a warning dialog. Before you continue, ensure that you use FIPS- certified firmware to update the drive.

5. After completion, the Status field on the lower right side indicates PASSED if the firmware was updated successfully and FAILED otherwise.

Type R and press Enter to return to the Device List.

# Add device manually

On the Start screen, press the [A] key followed by Enter to add a drive and a changer manually. In a dialog box, the tape device and the associated changer device can be defined. Supported formats are: devicename and H B T L. For a standalone tape drive the changer device is not required.

A default tape device is set and stepping through the device names the values can be altered.

When Enter is pressed, those drives which are accessible are opened. The manually added devices are shown in the same way as they are shown after an ITDT Scan command.

# Standard Edition - Program options

For problem determination and customization, ITDT is providing the following command line options.

- -help Prints help information 
-version Displays the version of ITDT, 
-used configuration files and creation dates. 
-force-generic-dd the usage of the generic. Operating System driver 
-not using the IBM Tape Device driver) will be forced. 
-LP logpath Use 'logpath' as logging path (default: output) 
-L logfile Use 'logfile' for log messages (default: metro.log) 
-LL Errors|Warnings|Information|Debug Set log level (default: Error) 
-R resultdir Use 'resultdir' as result file path (default: output) 
- settings Change default values of ITDT configuration parameters. 
-start deviceaddress 
-end deviceaddress Set range for device scan function, devicename and H B T L is supported.

# Standard Edition - Tapeutil scripting commands

Scripting is enabled with the 4.0 release of ITDT SE. ITDT- SE provides compatibility with earlier versions for existing tapeutil scripts. While some legacy commands exist, they are not documented in their entirety as they are phased out over time. New scripts must always use the scripts that are listed in this guide, the Common Command set (CS). Also, existing scripts must be modified for forward compatibility with ITDT.

You can find a list of commands, on each command you find the command, a description, parameter list, and which platforms are supported. Some commands have numbers after them. The numbers mean that a corresponding menu command is in "Standard Edition - Tapeutil menu commands" on page 106.

The following are the generic invocation parameters as in use by the legacy command sets: - f filename

Note: "filename" is a device special file for the drive/changer, for example:

/dev/mt0 (AIX), /dev/mt/ost (Solaris), /dev/mt/om (HP- UX), /dev/IBMtapee (Linux), \\\\tape0 (Windows)

The calling convention for the Common Command set is itdt - f filename [Open Mode] Subcommand [Subcommand ...]

Note: "filename" is a device special file for the drive/changer or the device address (host bus target lun). For a complete list of the file name or address syntax, refer the section "Special Files" on each platform or go to "Device file names - device addressing" on page 106.

The calling convention for the Common Command set is itdt - f filename [Open Mode] Subcommand [Subcommand ...]

The Open Mode flag is supported on all platforms. If the flag is not set, the device is opened in read/write mode. More parameters that might be required for opening the device are automatically detected and set.

- w mode  Open mode, by default Read/Write.  Valid modes are:  1 = Read/Write  2 = Read Only  3 = Write Only  4 = Append

Note: Always use the Read Only mode when you are working with write- protected media.

The new command set enables legacy commands on every platform, even if that is not previously supported by Tapeutil. The output follows current Tapeutil conventions. But, if different output displays for a single command on various platforms, the output is implemented according to the AIX output as the primary scripting platform.

Tapeutil allows undocumented abbreviations for some of the commands. For example, it was possible to shorten "inquiry" to "inq" or "inqu" or "inqui". The following command abbreviations are supported by ITDT- SE too: inq(uiry), req(sense), pos(ition), ele(mentinfo), iny(entory), devid(s), cartridge(location). Deprecated commands are listed at "Deprecated commands" on page 150. Also, there is a list of unsupported commands and known exceptions at "Standard Edition scripting commands: known limitations and deviations" on page 152.

The following commands are described.

General commands

- "allow" on page 121  
- "devinfo" on page 121  
- "inquiry | inq | inqj" on page 121  
- "logpage | logpagej" on page 121

loop" on page 122 "modepage | modepagej" on page 123 prevent" on page 123 print" on page 123 qrypath" on page 123 qryversion" on page 124 release" on page 124 reqsense" on page 124 reserve" on page 124 scanscan" on page 125 sleep" on page 127 "tur" on page 127 vpd" on page 127

Tape commands "append" on page 128 "bsf" on page 128 "bsr" on page 128 "channelcalibration" on page 128 "chgpart" on page 128 "density" on page 129 "display" on page 129 "erase" on page 129 "formattape" on page 129 "fdp" on page 130 "fdpl" on page 130 "fsf" on page 130 "fsr" on page 130 "getparms" on page 131 "idp" on page 131 "idpl" on page 131 "list" on page 132 "load" on page 132 "logsense" on page 132 "qrypar | qrypart" on page 132 "qrylbp" on page 132 "qrypos" on page 133 "qrytcpip" on page 133 "read" on page 134 "resetdrive" on page 134 "rmp" on page 134 "runtimeinfo | qryruntimeinfo" on page 135 "rewind" on page 135 "rtest" on page 135

- "rwtest" on page 135- "sdp" on page 136- "sdpl" on page 136- "seod" on page 136- "setparm" on page 136- "setpos" on page 137- "settcpip" on page 138- "sync" on page 139- "unload" on page 139- "vertbp" on page 139- "weof" on page 139- "write" on page 140- "wtest" on page 140

Medium Changer Subcommands

- "audit" on page 140- "cartridgelocation|cartridgelocationj" on page 141- "elementinfo|elementinfoj" on page 141- "exchange" on page 141- "inventory | inv | inventoryj | invj" on page 141- "librarymediaoptimization | lmo" on page 142- "move" on page 142- "position" on page 142

Service Aid commands

- "dump" on page 143- "ekmtest" on page 143- "encryption" on page 143- "ucode" on page 143- "tapephcp" on page 143- "ltfsphcp" on page 144- "verify" on page 145- "devicestatistics" on page 145- "checkltfsreadiness" on page 145- "ltfsdefragmentation" on page 145- "standardtest" on page 146- "fullwrite" on page 146- "systemtest" on page 147- "tapeusage" on page 147Library RESTful commands- "TS4500 REST over SCSI" on page 147- "TS4300: RESTful API" on page 148

# allow

allow(Deprecated: unlock, - o rem) Allow medium removal for tape or changer devices (unlock door). The counter command for this is "prevent" on page 123. Parameters:

Parameters:

Supported platforms: All

# devinfo

devinfo(Deprecated: - o gdi) Show device information (device type, sub type and block size)

Parameters:

None

Supported platforms: All

# inquiry / inq / inqj

inquiry / inq / inqjinquiry or inq command identifies the device and returns the output in hexadecimal format. inqj command is similar to inquiry or inq command but generates the output in JSON format.

Parameters:

[Page code in Hex, 00- FF without leading x]

Following is an example of an output from inq Ox83 and inquiry with JSON output inqj Ox83 command:

sudo ./itdt - f /dev/sg3 inq Ox83 inqj Ox83 Issuing inquiry for page Ox83. . Inquiry Page Ox83, Length 74 0 1 2 3 4 5 6 7 8 9 A B C D E F 0123456789ABCDEF 0000 - 0183 0046 0201 0022 4942 4D20 2020 2020 [...F..."IBM 0010 - 554C 5433 3538 302D 5444 3920 2020 2020 [ULT3580- TD9 0020 - 3139 3133 3030 3033 3736 0103 0008 5005 [1013000376. ..P. 0030 - 0763 1219 A083 0194 0004 0000 0001 0193 [...c. . . . . . . . . . . . . . . . . . . . . . . . . . . . . . . . . . . . . . . . . . . . . . . . . . . . . . . . . . . . . . . . . . . . . . . . . . . . . . . . . . . . . . . . . . . . . . . . . . . . . .. . . . . . . . . . . . . . . . . . . . . . . . . . . . . . . . . . . . . . . . . . . . . . . . . . . . . . . . . . . . . . . . . . . . . . . . . . . . . . . . . . . . . . . . . . . . . . . . . . . . ... . . . . . . . . . . . . . . . . . . . . . . . . . . . . . . . . . . . . . . . . . . . . . . . . . . . . . . . . . . . . . . . . . . . . . . . . . . . . . . . . . . . . . . . . . . . . . . . . . . . 1 "command": "inquiry" "parameter": 1 "pageCode": 131, "evpd": true 1 "length": 96 "data": 1 "pageCode": 131, "vendorIdentification": "IBM "productIdentification": "ULT3580- TD9 "serialNumberDrive": "1013000376" "worldWideNodeName": "500507631219A083" "relativeTargetPort": "0001" "worldWidePortName": "500507631259A083" 3

Supported platforms: All

# logpage / logpagej

logpage / logpagej(Deprecated: - o log) This subcommand issues the SCSI Log Sense command to the device for the specified page and displays the log sense data. The generated output is in hexadecimal format. logpagej command is similar to logpage command but generates the output in JSON format.

# Parameters:

Page Logpage (in hex without leading x) [Subpage] Subpage (in hex without leading x)

Following is an example of an output from logpage and logpagej command:

sudo ./itdt - f H1- B0- T1- L0 logpage logpagej Issuing log sense for page Ox00. Log Sense Page 6x00, Length 28 0123456789ABCDEF 0000 - 0000 0018 0002 0306 0C11 1214 1617 1A1B 0010 - 2E30 3132 3334 3738 3938 3C3D [.01234789;<= ] "command": "log sense" "parameter": "pageCode": 0 "subPage": 0 "length": 255 "data": "subPage": "00: "page": "00: Supported Pages" "page": "02: Write Error Counters" "page": "03: Read Error Counters" "page": "06: Non- Medium Errors" "page": "0C: Sequential Access Device" "page": "11: DT Device Status" "page": "12: Tape Alert Response" "page": "14: Device Statistics" "page": "16: Tape Diagnostic Data" "page": "17: Volume Statistics" "page": "1A: Power Condition Transitions" "page": "1B: Data Compression" "page": "2E: Tape Alert" "page": "30: Tape Usage Log" "page": "31: Tape Capacity" "page": "32: Data Compression" "page": "33: Write Errors" "page": "34: Read Forward Errors" "page": "37: Performance Characteristics" "page": "38: Blocks/Bytes Transferred" "page": "39: Host Port 0 Interface Errors" "page": "3B: Host Port 1 Interface Errors" "page": "3C: Drive Usage Information" "page": "3D: Subsystem Statistics"

Following is an example of an output from logpagej command:

# 1ogpage

Supported platforms: All

# loop

This subcommand loops all subsequent subcommands continuously or a number of times if the Count parameter is specified. Also refer to the sleep subcommand.

# Parameters:

loop [Count]

Supported platforms: All

# modepage [modepagej

(Deprecated: - o mod) This subcommand issues the SCSI Mode Sense command to the device for the specified page and displays the mode sense data.modepagej command is similar to modepage command but generates the output in JSON format.

Parameters:

pagecode |subpagecode]

Example mode page - page 0x30 and sub- page 01:

sudo ./itdt - f /dev/sg3 modepagej 0x30 0x01 1 "command": "mode sense" "parameter": 1 "pageCode": 48 "subPage": 1 3 "length": 255 "data": 1 "subPageCode": "01" "addressDescriptorLength": "000C" "numberOfDescriptors": "01" "drivePortMacAddress": "00 01 00 21 5E D3 88 50" 3

Supported platforms: All (Deprecated: - o lck, lock) Prevent medium removal for tape or changer devices (lock door). The counter command for this command is "allow" on page 121.

# prevent

Parameters:

![](images/7babcc5309d3a97b25b91c34c78bfd2a6184000472aea2843091de7bf0dc1f9c.jpg)

Supported platforms: All

# print

This subcommand prints the associated text to standard output. It can be used at any time to display the progress of the subcommands.

Parameters:print Text

Supported platforms: All

# qryhba | qryhbainfo

The qryhba | qryhbainfo command issues a set of unique properties like HBA vendor, HBA type, and driver version for the current host bus adapter. The number of properties displayed depends on the operating system and HBA vendor.

Parameters:

![](images/19f10e73dc445281acfcea9e1b341b29b13b9b6282fbf3b46b93118c5a084773.jpg)

Supported platforms: All, except IBM System i

# qrypath

This subcommand displays information about the device and SCSI paths, such as logical parent, SCSI IDs, and the status of the SCSI paths for the primary path and all alternate paths that are configured.

# Parameters:

None

Supported platforms: All

Note: only supported for devices that are using the IBM Tape Device Driver.

Deprecated: - o phs, path, checkpoint

Example:

![](images/d027d6747a8b27a3bc3220589a25c70ca27d6c203fb55a8907e6918fcc4319fb.jpg)

Note: ITDT shows the entire path information for all the commands.

# qryversion

(qryversion)(Deprecated: - o driv) This subcommand prints out the current version of the IBM Tape Device device driver.

Parameters:

Supported platforms: All release(Deprecated: - o rel) This subcommand explicitly releases a device and makes it available for other hosts by issuing the SCSI Release command.

# release

Parameters:

![](images/f03e90c2bf602aa5e3be77df3858267387434d8b720b546043921f1a199dea3b.jpg)

Supported platforms: All reqsense(Deprecated: - o req) This subcommand issues the SCSI Request Sense command to the device and displays the sense data in hex format.

# reqsense

Parameters:

![](images/41f5fec6e3d69af211d50a821a79ade46f696b6ab4ea082490ad1a83c7f676f7.jpg)

Supported platforms: All reserve(Deprecated: - o res) This subcommand explicitly reserves a device by issuing the SCSI Reserve command.

# reserve

Parameters:

Supported platforms: All

# scan|scanj

scan command scans the system for connected tape and changer devices, and displays the list of connected devices. For each detected device following information will be reported. scanj command is similar to scan command but generates the output in JSON format.

Special file name Vendor ID- Firmware version- Serial- number- SCSI bus address- Associated changer- Device driver name

# Parameters:

![](images/146afb0691bb3336893576ab59123dfdd60d337d63cbf185124e7691b8fdf1e0.jpg)

Following is an example of an output from scan command:

. /itdt scan Scanning SCSI Bus #0 /dev/IBMchange0 - [3573- TL TS4300]- [1600] S/N:55L3A7801CLMLL01 H1- B0- T2- L1 (IBM- Device) #1 /dev/IBMtapen - [ULT3580- TD8]- [S11F] S/N:1175B3E05B H1- B0- T2- LO Changer:55L3A7801CLMLL01 (IBMDevice) #2 /dev/IBMtapen2n - [ULT3580- TD9]- [S1GF] S/N:1013000376 H1- B0- T1- LO (IBM- Device) #3 /dev/IBMtapen1n - [ULT3580- HH8]- [Q385] S/N:1175B3E06F H1- B0- T0- LO Changer:55L3A7801CLMLL01 (IBMDevice) #4 /dev/IBMtapen0n - [ULT3580- TD9]- [M61F] S/N:1013000284 H2- B0- T24- LO (IBM- Device) Exit with code: 0

Following is an example of an output from scanj command:

# ./itdt scanj [ "index": 0 "driverName": "/dev/IBMchanger0" "modelName": "3573- TL TS4300" "firmwareVersion": "1600" "serialNumber": "55L3A7801CLMLL01" "hostAdapterId": 1 "busNumber": 0 "targetId": 2 "lunId": 1 "deviceSpecialFile": "IBM" "index": 1 "driverName": "/dev/IBMtape3n" "modelName": "ULT3580- TD8" "firmwareVersion": "S11F" "serialNumber": "1175B3E05B" "hostAdapterId": 1 "busNumber": 0 "targetId": 2 "lunId": 0 "library": "55L3A7801CLMLL01" "deviceSpecialFile": "IBM" "index": 2 "driverName": "/dev/IBMtape2n" "modelName": "ULT3580- TD9" "firmwareVersion": "S1GF" "serialNumber": "1013000376" "hostAdapterId": 1 "busNumber": 0 "targetId": 2 "lunId": 0 "deviceSpecialFile": "IBM" "index": 3 "driverName": "/dev/IBMtape1n" "modelName": "ULT3580- HH8" "firmwareVersion": "Q385" "serialNumber": "1175B3E06F" "hostAdapterId": 1 "busNumber": 0 "targetId": 0 "lunId": 0 "library": "55L3A7801CLMLL01" "deviceSpecialFile": "IBM" "index": 4 "driverName": "/dev/IBMtape0n" "modelName": "ULT3580- TD9" "firmwareVersion": "M61F" "serialNumber": "1013000284" "hostAdapterId": 2 "busNumber": 0 "targetId": 24 "lunId": 0 "deviceSpecialFile": "IBM"

By using the optional parameter - force- generic- dd, the usage of the generic Operating System driver (not with the IBM Tape Device Driver) is forced.

With the parameters - start, - end, and - exclude, the scan can be limited to only a particular HBA or target ID range (partial scan). It starts at - start=device to search for supported devices until - end=device is reached. Devices that are specified with - exclude are skipped during the scan. Multiple excluded devices must be separated by colons. Supported formats are devicename and H B T L.

The format string controls the output and specifies how the connected devices must be reported. It can include any alphanumeric character. The default format string is  $\% D - [\% P] - [\% F]S / N:\% S H$ $\% H - B\% B - T\% T - L\% L$

The following list contains all the interpreted identifiers:

<table><tr><td>%D device name</td></tr><tr><td>%V vendor name</td></tr><tr><td>%F product name</td></tr><tr><td>%F firmware version</td></tr><tr><td>%S serial number</td></tr><tr><td>%H host adapter</td></tr><tr><td>%B bus number</td></tr><tr><td>%T target id</td></tr><tr><td>%L logical unit</td></tr><tr><td>%h host adapter as hexadecimal value</td></tr><tr><td>%b bus number as hexadecimal value</td></tr><tr><td>%t target id as hexadecimal value</td></tr><tr><td>%l logical unit as hexadecimal value</td></tr><tr><td>%I interface type</td></tr><tr><td>%# enumeration number</td></tr><tr><td>%C serial number of associated changer device</td></tr></table>

Any combination of the identifiers that are listed here are supported.

An integer that is placed between a  $\%$  sign and the format command acts as a minimum field width specifier. A negative value uses right text alignment.

Following is an example of scan command with format string:

<table><tr><td># ./itdt scan -o %-2#. %-10S %-15P %F %I&quot; 
Scanning SCSI bus . . .</td></tr><tr><td>0. 55L3A7801CLMLLO1 3573-TL TS4300 1600 N/A</td></tr><tr><td>1. 1175B3E05B ULT3580-TD8 S11F FC</td></tr><tr><td>2. 1013000376 ULT3580-TD9 S1GF FC</td></tr><tr><td>3. 1175B3E06F ULT3580-HH8 Q385 FC</td></tr><tr><td>4. 1013000284 ULT3580-TD9 M61F SAS</td></tr><tr><td>Exit with code: 0</td></tr></table>

The option - showallpaths forces ITDT to display all available data paths that are detected during the device scan. The default device serial number- based filtering is not performed.

Supported platforms: All sleep for the specified number of seconds before running the next subcommand.

# sleep

Parameters:

Supported platforms: All tur (Deprecated: - o tur) This subcommand issues the SCSI Test Unit Ready command to the device. Parameters:

# tur

None

Supported platforms: All vpd This subcommand displays Vital Product Data (VPD) that are part of the Inquiry command data and outputs Manufacturer, Product Identification and Revision Level.

# vpd

Parameters:

None

Supported platforms: All

# append

Opens the device in append mode. The file access permission is Write Only.

Parameters:

None

Supported platforms: All, but on Windows this open mode is not supported by the IBM Tape Device Driver. On HP- UX this open mode is remapped to  $\pm /\mathsf{w}$  by the IBM Tape Device Driver.

# bsf

(Deprecated: - o bsf) This subcommand backward spaces Count filemarks. The tape is positioned on the beginning of the last block of the previous file. An optional Count can be specified. The default is 1.

Parameters:

Note: The command is only supported for tape devices and requires a loaded cartridge.

Supported platforms: All

# bsr

(Deprecated: - o bsr) This subcommand backward spaces Count records. An optional count can be specified. The default is 1.

Parameters:

[count]

Note: The command is only supported for tape devices and requires a loaded cartridge.

Supported platforms: All This subcommand issues a self diagnostic command for drive servo channel calibration. The channel calibration returns PASSED or FAILED regarding the result of the command.

# channelcalibration

Parameters:

None

Note: The command is only supported for tape devices and requires a loaded cartridge.

Supported platforms: All

# chgpart

This subcommand changes the current active tape partition to a new partition specified by Number. Optionally, a Blockid can also be specified. If Blockid is omitted, the tape is positioned at the start of the new partition. Otherwise, the tape is positioned at the Blockid specified.

To query the current cartridge partitions, refer to the command "qrypar | qrypart" on page 132

Parameters:

Number [Blockid]

Note: The command is only supported for tape devices and requires a loaded cartridge.

Supported platforms: All

# density

density(Deprecated: - o gdn / - o rds) This subcommand issues the SCSI Report Density Support command for all supported media and for the current media loaded in the drive, and displays the results. If the drive is not loaded, the current media density is not reported.Parameters:

Parameters:

None

Note: ITDT- SE outputs detailed information on all platforms.

Supported platforms: All Display(Deprecated: - o msg) This subcommand displays a message on the display panel of the tape device. Up to 16 characters can be used for the message. If the message is longer than eight characters, the display alternates between the first eight characters and the remainder of the message.Parameters:

# display

Parameters:

"message1" "message2"

Supported platforms: All erase(Deprecated: - o era) This subcommand writes EOD at the current position and erases the tape from EOD to the end of current partition.Parameters:

# erase

Parameters:

[- short]

Supported platforms: All except i5/OS operating system

Note: The erase command triggers a long erase of the cartridge that sets EOD to the current position. Then, it writes the Data Set Separator (DSS) pattern from the new EOD to the end of the current partition. This process overwrites any data that is on the cartridge after the current logical position. To remove the entire cartridge, the user must remove all partitions (use the rmp command for LTO 5, TS1140, and newer drives). Then, issue the rewind command before the erase command.Examples:For LTO 5 / TS1140 and later

Examples:

For LTO 5/TS1140 and later

./itdt - f <device name> load rmp rewind erase

For all earlier LTO and Enterprise drive generations

./itdt - f <device name> load rewind erase

![](images/50ab9830e2472240f4a86d54984170bffd0b31284128f087cce055dfabb7284f.jpg)

CAUTION: The command is overwriting the content of the loaded cartridge, causing any data to be lost.

Note: The command is only supported for tape devices and requires a loaded cartridge.

# formattape

This command formats the media on the device.

Parameters:

[- immed] This parameter immediately returns the status of the device. [- verify] This parameter helps the device to run a verification after media is formatted.

![](images/915396a645dc2ced10a960f8900cfb607e0a981531cf238ba6dfe79ac223c95e.jpg)

CAUTION: The command is overwriting the content of the loaded cartridge, causing any data to be lost.

Note: The command is only supported for tape devices and requires a loaded cartridge.

Supported platforms: All

# fdp

fdpThis subcommand creates fdp (fixed data partitions) wrap wise. The default size for LTO 5 of partition 0 in this case is 1425 GB and the size of partition 1 is 37.5 GB. It also works for TS1140 but the size depends on the used cartridge. Supported by LTO 5, TS1140, and later.Parameters:

Parameters:

None

CAUTION: The command is overwriting the content of the loaded cartridge, causing any data to be lost.

Note: The command is only supported for tape devices and requires a loaded cartridge.

Supported platforms: All

# fdpl

fdplThis subcommand creates fdp (fixed data partitions) longitudinal. The command fdpl is valid only for TS1140 and later drives and creates partitions 0 and 1 on the cartridge. The size depends on the used cartridge.Parameters:

Parameters:

None

Supported platforms: All

CAUTION: The command is overwriting the content of the loaded cartridge, causing any data to be lost.

Note: The command is only supported for tape devices and requires a loaded cartridge.

# fsf

fsf(Deprecated: - o fsf) This subcommand forward spaces count filemarks. The tape is positioned on the first block of the next file. An optional count can be specified. The default is 1. Parameters:

Parameters:

Note: The command is only supported for tape devices and requires a loaded cartridge.

Supported platforms: All

fsr(Deprecated: - o fsr) This subcommand forward spaces count records. An optional count can be specified. The default is 1. Parameters:

Parameters:

Note: The command is only supported for tape devices and requires a loaded cartridge.

Supported platforms: All

# getparms

(Deprecated: - o parms / status / - o gpa) Get and show drive, media and driver parameters. Parameters:

None

Supported platforms: All

# idp

This subcommand creates initiator defined partitions (IDP) wrap wise on tape. The parameter pSize0 is used to specify the size of partition 0 and the parameter pSize1 is used to specify the size of partition 1. One of pSize0 or pSize1 must have a value that is entered in hex matching  $37.5 * n$  with  $(1 < = n < = 38)$  to specify the wanted size of that partition. The other parameter of pSize0 or pSize1 must have the value 0xFFFF to specify that the remaining capacity is used for that partition. If 0xFFFF is not used for one of the parameters, pSize0 or pSize1, the drive might reject the command | unless pSize0 and pSize1 exactly match predefined allowable values.

For TS1140 and later drives (not LTO), the parameters pSize2 and pSize3 are valid and they follow the same rules as described earlier.

For example: If you want a 37.5 GB partition (the minimum size partition) in partition 0 and the remainder in partition 1, then set pSize 0 to  $0\times 26$  and pSize1 to 0xFFFF. This action results in a volume with a 37.5 GB sized partition 0 and a 1425 GB sized partition 1.

# Parameters:

idp pSize0 pSize1  pSize0: size of partition 0  pSize1: size of partition 1

Example Call:

idp 0x26 0xffff

CAUTION: The command is overwriting the content of the loaded cartridge, causing any data to be lost.

Note: The command is only supported for tape devices and requires a loaded cartridge.

Supported platforms: All

# idpl

This subcommand creates initiator defined partitions (IDP) longitudinal wise on tape. The parameter pSize0 is used to specify the size of partition 0 and the parameter pSize1 is used to specify the size of partition 1. One of pSize0 or pSize1 must have a value that is entered in hex matching  $37.5 * n$  with  $(1 < = n < = 38)$  to specify the wanted size of that partition. The other parameter of pSize0 or pSize1 must have

For TS1140 and later drives (not LTO), the parameters pSize2 and pSize3 are valid and they follow the same rules as described earlier.

For example: If you want a 37.5 GB partition (the minimum size partition) in partition 0 and the remainder in partition 1, then set pSize 0 to  $0\times 26$  and pSize1 to 0xFFFF. This action results in a volume with a 37.5 GB sized partition 0 and a 1425 GB sized partition 1.

# Parameters:

pSize0: size of partition 0  pSize1: size of partition 1

Example Call:

idp 0x26 0xffff

CAUTION: The command is overwriting the content of the loaded cartridge, causing any data to be lost.

Note: The command is only supported for tape devices and requires a loaded cartridge.

Supported platforms: All

# list

This subcommand displays the content of a tape. The output lists filemarks and the size of each record found on the tape until the end of data is reached. The output that is generated from this subcommand can be large depending on the amount of data on the tape and must usually be redirected to a file.

Parameters:

list [- start blockid] [- count recordcount]

Note: The command is only supported for tape devices and requires a loaded cartridge.

Supported platforms: All

# load

(Deprecated: - o lod) This subcommand issues a Scsi Load command to load a tape. The subcommand loops all subsequent subcommands continuously or a number of times if the Count parameter is specified. Also refer to the sleep subcommand.

Parameters:

[- amu 011]

amu enables or disables archive mode unthread during Load command.

Supported platforms: All

# logsense

Retrieves all Log Sense pages and outputs them as hex.

Parameters:

<table><tr><td>None</td></tr></table>

Supported platforms: All

# qrypar I qrypart

Queries and displays tape partitioning information.

Parameters:

None

Supported platforms: All

Note: The command is only supported for tape devices and requires a loaded cartridge.

# qrylbp

Queries and displays logical block protection information.

132 IBM Tape Device Drivers Installation and User's Guide

# Parameters:

None

Supported platforms: All

# qrymon / qrymediaoptimizationneeded

Issuing the qrymon command prompts ITDT to run a query on the media auxiliary memory (MAM) of the cartridge and check whether media optimization is needed.

Parameters:

None

Note: The command is only supported for tape devices and requires a loaded cartridge.

Supported platforms: LTO- 9 only

# qrymov / qrymediaoptimizationversion

Issuing the qrymov command prompts ITDT to run a query on the media auxiliary memory (MAM) of the cartridge and display the version of media optimization.

Parameters:

![](images/b041d96b605a424939174162dadff48c9dc763aec359288a26d323a1e52f1155.jpg)

Supported platforms: LTO- 9 only

Note: The command is only supported for tape devices and requires a loaded cartridge.

# qrypos

(Deprecated: - 0 gpo) This subcommand displays the current tape position.

Parameters:

![](images/68f965d936877c6d0a23697ff043999d1862a72cf8c77664b5681160ae92325d.jpg)

Note: The command is only supported for tape devices and requires a loaded cartridge.

Supported platforms: All

# qrytcpip

This subcommand outputs the current drive TCP/IP configuration. Only supported with LTO 5, TS1140, and later drives. Outputs adapter and TCP/IP address information for IPv4 and IPv6 with address, port and subnet mask. For example:

sudo ./itdt - f /dev/IBMtape0 qrytcpip Initializing device... Reading current TCP/TP Configuration... Number of Port Descriptors 1 Port Descriptor for Port 1 DHCP 0 Number of Socket Descriptors 2 Socket:1 IPV4 9.155.49.101/23 Socket:2 IPV6 [1111:2222:3333:4444:5555:6666:7777:ABCD]/127 Active IP Addresses: IPv6:FE80:0000:0000:0000:0221:5EFF:FED3:B850 IPv6:1111:2222:3333:4444:5555:6666:7777:ABCD IPv4:169.254.0.3 IPv4:9.155.49.101 Result: PASSED Code:OK Exit with code: 0

Parameters:

Supported platforms: All

# qrytemp

Reads the thermal sensor values of a tape drive and writes the following output:

Thermal Value, Max Thermal Value, Fencing Threshold and Fencing Removal Threshold. All values are units of  $^\circ \mathrm{C}$

# Parameters:

None

# read

readThis subcommand reads a file, or a specified number of records, from the tape to the destination file name specified with the - d flag. If the optional count parameter is used, only the number of records that are specified with the - c flag are read unless a filemark is encountered before the number of specified records. If the count parameter is not used, all records up to the next filemark on tape are read.

# Parameters:

read - d Dest [- c Count]

Supported platforms: All readattrThis subcommand reads a Medium Auxiliary Memory (MAM) attribute from a cartridge to the destination file name that is specified with the - d flag. The partition parameter - p is a number in the range 0 - 3. The attribute parameter - a is the hexadecimal value of the identifier. All three parameters are required.

# readattr

# Parameters:

- p|0|1|2|3] 
-a Identifier 
-d DestinationPathFile

Supported platforms: All

Note: The command is only supported for tape devices and requires a loaded cartridge.

# resetdrive

This subcommand issues a Send Diagnostic command (Reset Drive subcommand) to reset the device.

# Parameters:

None

Supported platforms: All rmpRemove the current partitioning of the loaded cartridge.

# rmp

Parameters:

None

Supported platforms: All

CAUTION: The command is overwriting the content of the loaded cartridge, causing any data to be lost.

Note: The command is only supported for tape devices and requires a loaded cartridge.

# runtimeinfo / qryruntimeinfo

This subcommand is used to query the dynamic runtime information. Dynamic runtime information allows an initiator to set dynamic runtime attributes (DRA) about itself into a device server. The device server then associates those attributes to the I_T_L nexus and uses the information and associations for enhanced data collection and debugging.

Parameters:

None

Supported platforms: All (supported since LTO 5 and 3592 E07)

# rewind

(Deprecated: - o rew) Rewinds the tape.

Parameters:

None

Supported platforms: All

Note: The command is only supported for tape devices and requires a loaded cartridge.

# rtest

(Deprecated: - o rea) This subcommand runs a read test by reading a random data pattern from the tape and verifying that it matches the written data. The rtest subcommand can be used after the wtest subcommand to verify the data. An optional block size, count, and repetition can be specified with the - b, - c, and - r flags, respectively. If the block size is fixed, then the count specifies the number of blocks to read on each repetition. If the block size is zero (variable), then the count specifies the number of bytes to read on each repetition. The default is a block size of 10240, a count of 20 blocks, and a repetition of 1.

Parameters:

[- b Blocksize] [- c Count] [- r Repetition]

Note: The command is only supported for tape devices and requires a loaded cartridge.

Supported platforms: All

# rwtest

This subcommand runs a read and write test by writing a random data pattern on the tape, reading it, and verifying that it matches the written data. An optional block size, count, and repetition can be specified with the - b, - c, and - r flags, respectively. If the block size is fixed, then the count specifies the number of blocks to write on each repetition. A single transfer transmits (block size * count) bytes. The operation is rejected if the total amount exceeds the transfer size the system is capable of. If the block size is zero (variable), then the count specifies the number of bytes to write on each repetition. The default is a block size of 10240 bytes, a count of 20 blocks, and a repetition of 1.

In SCSI Generic mode, the actual transfer size equals to blocksize. In IBM Device Driver mode, the transfer size is blocksize * blocks per read/write. In either case, the blocksize must be equal to or less than the maximum Host Bus Adapter supported transfer size. The maximum supported transfer size for SCSI Generic is 1048576 Bytes and for IBM Device Driver is 500 MB.

Parameters:

Supported platforms: All

CAUTION: The command is overwriting the content of the loaded cartridge, causing any data to be lost.Note:

Note: The command is only supported for tape devices and requires a loaded cartridge.

# sdp

The command sdp creates SDP (Select Data Partitions) wrap wise on tape.

Parameters:

count

For LTO (5 and higher) only the values 0 and 1 are valid. Using value 6 as parameter leads to partition 0 with 1.5TB and partition 1 does not exist. Using value 1 as parameter leads to partition 0 with 750 GB and partition 1 with 712.5 GB. For TS1140 and later drives values 0, 1, 2 and 3 are valid. Using value 6 as parameter will create only one partition, value 1 creates two and so on. The sizes of the partitions are depending on the cartridge used in drive.

Supported platforms: All

Note: The command is only supported for tape devices and requires a loaded cartridge.

# sdpl

The command sdpl creates SDP (Select Data Partitions) longitudinal on tape.

Parameters:

sdpl count For TS1140 and later drives values 0 and 1 are valid. Using the value 0 as the parameter creates partition 0 only and the value 1 as the parameter creates partitions 0 and 1. The sizes of the partitions depends on the cartridge used in drive.

Supported platforms: All

Note: The command is only supported for tape devices and requires a loaded cartridge.

# seod

(Deprecated: - o eod) Spaces to end of data on the tape.

Parameters:

None

Supported platforms: All

Note: The command is only supported for tape devices and requires a loaded cartridge.

# setparm

(Deprecated: - o spa / valid / compress / nocompress / sili / nosili / autoload / noautoload / retain / noretain)

ITDT- SE uses the new setparm option that corresponds to the current interactive mode options.

Parameters: The value are

- 0-65535 for the blocksize parameter- 0-100 for the capacity parameter (=percentage)

136 IBM Tape Device Drivers Installation and User's Guide

- 1 (SCSI) and 2 (AIX) for recordspacemode- The Volume Id string for the valid parameter- NONE|ASSO|PERS|WORM for the writeprotect parameter- 0 for off/no and 1 for on/yes for setparm autoload, autodump, buffering, compression, immediate, readpastfilenameark, sili, simmim, trace, trailer, volumeloggingSupported platforms: All, but only a subset of the parameters is supported by the platform's device drivers.

Supported platforms: All, but only a subset of the parameters is supported by the platform's device drivers.

<table><tr><td colspan="6">Table 15. Supported platforms</td></tr><tr><td></td><td>Linux</td><td>Windows</td><td>AIX</td><td>Solaris</td><td>HP-UX</td></tr><tr><td>setparm autoload</td><td></td><td></td><td>X</td><td></td><td></td></tr><tr><td>setparm autocump</td><td>X</td><td></td><td></td><td></td><td></td></tr><tr><td>setparm blocksize</td><td>X</td><td>X</td><td>X</td><td>X</td><td>X</td></tr><tr><td>setparm buffering</td><td>X</td><td></td><td>X</td><td>X</td><td>X</td></tr><tr><td>setparm capacity</td><td>X</td><td>X³</td><td>X</td><td>X</td><td>X</td></tr><tr><td>setparm compression</td><td>X</td><td>X</td><td>X</td><td>X</td><td>X</td></tr><tr><td>setparm datasafemode¹</td><td>X</td><td>X</td><td>X</td><td>X</td><td>X</td></tr><tr><td>setparm immediate</td><td>X</td><td></td><td>X</td><td>X</td><td>X</td></tr><tr><td>setparm readpastfilemark</td><td>X</td><td></td><td></td><td></td><td></td></tr><tr><td>setparm recordspacemode</td><td></td><td></td><td>X</td><td></td><td></td></tr><tr><td>setparm sili</td><td></td><td></td><td>X</td><td>X</td><td>X</td></tr><tr><td>setparm simmim</td><td>X</td><td></td><td></td><td></td><td></td></tr><tr><td>setparm skipsync²</td><td>X</td><td>X</td><td>X</td><td>X</td><td>X</td></tr><tr><td>setparm sleepmode</td><td>X</td><td>X</td><td>X</td><td>X</td><td>X</td></tr><tr><td>setparm trace</td><td>X</td><td></td><td></td><td></td><td></td></tr><tr><td>setparm trailer</td><td>X</td><td></td><td>X</td><td>X</td><td>X</td></tr><tr><td>setparm void</td><td></td><td></td><td>X</td><td></td><td></td></tr><tr><td>setparm volumelogging</td><td>X</td><td></td><td>X</td><td></td><td></td></tr><tr><td>setparm writeprotect²</td><td>X</td><td>X³</td><td>X</td><td>X</td><td>X</td></tr><tr><td>setparm archivemodeunthread</td><td>X</td><td>X</td><td>X</td><td>X</td><td>X</td></tr></table>

# Note:

Note:1. The datasafemode can be set to YES or NO when no cartridge is loaded. When a cartridge is loaded, the datasafemode can be set only to YES.2. Depending on the support of the device.3. Only supported by 3592.

Note: The command is only supported for tape devices and requires partly a loaded cartridge.

# setpos

setpos(Deprecated: - o spo / asf) This subcommand issues the SCSI Locate command to the device to set the tape position. If the optional Blockid parameter is specified, the tape position is set to the Blockid.

Otherwise, if the **Blockid** parameter is omitted, the tape position is set to the last position saved by using the qrypos subcommand.Parameters:

Parameters:

Note: The command is only supported for tape devices and requires a loaded cartridge.

# settcpip

settcpipThis subcommand sets the drive ethernet port TCP/IP settings for LTO 5, TS1140, and later drives. Either a static IPv4 or IPv6 address can be set or DHCP enabled.

Example DHCP:

./itdt - f /dev/sg3 settcpip - p 1 - s 1 dhcp Initializing device... Setting TCP/TP Configuration... Reading current TCP/TP Configuration... Number of Port Descriptors 1 Port Descriptor for Port 1 DHCP 1 Number of Socket Descriptors 2 Socket:1 IPv4 9.155.49.101/23 Socket:2 IPv6 [1111:2222:3333:4444:5555:6666:7777:ABCD]/127 Active IP Addresses: IPv6:FE80:0000:0000:0000:0221:5EFF:FED3:B850 IPv6:1111:2222:3333:4444:5555:6666:7777:ABCD IPv4:169.254.0.3 IPv4:9.155.49.101 Result: PASSED Code:OK Exit with code:0

IPv4 or IPv6 addresses are entered in the syntax a.b.c.d/subnet_mask_length where a, b, c, and d are values with 1 to 3 digits. If the optional parameter subnet_mask_length is not specified, the current setting is kept.

Example static IPv4:

sudo ./itdt - f /dev/sg3 settcpip - p 1 - s 1 9.155.49.101/23 Initializing device... Setting TCP/TP Configuration... Reading current TCP/TP Configuration... Number of Port Descriptors 1 Port Descriptor for Port 1 DHCP 0 Number of Socket Descriptors 2 Socket:1 IPv4 9.155.49.101/23 Socket:2 IPv6 [1111:2222:3333:4444:5555:6666:7777:ABCD]/127 Active IP Addresses: IPv6:FE80:0000:0000:000o:0221:5EFF:FED3:B850 IPv6:1111:2222:3333:4444:5555:6666:7777:ABCD IPv4:169.254.0.3 IPv4:9.155.49.101 Result: PASSED Code:OK Exit with code:0

138 IBM Tape Device Drivers Installation and User's Guide

Example to reset an port /socket:

sudo ./itdt - ll c - f /dev/sg3 settcpip - p 1 - s 1 reset Initializing device... Setting TCP/TP Configuration... Reading current TCP/TP Configuration... Number of Port Descriptors 1 Port Descriptor for Port 1 DHCP 0 Number of Socket Descriptors 2 Socket 1 is not configured Socket:2 IPv6 [1111:2222:3333:4444:5555:6666:7777:ABCD]/127 Active IP Addresses: IPv6:FE80:0000:0000:0000:0221:5EFF:FED3:B850 IPv6:1111:2222.3333:4444:5555:6666:7777:ABCD IPv4:169.254.0.3

Result: PASSED Code: OK Exit with code: 0

# Parameters:

- p 1|2 
-s 1|2 ip-address/subnet_mask_length|dhcp|reset

Supported platforms: All

Note: With the current firmware level, the device can be reached (ping, FTP) only within the same subnet. For example, the sample is configured with a static IP address (9.155.49.101. The drive can be pinged only within the same subnet (9.155.101. xxx).

# sync

(Deprecated: - o syn) This subcommand synchronizes buffers/flushes the tape buffers to tape.

Parameters:

None

Supported platforms: All

Note: The command is only supported for tape devices and requires a loaded cartridge.

# unload

(Deprecated: - o off / offline / rewoffl) This subcommand rewinds and unloads the tape.

Parameters:

unload [- amu 0|1]

amu enables or disables archive mode unthread during Load command.

Supported platforms: All

Note: The command is only supported for tape devices.

# verlbp

This subcommand verifies logical block protection written blocks. The verification length can be set with parameter value filemarks count or with EOD.

Parameters:

filemarks (numeric value) | eod

Supported platforms: All

Note: The command is only supported for tape devices and requires a loaded cartridge.

# weof

(Deprecated: - o eof / eof) These subcommands write count filemarks. An optional count can be specified. The default is 1.

Parameters:

weof [Count]

Note: The weof parameter [count] is optional, if it is not supplied, one filemark is written.

Supported platforms: All

Note: The command is only supported for tape devices and requires a loaded cartridge.

# write

writeThis subcommand writes the source file specified with the - s flag on the tape. In case the parameter 'raw' is specified, the blocksize specified in setparm (setparm blocksize) is used instead of the default blocksize of 64 kB. The parameter count specifies the amount of blocks which should be written.Parameters:

Parameters:

[- raw] [- count] - s Source

Supported platforms: All

CAUTION: The command is overwriting the content of the loaded cartridge, causing any data to be lost.

Note: The command is only supported for tape devices and requires a loaded cartridge.

# writeattr

writeattrThis subcommand writes a Medium Auxiliary Memory (MAM) attribute from a cartridge from the source file name that is specified with the - s flag. The partition parameter - p is a number in the range 0 - 3. The attribute parameter - a is the hexadecimal value of the identifier. All three parameters are required.Parameters:

Parameters:

- p[0|1|2|3]-a Identifier-s SourcePathFile

Supported platforms: All

Note: The command is only supported for tape devices and requires a loaded cartridge.

# wtest

wtest(Deprecated: - wri) This subcommand runs a write test by writing a random data pattern on the tape. The wtest subcommand can be used after the wtest subcommand to verify the data that was written. An optional block size, count, and repetition can be specified with the - b, - c, and - x flags, respectively. If the block size is fixed, the count specifies the number of blocks to write on each repetition. If the block size is zero (variable), the count specifies the number of bytes to write on each repetition. The default is a block size of 10240, a count of 20 blocks, and a repetition of 1. Parameters:

Parameters:

[- b Blocksize] [- c Count] [- r Repetition]

Supported platforms: All

![](images/bd5b27b27276d4ed40695680cb487351ecdda2870960c6492d7b06321f3be050.jpg)

CAUTION: The command is overwriting the content of the loaded cartridge, causing any data to be lost.

Note: The command is only supported for tape devices and requires a loaded cartridge.

# audit

audit(Deprecated: - o aud / - o ier) This subcommand with no parameters issues the SCSI Initialize Element Status command to the device. Using the optional parameters Address and Count issues the SCSI Initialize Element Status With Range command to the device. The Address parameter specifies the starting element address and the Count parameter, if used, specifies the number of elements to initialize. If Count is omitted, it defaults to 1. Parameters:

Parameters:

[[Address] [Count]]

Supported platforms: All

Note: The command is only supported for tape library devices.

# cartridgelocation|cartridgelocationj

This subcommand with no parameters issues the SCSI Read Element Status command to the device to report all slots with the cartridge location information. Using the optional parameters Slot and Count issues the SCSI Read Element Status to the device for a specific starting Slot address and optionally the Count specifies the number of slots to return. If Count is omitted, it defaults to 1.

The command cartridgelocationjprints out the content in JSON format.

Parameters:

Supported platforms: All

Note: The command is only supported for tape library devices.

# elementinfo|elementinfoj

(Deprecated: - o ele) This subcommand displays element information (number and address) of each element type.

The command elementinfoj prints out the content in JSON format.

Parameters:

None

Supported platforms: All

Note: The command is only supported for tape library devices.

# exchange

(Deprecated: - o exh) This subcommand issues the SCSI Exchange Medium command to the device by using the Source, Dest1, and Dest2 addresses specified. This command runs the equivalent function of two Move Medium commands. The first moves the cartridge from the element address that is specified by the Dest1 parameter to the element address specified by the Dest2 parameter. The second moves the cartridge from the element address that is specified by the Source parameter to the element address specified by the Dest1 parameter.

Parameters:

exchange Source Dest1 Dest2

Supported platforms: All

Note: The command is only supported for tape library devices.

# inventory / inv / inventoryj / invj

(Deprecated: - o inv) The subcommand inventory | inv with no parameters issues the SCSI Read Element Status command for each element type and displays the element status information. If the optional - i parameter is used, then only the import/export element status information is returned. If the optional - v parameter is used, then only the element status information for the specified Valid if found is returned.

The command inventoryj | invj prints out the content in JSON format.

Parameters:

[- i | - v Valid]

Note: ITDT supports the optional parameters on all platforms.

Supported platforms: All

Note: The command is only supported for tape library devices.

# librarymediaoptimization / lmo

Starting with LTO- 9 the LTO tape drive will perform a media characterization/optimization process for each initially loaded cartridge.

Regarding the longer lasting optimization time, cartridges should be optimized proactively in the target environment.

The librarymediaoptimization subcommand for medium changers offers the option to identify uninitialized cartridges in a logical tape library and load cartridges in the desired tape drive(s) for optimization.

This subcommand with no parameters determines all tape drives and storage slots available in a logical tape library and use those for media optimization.

Already initialized cartridges will be skipped.

With the optional - start number parameter, the first storage element address can be specified used for and with optional parameter - slots numberofslots, the numbers of slots can be limited.

The optional parameter- drives numberofdrives specifies the numbers of drives and with the optional parameter - drivelist identifiers, a defined list of drives is used - identifiers is a comma separated list containing the drive element address or the drive serial number.

The optional parameter - volserlist identifiers allows the specification of a list of volser names where the media characterization/optimization should be performed, identifiers is a comma separated list containing the volser names. The characters \* and ? can be used as wildcard characters.

# Parameters:

[- start number] [- slots numberofslots] [- drives numberofdrives] [- drivelist identifiers][- volserlist identifiers]

# Examples:

./itdt - f /dev/IBMChanger0 librarymediaoptimization ./itdt - f /dev/IBMChanger0 librarymediaoptimization - slots 10 - drives 2 ./itdt - f /dev/sg7 librarymediaoptimization - start 4096 - slots 10 ./itdt - f /dev/sg7 librarymediaoptimization - drivelist 01234567890,0123456789A ./itdt - f /dev/sg7 librarymediaoptimization - drivelist 256,257 - volserlist 187AAHL9,187AAKL9,187AALL9

Supported platforms: All,

Note: The command is only supported for tape library devices.

# move

(Deprecated: - p mov) This subcommand issues the SCSI Move Medium command by using the source and destination addresses specified. The element addresses can be obtained with the element info subcommand.

Parameters:

Source Dest

Supported platforms: All

Note: The command is only supported for tape library devices.

# position

(Deprecated: - p pos) This subcommand issues the SCSI Position to Element command by using the destination specified.

# Parameters:

position Dest

Supported platforms: All

Note: The command is only supported for tape library devices.

# dump

Dump(Deprecated: - o sdp) This subcommand forces a dump of the tape drive and stores the dumps before and after the force dump in the ITDT- SE output folder with the ITDT- SE naming convention (serialnumber.a.gz and serialnumber.b.gz).

Parameters:

None

Supported platforms: All

# ekmtest

Test encryption key path/setup.

Parameters:

None

Supported platforms: AIX, Linux, Solaris, Windows

# encryption

Query tape drive encryption settings and display the encryption state.

Parameters:

![](images/b989617ab21a4759536702c650d83229f3ac5e514fa7654df81c15c5f42718c7.jpg)

Supported platforms: All

# ucode

(Deprecated: - o dmc) This subcommand downloads microcode to the device. The Filename is a file that contains the ucode(firmware).

The TS4300 (3555) tape library does not support code update with SCSI commands. ITDT integrated RESTFul API functions to support code update by using the HTTPS connection of the library. The process is similar to the regular code update sequence, but in addition the user must enter credentials during the code update sequence.

Parameters:

ucode [- password loginpassword - user username - ipaddress library Address] Filename

Supported platforms: All

# tapephcp

Creates a physical copy of a tape cartridge. The created cartridge has the same physical layout and contents as the origin cartridge. The amount of transferred data and the current data transfer rate is displayed every 3- 5 minutes. tapephcp is supported for LTO and 3592 tape drives and can therefore be used for data migration. A tapephcp command that is issued to copy data from a 3592 drive to an LTO drive or from an LTO Gen x to an LTO Gen y works, if the amount of used data from the source device is equal or less than the capacity of the destination device.

# Parameters:

[- cqs Memorysize] source destination

Source and destination can either a special device file name, or a tape image file name. The special device file format is identical to the format specified in chapter 3.19 parameter "- f device".

Examples: Tape to Tape copy with the IBM tape driver

./itdt tapephcp /dev/IBMtape0 /dev/IBMtape1 Tape to Image File using generic interface ./itdt tapephcp 3 2 1 0 /tmp/MyTapeImage.img Image File to Tape using IBM Tape driver ./itdt tapephcp /tmp/MyTapeImage.img /dev/IBMtape1 Tape to Tape copy with adjusted memory allocation ./itdt tapephcp - cqs 1000 /dev/IBMtape0 /dev/IBMtape1

To ensure maximum performance for tape to tape copy actions, tapephcp allocates a read buffer of 2500 MB (assuming the maximum system block size is 1 MB). If the system does not provide this buffer, the operation is aborted with MEMORY ALLOCATION FAILED. The value for memory allocation can be changed with an integer value for the parameter cqs (Command Queue Size). The cqs value must be multiplied with the maximum supported system blocksize to determine the size of memory that is allocated by ITDT. Supported copy operations are from tape to tape, from image file to tape, and from tape to image file. On UNIX operating systems, a warning to check the file size limit is shown before an image file is created, if the maximum file size is below 2 GB.

# ltfsphcp

Creates a physical copy of an LTFS formatted cartridge. ltfsphcp is based on tapephcp. The LTFS specified parameters volumeuuid and vCI are adjusted during this copy operation. The created cartridge has the same physical layout as the origin cartridge. Expect the volumeuuid to be identical to the contents of the two cartridges. The amount of transferred data and the current data transfer rate is displayed every 3- 5 minutes. When ltfsphcp is used with a non- LTFS formatted cartridge, the behavior of ltfsphcp is identical to tapephcp.

# Parameters:

[- cqs Memorysize] [- vOL1 volume id] source destination

Source and destination can either a special device file name, or a tape image file name. The special device file format is identical to the format specified in chapter 3.19 parameter "- f device".

# Examples:

Tape to Tape copy using IBM tape driver ./itdt ltfsphcp /dev/IBMtape0 /dev/IBMtape1 Tape to Image File using generic interface ./itdt ltfsphcp 3 2 1 0 /tmp/MyTapeImage.img Image File to Tape using IBM Tape driver ./itdt ltfsphcp /tmp/MyTapeImage.img /dev/IBMtape1 Tape to Tape copy with adjusted memory allocation ./itdt ltfsphcp - cqs 1000 /dev/IBMtape0 /dev/IBMtape1

To ensure maximum performance for tape to tape copy actions, ltfsphcp allocates a read buffer of 2500 MB (assuming the maximum system block size is 1 MB). If the system does not provide this buffer, the operation is aborted with MEMORY ALLOCATION FAILED. The value for memory allocation can be changed with an integer value for the parameter cqs (Command Queue Size). The cqs value must be multiplied with the maximum supported system blocksize to determine the size of memory that is allocated by ITDT. Supported copy operations are from Tape to tape, from image file to tape, and from tape to image file. On UNIX operating systems, a warning to check the file size limit is shown before an image file is created, if the maximum file size is below 2 GB.

For LTFS formatted cartridges, the volume identifier can be changed for a physical target with the parameter VOL1. The value must be alphanumeric with a minimum of 1 and a maximum of 6 characters.

# verify

VerifyVerifies the physical contents of two cartridges. The physical data layout and the binary data are compared.

# Parameters:

source destination

Source and destination can either a special device file name, or a tape image file name. The special device file format is identical to the format specified in chapter 3.19 parameter - f.

# Examples:

./itdt verify /dev/IBMtape0n /dev/IBMtape0n

# devicestatistics

This command collects relevant device parameters from several log pages and inquiry data and prints out in a table.

# Example for devicestatistics:

<table><tr><td colspan="5">sudo ./itdt -f /dev/IBMtapeOn devicestatistics</td></tr><tr><td colspan="5">Initializing device...</td></tr><tr><td colspan="5">Retrieving Log Pages...</td></tr><tr><td colspan="5">Device Statistics:</td></tr><tr><td>Lifetime Media Loads</td><td>103</td><td>Lifetime Cleaning Op.</td><td>0</td><td></td></tr><tr><td>Lifetime POH</td><td>1850</td><td>Lifetime MMH</td><td>16</td><td></td></tr><tr><td>Lt Meters Tape Processed</td><td>298950</td><td>Lt MMH Incompatib. Media</td><td>0</td><td></td></tr><tr><td>Lt POH Temperature Cond.</td><td>0</td><td>Lt POH Power Cons. Cond.</td><td>0</td><td></td></tr><tr><td>MMH Since Last Clean</td><td>16</td><td>MMH Since 2-last Clean</td><td>16</td><td></td></tr><tr><td>MMH Since 3-last Clean</td><td>16</td><td>Lt POH Forced Reset/Eject</td><td>0</td><td></td></tr><tr><td>Lifetime Power Cycles</td><td>12</td><td>Vol Loads Since Par Reset</td><td>3</td><td></td></tr><tr><td>Hard Write Errors</td><td>0</td><td>Hard Read Errors</td><td>0</td><td></td></tr><tr><td>Duty Cycle Sample Time</td><td>14672267</td><td>Read Duty Cycle</td><td>0</td><td></td></tr><tr><td>Write Duty Cycle</td><td>0</td><td>Activity Duty Cycle</td><td>0</td><td></td></tr><tr><td>Vol Not Present Duty Cycle</td><td>0</td><td>Ready Duty Cycle</td><td>99</td><td></td></tr><tr><td>Drive MFG SN</td><td>1013000284</td><td>Drive SN</td><td>1013000284</td><td></td></tr><tr><td>Medium Removal Prevented</td><td>0</td><td>Max Recom Mech Temp Exceed</td><td>0</td><td></td></tr><tr><td>Result: PASSED</td><td></td><td></td><td></td><td></td></tr><tr><td>Code: OK</td><td></td><td></td><td></td><td></td></tr><tr><td>Exit with code: 0</td><td></td><td></td><td></td><td></td></tr></table>

# checkltfsreadiness

This subcommand issues the LTFS Readiness Check test.

The LTFS Readiness Check analyzes the operating system and tape drive environment to ensure that the IBM Linear Tape file system can be installed. This test checks the Operating System version, the tape device driver version, the tape drive firmware, and the LTFS HBA requirements. LTFS Readiness Check requires an empty data cartridge.

# Parameters:

None

# ltfsdefragmentation

On an LTFS formatted cartridge, the physical data records for a single file can be fragmented across the entire media. When such a file is accessed, a long response time might result. The tape drive must locate to different cartridge positions to retrieve the entire contents the file. If the first data records of a file are stored at the end of the tape and the other records are stored at the beginning of the media, the tape drive must run several times intensive seek operations to fulfill the complete file retrieval. This subcommand creates a copy of the cartridge with unfragmented content.

As an initial step, ITDT stores the complete content of the source tape media in a Tape Image file that is on an HDD. Using this Tape Image file and the ITDT image file backend driver for LTFS, LTFS is able

to mount the previously created Tape Image file as a read- only volume. As the final step, the data is defragmented by copying the files from the mounted Tape Image file to the mounted destination cartridge. This algorithm avoids any seek operations on the physical tape device. The seek operations are completed on the temporary ITDT image file that is on a hard disk. The source and destination tape device are accessed at maximum media speed. The defragmentation of a cartridge can take up to 6 hours.

# Parameters:

source tempdirectory destination [options]

Supported platforms: Linux x86_64 only

Requirements: IBM LTFS SDE Version 2.2 (Build 4700 or later) and sufficient free hard disk space for temporary Tape Image file

Example:

./itdt ltfsdegragmentation /dev/IBMtape0 /tnm/tapeimages /dev/IBMtape1 - verbose - mkltfsoption  $\equiv$  - - force

# standardtest

The Test function (Scan Menu Command [T]) checks if the tape device is defective and outputs a pass/fail result. This test requires a loaded cartridge.

# Parameters:

Supported platforms: All

CAUTION: The command is overwriting the content of the loaded cartridge, causing any data to be lost.

Note: The command is only supported for tape devices and requires a loaded cartridge.

# fullwrite

The fullwrite command runs the ITDT full write test (Scan Menu Command [F]). Issuing a fullwrite command writes the entire cartridge with a given block size either with compressible or incompressible data and output performance data. This test requires a loaded cartridge.

# Parameters:

[- b Blocksize] [- compressible|- incompressible| 1.5compressible|- 2.5compressible|- 5.0compressible|- 100compressible] [- forcedataoverwrite][- percent number]

By using the optional parameters, such as - compressible, - incompressible, - 1.5compressible, - 2.5compressible, - 5.0compressible and - 100compressible, a data pattern with a predefined compressible ratio could be selected.

# Remember:

- The parameters 
-compressible and 
-2.5compressible are equivalent. 
- The 
-percent number can be set as a decimal point value ranging from 0.001 to 100 with up to 3 decimal places.

Supported platforms: All

CAUTION: The command is overwriting the content of the loaded cartridge, causing any data to be lost.

Note: The command is only supported for tape devices and requires a loaded cartridge.

# systemtest

Runs the ITDT Systemtest (Scan Menu Command [Y]).

The System Test is a short test that runs the following actions.

Reveals system performance bottlenecks. Compressible data throughput values can reveal bandwidth limitations that are caused by the system or cabling or HBA. Measures performance variations across the different block sizes to find the ideal block size for the system configuration.

This test requires a loaded cartridge.

Parameters:

[- forcedataoverwrite]

Supported platforms: All

CAUTION: The command is overwriting the content of the loaded cartridge, causing any data to be lost.

Note: The command is only supported for tape devices and requires a loaded cartridge.

# tapeusage

Displays the tapeusage information (Scan Menu Command [U]).

This test requires a loaded cartridge.

Parameters:

None

Supported platforms: All

# TS4500 REST over SCSI

The TS4500 supports a SCSI in band method for sending REST API commands and receiving HTTP responses that use SCSI Write Buffer and Read Buffer commands. The method is called REST over SCSI or RoS, for short. ITDT handles the RoS calls.

Syntax:

ros | rosraw GET | PATCH | POST url- endpoint [options] [- o|i filename]

Parameters:

- Use 'ros' for prettified output, 'rosraw' for raw mode (no indents, line breaks, Unicode filtering)- HTTP-Method: GET | PATCH | POST- Url-endpoint is the RoS method '/v1/...'- Options in brackets [] 
-string must be masked with single quotation marks ('')- -o filename where the output is stored.- -i filename for an input file like firmware file.

For a detailed URL endpoint definition, refer to the TS4500 RESTful documentation: https://www.ibm.com/docs/en/ts4500- tape- library/1.11.0?topic=reference- rest- api.

Example:

$\Phi$  ./itdt - f /dev/smc30 ros GET /v1/accessors HTTP/1.1 200 OK Content- Type: application/json Content- Length: 461 { "location": "accessor_Aa", "state": "onlineActive", "pivots": "13256",

"barCodeScans": "109653", "travelX": "9475", "travelY": "11640", "getsGripper1": "4488", "putsGripper1": "4487", "getsGripper2": "4486", "putsGripper2": "4465", "location": "accessor Ab", "state": "onlineActive", "pivots": "3496", "barCodeScans": "61212", "travelX": "2845", "travelY": "139", "getsGripper1": "160", "putsGripper1": "154", "getsGripper2": "163", "putsGripper2": "148", "fit with code: 0

# Endpoints with file transfers

The URL- endpoints

GET /v1/logs/[filename]/export GET /v1/library/saveConfig store the content in a file. The file name is printed at the end.

# Examples:

./itdt - f /dev/smc30 ros POST /v1/tasks "type": "inventoryTier0and1", "location": "library"3 ./itdt - f /dev/smc30 ros GET /v1/logs/TS4500_LOGA0004_20191106143648. zip/export ./itdt - f /dev/smc30 ros GET /v1/diagnostic0cartridges7DG 02000L4

# Notes:

1. 1. The IBM Tape Device Driver is sensitive to not ready conditions that can prevent the usage of RoS commands. For this reason, the generic operating system driver is recommended with RoS. For details, see "Generic Operating System driver with ITDT-SE" on page 82.

2. The Microsoft Windows command prompt does not support a single quotation mark () to send a string to an application. Every special character must be escaped. Example:

ros POST /v1/tasks "type": "inventoryTier0and1", "location": "library"3

works on Linux, but must be sent on Windows as:

ros POST /v1/tasks \\"type\"\"inventoryTier0and1\",\"location\"\"library\"3

# TS4300:RESTful API

The TS4300 REST API is a simple application planning interface (API) to manage the 3U scalable tape libraries remotely over an HTTPS interface. This API is requested and needed for manufacturing and for automated test and monitoring systems.

Syntax:

https- address {login- credentials} GET | PATCH | POST url- endpoint [JSON data]

# Parameters:

- https-address- Login credentials in JSON format- HTTP-Method: GET | PATCH | POST- url-endpoint- [JSON data]

# https-address

The IP address of the library.

# Login credentials

The library differentiates the following security levels:

Admin Security User Security Service Security

The user/password that is used to log in. Additionally, for some product variants in case of service level login the service password (service_password) and the administrator password (password) must be sent. In case the administrator password is not required the 'service_password' must be set through the normal password field. Parameter:

"username":"administrator", "password":"password" ["service_password":"servicepassword"]3

# http-method

The method for a dedicated request: GET, PUT, POST

# url-endpoint

Valid URLs in combination with the http method. If the parameter is required, it can be passed with ? and &.

For a detailed URL endpoint definition, refer to the Ts4300 RESTful documentation: https:// www.ibm.com/support/knowledgecenter/STAKK2/ts4300_kc/con_3U_REST_overview.html.

# JSON data

Some commands need extra data in JSON format like 'serialnumber change'.

Example:

sudo ./itdt https://1.2.3.4"username":"service","password":"123456"3GET /library/baseinfo BaseInfo 1 "SerialNumber":"1234567890123455", "MacAdress_1"："00:0e:11:1c:31:b5", "MacAdress_2"："00:0e:11:1c:31:b7", "Vendor": "IBM" "ProductID":"673- TL", "BaseFwRevision":"1.3.0.0- DO0", "BaseFwBuildDate":"07- 2019", "ExpansionwRevision":"00.30", "WWNodeName":"50000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000" "RobotichwRevision":"4", "RoboticFwRevision":"6.13", "RoboticSerialNumber":"12345678901234", "NoOfModules":"2", "LibraryType":"32" ModulesInfo":[ 1 "PhysicalNumber":"4", "LogicalNumber":"1", "ReadyStatus":"TRUE", "SerialNumber":"3555L3A1234567" 1 "PhysicalNumber":"5", "LogicalNumber":"2", "ReadyStatus":"TRUE", "SerialNumber":"3555E3A0234567" 1

] Exit with code: 0

# Notes:

1. ITDT sends a login request for each command.

2. Microsoft Windows command prompt does not support a single quotation mark (") to send a string to an application. That is, every special character must be escaped. The Microsoft Windows command shell is different and the command must be sent with the following form.

Example for Windows:

itdt.exe https://1.2.3.4 "\\\"username\\":\"service\\",\"password\\":\"123456\\"3" GET /library/走过ifex itdt.exe https://1.2.3.4 "\\\"username\\":\"service\\",\"password\\":\"123456\\"3" GET /library/getevents?EventType=Info&MaxNum=2"

# Deprecated commands

The following is a list of commands that are currently available in this version of ITDT. However, in a future release the following commands and some alternate calls of the Common Command Scripting set are no longer available and the scripts that contain these commands must be changed. The scripts that use the deprecated commands must be changed for future editions.

General commands - - o dis/disablepath - - o ena/enablenamefuser - kill - passthru - resetpath

Tape commands - bsfm - - o chk - fsfm - - o grs - list - - o gmi - qryinquiry - qrysense - - o ret - setblk - - o gds - getrecsize - setrecsize

Medium Changer subcommands - - o dvc - mount - demount Service Aid commands

- fmrtape- -o fdp- reset- -o qmc

# Alternative mount/demount script - sample for Windows

The script (load. drive. cmd) issues a mount/umount command to the specified library. If the drive is empty, a cartridge is moved from the first available storage slot to the drive. If the drive contains a cartridge, the cartridge is moved to the previous storage location.

Requirements:

Windows operating system- IBM tape library with one or more tape drives- IBM tape device driver or Generic SCSI driver- Path variable must include ITDT executable

Usage: load- drive.cmd drivernamelhbtl [drivenumber] drivernamel is the device driver name assigned by the IBM device driver hbtl is the associated SCSI address h=host, b=bus, T=target id and l=lun drivenumber, logical drive number, default is 1

Example for using the IBM device driver: load- drive.cmd \\\\.\Changer0

Example for using the SCSI Generic driver: load- drive.cmd "2 0 3 1"

load- drive.cmd: @ECHO OFF IF [%1]  $= = []$  echo echo usage load- drive.cmd drivernamelhbtl [drivenumber] echo echo drivernamel is the drivernamel assigned by the IBM device driver echo example:load- drive.cmd \\\\.\Changer0 echo hbtl is the associated SCSI address h=host, b=bus, T=target id and l=lun echo example: load- drive.cmd "3 0 2 1" echo. goto :EOF ) set drivename  $= \frac{1}{2}$  IF not  $[\% 2] = =[]$  set drivename  $= \% 2$  echo Loading/Unload Cartridge Drive:%driveNumbere% itdt - f  $\% = 1$  inventory >inventory.txt set /a count  $= 0$  set action  $=$  load set sourceAddress  $=$  set destination  $=$  set driveAddress  $=$  FOR /F "tokens  $= \frac{1}{2}$  1,2,3,4,5" %G IN (inventory.txt) DO ( call - checkline %G %H %T %J %K ) GOTO :moveMedium :checkline set name=%1 %2 if "%name"  $= =$  "Drive Address" ( set /a count  $+ = 1$  set currentTag  $= \%$  name% set drivename  $= \% 3$  ) if "%name"  $= =$  "Media Present" ( rem echo full drive addr:%driveAddress% slot addr:%slotAddress% action:%action% source:%sourceAddress% Tag:%currentTag% if "%count%"  $= =$  "%driveNumber%" if "%currentTag%"  $= =$  "Drive Address" if "%4"  $= =$  "Yes" ( set action  $=$  unload set sourceAddress  $= \%$  driveAddress%

if "%count%"=="%driveNumber%" if "%currentTag%"=="Drive Address" if not "%4"=="Yes" ( set action  $=$  load set destination=%driveAddress% if "%sourceAddress%"==" if "%currentTag%"=="Slot Address" if "%action%"=="load" if "%4"=="Yes" ( set sourceAddress  $=$  %slotAddress% ) ) if "%name"=="Source Element" ( if "%count%"=="%driveNumber%" if "%currentTag%"=="Drive Address" if "%action%"=="unload" ( set destination=%c ) if "%name"=="Slot Address" ( set currentTag=%name% set slotAddress=%4 ) GOTO:EOF :moveMedium echo %action% move media from %sourceAddress% to destination %destination% itdt - f %- 1 move %sourceAddress% %destination% del inventory.txt GOTO:EOF

# Standard Edition scripting commands: known limitations and deviations

The scripting commands idp, sdp, fdp, and qrypart are currently only supported by LTO 5, TS1140, and later drives.

When scripting, one must be aware of the following general deviations to the legacy tapeutil scripting command set.

The Verbose Mode parameter is ignored for the ITDT- SE implementation; instead always the full information is printed.

For some operations, the sense data length is limited to 32 bytes - this length is required by the ITDT test sequences.

The list command does not work on Linux variants where the reported maximum SCSI transfer size is larger than the size the system can actually transfer.

Because ITDT- SE opens the device read/write by default in scripting mode, the WORM mode cannot be deactivated in scripting mode. Use the Tapeutil interactive mode instead to deactivate the WORM mode.

Scripting mode command deviations to legacy tapeutil (tapeutil is not changed):

1. The erg (Erase Gap) command is not supported.  
2. The mtdevice command is not supported.  
3. The tell command is not supported.  
4. The seek command is not supported.  
5. The format command is not supported.  
6. The -o qsn command is not supported.  
7. path/quezypath/path / qrypath / checkpath / - o phs - output: always show all paths. The command is valid only in combination with an IBM device driver.  
8. devinfo - different output (decoded on all platforms)  
9. inquiry - different output on Linux (like AIX in hex)  
10. vpd - different behavior on Solaris (like AIX)

11. modepage - HP-UX and Solaris output deviations (like AIX)  
12. inventory - additional parameters on AIX available on all platforms  
13. cartridgelocation - AIX parameter deviation available on all platforms  
14. mediainfo - different output --> decode on all platforms

The command is valid only in combination with an IBM device driver. 15. setpos - logical versus physical position, only set logical position as on AIX 16. HPUx: - o gpo - t 1|2 - - > parameter - t1|2 (logical, physical) is not supported. 17. density - output all information on all platforms 18. setparm (new) - work around the inability to set all parameters on all platforms except the undocumented HP- UX release 19. getparms (new) - retrieve all parameters on all platforms, independent of flag set 20. qryinquiry - output the same as on AIX 21. logsense - does not output header with device/serial/volid, example:

05/12/09 15:10:44 Device: ULT3580- S/N: 1300000206 Volid: UNKNOWN

22. erase - does not work on i5/OS because of operating system limitation. 
23. Using -w x parameter without the necessary open leads to a core at least on Solaris.

# IBM Tape Diagnostic Tool - Graphical Edition

# Installing ITDT - Graphical Edition

# Installing ITDT-GE on Windows operating systems

To install ITDT- GE on Windows, download the executable file install_ITDT_GE_<version>.exe on a directory of your choice.

Double- click the downloaded file to start the installer application.

ITDT- GE installer automatically uninstalls any previous version before the current one is installed.

For the graphical user interface (GUI), a minimum screen resolution of 1024*768 pixels is required.

The supported Microsoft Windows operating systems for ITDT- GE are:

Microsoft Windows Server 2016 / 2012R2 (64- bit x64)

# Installing ITDT-GE on Linux operating systems

To install ITDT- GE on Linux, download the executable file install_ITDT_GE_<version>.bin to a directory of your choice.

install_ITDT_GE_<version>.bin must be run by a user with root access rights.

The ITDT- GE installer automatically uninstalls any previous version before the current one is installed.

For the graphical user interface (GUI) a minimum screen resolution of 1024*768 pixels is required. The supported Linux operating systems for ITDT- GE are

Linux Distributions with Kernel 2.6, glibc 2.2.5 and later (64- bit x64).

# Graphical Edition - known issues and limitations

This section describes the known issues and limitations of ITDT- GE.

# Linux operating systems

It is recommended to not operate Security Enhanced Linux (SELinux) in enforcing mode while ITDT- GE is running.

On Red Hat Enterprise Linux 4 and 5 and SuSE Enterprise Linux 10, starting the online help might cause an error message Problems opening link / Unable to open web browser on  $\{0\}$  Workarounds are to issue the commands

a) ln -s /usr/bin/firefox /usr/local/bin/mozilla or 
b) export MOZILLA_FIVE_HOME=/usr/lib/firefox<version>

Replace with the appropriate path to your installed Firefox version before ITDT- GE is started.

# Gnome desktop

If you are using the Gnome desktop, be sure to log in to the desktop session as root to use ITDT- GE to prevent device access issues.

# Linux GTK

ITDT- GE requires  $\tt GTK3+$  to run since version 9.4.0. On Linux systems with GTK2, ITDT can be installed or updated but does not start afterward.

# Windows operating systems

On Microsoft Windows systems where the maximum transfer size is limited to less than  $64~\mathrm{kB}$  , the Dump and Firmware update operations do not work.

# Performance issues

If you are using Adaptec SCSI Host Bus Adapters, ensure that you are using the latest Adaptec Host Bus Adapter Drivers instead of the drivers that are shipped with the Windows operating system.

# Devices disappear after firmware update

After a firmware update, devices might disappear. Repeated Scan operations might help to rediscover the device.

# All supported operating systems

# Command timeout

There is no instant operation termination upon ScSI command timeout. For example, when the SCSI cable is unplugged after POST A is started.

When a command timeout condition occurs, ITDT might still continue to run more operations (like unmounting the cartridge) instead of instantly terminating with a timeout condition.

# ITDT-GE user interface description

![](images/1adffb26aff03f868fa26c4ede6321e488200b0a4078add721bdabd2bea3e437.jpg)  
Figure 35. Graphical Edition user interface

To start ITDT- GE on Windows, click the shortcut that is created by the installation process. On Linux, no start menu entry is generated. Start ITDT- GE by opening a Terminal window, then switch to root user.

$\Phi$  su -

Finally, start ITDT- GE with

$\Phi$  /opt/ibm/itdt- ge/itdt- ge

The User Settings dialog box displays the first time that the program is run, allowing the entry of user specifications: User name, company name, output path, and log level.

The Output Path defines the location where the test reports and dumps are saved. The Windows default output path is

C:\Documents and Settings\<username>\.itdt- ge\output

or

C:\Users\<username>\.itdt- ge\output

where <username> is the Windows user login name. The Linux default output path is

/root/.itdt- ge/output

The Log Level must not be changed unless requested to do so by the IBM Support Center. It is recommended that this information be provided to allow for further analysis of test results.

![](images/f5041dc835ca2eff9bbd25ccfd799c29ed133bbf543412183752ee71b94d6d96.jpg)  
Figure 36. Graphical Edition preferences

![](images/d8454e3f8d98ecfddb128ddd788dc1182f7ff0de8ca6cec4212147dc4955495a.jpg)  
Figure 37. Graphical Edition preferences

The ITDT- GE interface contains the following sections:

![](images/509fa65466bc3b87c06a1733999c9396e1f4490170405585fe9f8af539329dea.jpg)  
Figure 38. Graphical Edition interface

- Main menu ( on Figure 38 on page 157)- in upper left (File, Window, Help) The following main program menu items are available:

![](images/4de94dfe6954d5ee5ab48f9072b851088e3952878454c3e9d05c1cc770cad7d5.jpg)  
Figure 39. Main program menu items

- Control Center (2 on Figure 38 on page 157)- On left side (Device operations 
- Scan, Test, Dump, and Update) Extra device operations are available by using drop-down arrows. Test Lab (3 on Figure 38 on page 157)- Located from center to right side (Contains running and previously run tests) Status Information (4 on Figure 38 on page 157)- Located below the Test Lab (Contains summary results of tests)

The Control Center is the primary section the ITDT- GE operations.

The following toolbar buttons for the device operations are available.

![](images/6e428aaae2a2652f4543577480de01f05d3ca65306dca858801f74f9e8679e21.jpg)  
Figure 40. Toolbar buttons

- Click the Scan menu item in the Control Center to display the list of tape devices found. When the scan is complete, select one device in the Test Lab by clicking the corresponding check box. Click the arrow next to the Scan menu item to add a device manually.

# - Test

Click the arrow next to the Test menu item to select an extended test.

# - Dump

Click the arrow next to the Dump menu item to select more log options.

# - Update

Click the Update menu item to start the firmware update. Click the arrow next to the Update menu item to select Online update options.

# - Config

Click the TCP/IP Port menu item to configure the TCP/IP port.

For each device on which the operation is started, a tab displays on the right panel (Test Lab area). The tab contains the operation information and status. Only one operation can be run at a time with ITDT- GE. The benefit in using tabs even for the single operation mode is that you get a history of operations as for each consecutive operation, a separate tab is opened.

# Graphical Edition - Scan menu commands

The following commands are described in this section:

# Scan

![](images/6cd582e0ffa461f61e1d5fb81ea1c9a60491c7cfb00e9e76f49bc8c9e8ef693d.jpg)  
Figure 41. Scan

The scan function is used to discover all supported tape and library devices that are attached to the computer system. Then, they can be selected for the subsequent ITDT- GE operations. The scan function also serves as a connection test that can be used to verify correct attachment of the devices.

Note: When Scan is pressed for the first time, a dialog box is displayed that warns the user to stop all backup jobs.

When the scan is finished, the device list is displayed in the Control Center area.

A scroll bar is available to show all the devices. When the device you want is displayed, select the device for test. Only one device can be selected.

# Specify Device Manually

The drop- down list next to Scan allows you to define a tape device and an associated changer device that must be used for testing. This function is a fast alternative to the Scan function where all Host Bus Adapters are discovered for supported tape and changer devices. The function supports the device name and HBTL addressing schema. For stand- alone tape devices the parameter **Changer Device** is not required.

Example device names:

Windows:  $\backslash \backslash$  .\tape0 Linux:/dev/IBMtape0

Example HBTL (all platforms):

H1- B0- T3- L0 oT1030

# Partial Scan

Partial ScanPartial Scan is used instead of Scan in cases when only a particular HBA or target ID range is used for device- discovering. The scan starts at "First device to scan" and checks for supported devices until "Last device to scan" is reached. Devices that are specified within "Exclude from scan" are skipped during the scan. If "First device to scan", "Last device to scan", or "Exclude from Scan" is not specified, ITDT performs a regular scan.

Partial Scan supports the device name and HBTL addressing schema.

Example device names:

![](images/592780ed592de802d9004d0acaa1e5b04c3500ee50975b8bef82251238cc9270.jpg)

Example HBTL (all platforms):

H1- B0- T3- L0 cr 1 0 3 0

# Health Test

![](images/9fdb2b54b490b6cadbfe7b45d81b237a3bafaf157d0808477c3c04c637f8fae3.jpg)  
Figure 42. Test

The Health Test function checks if the tape device is defective and outputs a pass/fail result.

# Attention:

1. The test functionality erases user data on the cartridge that is used for the test.  
2. The test can take from 15 minutes up to 2 hours.  
3. The test runs only on tape drives, not on autoloaders or libraries.

To start the Health Test function, it is recommended that a new or rarely used cartridge is used. Scaled (capacity- reduced) cartridges must not be used to test the device.

To test tape drives within a library, the library must be in online mode.

After the device is selected, start the Health Test function by selecting the Health Test menu item.

The Health Test function can be stopped by clicking Abort.

Note: It can take some time until the Health Test function stops.

![](images/bfae0d6818b5cf2adc119dbd035de055761943c856936c048f4ce041e5b0d3ae.jpg)  
Figure 43. Test results

Note: Information can be found in the.json/.blz files. See the Log Files section (1)

The Log Files section (1) contains the log files generated for this test. All log files can be opened in Graphical Log Visualization, or when double- clicked.

The test sequence contains the following steps:

1. Initialize Drive  
2. Read Thermal Sensor  
3. Mount Medium  
4. [Medium Qualification] – only if previous step indicated requirement  
5. Load/Write/Unload/Read/Verify  
6. POST A  
7. Performance Test (run 2 times if first run failed with performance failure)  
8. Unmount Medium  
9. Read Thermal Sensor  
10. Get FSC  
11. Get Logs

# Full Write

The Full Write function writes the entire cartridge with a specified block size either with compressible or incompressible data and output performance data.

# Attention:

1. The Full Write function erases data on the cartridge that is used for the test.  
2. The Full Write function takes several hours and up to 1 day when incompressible data is written, less time for compressible data.  
3. The amount of time is depended on the drive and cartridge that is used for.  
4. The Full Write function runs only on tape drives, not on autoloaders or libraries.

The Full Write test can be used to

- Demonstrate that the drive can write the full amount of data on a cartridge.- Reveal system performance bottlenecks.

Full Write runs the test with incompressible data first, then runs the test with compressible data. If no difference in overall performance is observed, a bandwidth limitation might be caused by the system or cabling or HBA.

- Identify system issues with compression.

Compression is always turned on during the full write. When run with compressible data, the output shows the compression rate. If it is higher than 1.0 but your system is not able to compress data on the cartridge, check your device driver and software settings to see whether they disable compression.

To run a full write on tape drives within a library, the library must be in online mode.

1. After the device you want to write to is selected, start the Full Write function by selecting Test > Full Write from the actions toolbar.

2. Click OK to start the full write.

3. ITDT-GE then shows the Full Write screen. If no cartridge is inserted, ITDT-GE prompts you to insert a cartridge. Either insert a cartridge and click OK or stop by clicking Abort.

Note: If ITDT- GE detects data on the cartridge, it shows the following message.

![](images/8096a021e5091222b929a7684b40b234ab0a5422941eb3dc39e0641808d08b13.jpg)  
Figure 44. Overwrite data?

Click Yes to continue the test if you are sure that data on the cartridge can be overwritten. If you are unsure, click No to stop the test.

4. The system detects the maximum transfer size in KiB and prompts for entry of a transfer size between the maximum value and 4 less values step down by 2. Select the appropriate value for your system.

![](images/199a805b1d37a68e09228696e0718ecb71419aa235b4c3526457d58dce8e80b6.jpg)  
Figure 45. Transfer size

5. Select the type of data to write.

5. Select the type of data to write.- Compressible 1.5x to write the amount of data from the cartridge with a compression factor of 1.5. This selection fills up the cartridge to about  $2 / 3$ .- Compressible 2.5x to write the amount of data from the cartridge with a compression factor of 2.5. This selection fills up the cartridge to about  $40\%$ .- Compressible 5.0x to write the amount of data from the cartridge with a compression factor of 5.0. This selection fills up the cartridge to about  $20\%$ .- Incompressible to write non-compressible data to the entire cartridge.

![](images/b3a5e828b2add42b4cbba148766716870b54475c3ce47acc2c824d23a0bc35ea.jpg)  
Figure 46. Type of data used?

The full write can be stopped by clicking Abort.

Note: It can take some time until the full write stops.

If all write operations are finished, ITDT- GE shows the performance statistics for the selected transfer size that is written on the cartridge, in the Status Information area. If an error occurred during the full write, data is only partially written.

# System Test

The System Test is a short test that runs the following tests:

Reveals system performance bottlenecks. Compressible data throughput values can reveal bandwidth limitations that are caused by the system or cabling or HBA. Measures performance variations across the different transfer sizes to find the ideal transfer size for the system configuration.

The System Test runs only on tape drives, not on autoloaders or libraries. To run a System Test on tape drives within a library, the library must be in online mode.

After the device you want to test is selected, start the System Test function by selecting Test > System Test from the actions toolbar.

ITDT- GE then shows the System Test screen. If no cartridge is inserted, ITDT- GE prompts you to insert a cartridge. Either insert a cartridge and click OK or stop by clicking Abort.

Note: If ITDT- GE detects data on the cartridge, it shows the following message -

Cartridge not empty! Overwrite data?

Click Yes to continue the test if you are sure that data on the cartridge can be overwritten. If you are unsure, click No to stop the test.

![](images/3e77ffe35b6ba09f17999551fc92e851e8255991a46444dd827aeaa3a98de95e.jpg)  
Figure 47. System Test

The System Test is run as follows:

1. System Test determines the amount of data to write for each supported transfer size (a percentage of the cartridge is written for each transfer size). 
2. The test determines the maximum supported transfer size of the system. 
3. System Test runs the test with incompressible data first, then runs the test with compressible data. 
4. System Test writes the amount of data with up to 5 different transfer sizes. It begins with the maximum supported transfer size and powers of two down to the next. First with incompressible, next with compressible data. Then, it shows performance data and a progress screen. 
5. At the end of the test, a summary information is shown.

# Library Diagnostic Self-Test

The Library Self- Test starts and monitors the library- internal self- test. This test runs only on libraries and autoloaders, not on tape drives.

After the device you want to test is selected, start the Library Self- Test function by selecting Test > Library Diagnostic Self- Test from the actions toolbar.

At the end of the test, the results are shown in the Status Information area.

# Library Media Screening

The Automated Library Media Screening test generates dumps for each drive and cartridge within a library. It runs only on libraries (except TS3500/TS4500/DiamondBack) and auto- loaders, not on tape drives.

First, the test tries to read dump files from each drive that is installed from the library. Then, you can select one drive for loading the cartridges.

![](images/39a7f78c7b64b5e4426c288ecbb38ea716a6d498c4dd87bf510f827ab729f684.jpg)  
Figure 48. Lib Media Screening

All cartridges of the I/O and storage slots are moved - one after the other from their source to the selected drive. A dump is taken and moved back to the source address. In the result screen, the dumps taken and the count of dumps are shown.

![](images/3f5672f47284e7e6d93d24f651a6fc7824fb74c733b944b1d5a2909142e9fdf9.jpg)  
Figure 49. Lib Media Screening Final

# Encryption

![](images/eab637e5534f02663c1a7dd10c0005bd2c6b5af292c0d9d5026b3562918f2132.jpg)  
Figure 50. Encryption

Note: The test that is shown on the graphic was run on a non- encrypted device and is showing a failure.

The Encryption function is used to verify whether data on the cartridge is written encrypted. It reads both decrypted and raw data from the cartridge into two separate files on disk. The user can then verify that the data differs to ensure that encryption worked.

The Encryption function is only supported on encryption enabled drives and requires an encryption infrastructure, including the Encryption Key Manager (EKM) to be properly set up.

1. After the device you want to test is selected, start the encryption function by selecting Test > Encryption Verification from the actions toolbar.  
2. ITDT-GE then shows the Encryption Verification screen. On this screen, the system requires the entry of the number of the start record and the amount of data (in KB) to be read.  
3. Enter the required values and click OK to start the encryption. The Encryption function can be stopped by clicking Abort.  Note: It can take some time until the Encryption function stops.

Table 13 on page 103 defines the abort codes.

# LTFS Readiness Check

The LTFS Readiness Check analyzes the Operating System and tape drive environment to ensure that the IBM Linear Tape File system can be installed. For extended information, refer to "Check LTFS Readiness" on page 101.

![](images/e510cc68899be1b3419baf227b991e68ae4cce861a3c55b2fb4d513f73c36f43.jpg)  
Figure 51. LTFS Readiness

# Eject Cartridge

The Eject Cartridge [J] function unloads a cartridge from a tape drive. To unload a loaded cartridge select the Test > Eject Cartridge from the actions toolbar. The command sends an PREVENT REMOVAL command in addition to the UNMOUNT.

![](images/8a0e5bb13f3a5a30dfd73da74b5a4f6fb5661b58d20e8d3aa132a308f890cf02.jpg)  
Figure 52. Eject Cartridge

# Configure TCP/IP Ports

For LTO 5, TS1140, and later drives, the Ethernet port settings can be configured with the Config Config TCP/IP Port. The command displays the current settings:

![](images/c65d9f0d1a6de21465bfe66cc4098272c7f1cb743bd539c45ae6b784351ea5b5.jpg)  
Figure 53. Configure TCP/IP Ports

Note: LTO- 5 drives and later have one port with 2 sockets and TS1140 and later drives have two ports and 4 sockets can be configured.

In the Current Configuration: table, the current configured values are displayed. For each Port/Socket combination, an entry is listed and can be altered.

After you selected an entry in the Current Configuration: table and press the Configure, the IP parameters dialog appears.

![](images/d621ca18f262eac729a79fa7e479ec01fb7528a1f14a9223cf795250509369b8.jpg)  
Figure 54. IP parameters

Changeable parameters:

- IP address: Either in IPv4 format (decimal values separated by periods) or in IPv6 format (hexadecimal values that are separated by colons).- Mask Length: Subnet Mask Length for IPv4 is between 0 and 23 and between 0 and 127 for IPv6.- DHCP_V4: Is set to 1 for requesting an IPv4 DHCP address.

Press Apply to apply any changes or press Cancel to return without any changes made. You can also press Reset to clear any data.

Note: Because earlier drive generations do not have an Ethernet port, the Configure TCP/IP Ports command is rejected for these devices with the following message:

TCP/IP configuration is not supported on this product.

# Dump

After the device you want to dump is selected, start the Dump function by selecting Dump > Dump from the actions toolbar.

When the dump process is run on a tape library or autoloader other than the 3584/TS3500/TS4500, the Dump function stores 1 log file in the output folder of the program (*.blz). For the 3584/TS3500/TS4500, a dump file (*.a) is stored in the output folder. Both files start with the serial number of the device (1).

When the Dump is selected for tape devices, up to 4 files were generated and listed in Log files (2)

<table><tr><td>Log file name example</td><td>Description</td></tr><tr><td>1013000376.005.dum</td><td>Bipassy.BIDDT log file (Binary Large Compressed) includes 2 dump files and device information. The file can be opened in Graphical Log Visualization or when double-clicked.
ITDT Name Structure: &lt;device number&gt;.&lt;index&gt;.dump.&lt;pass | fail|abort&gt;.blz</td></tr><tr><td>1013000376.005.dum</td><td>Binary Tape dump file before counter data was updated. The file can be opened in Graphical Log Visualization or when double-clicked.</td></tr><tr><td></td><td>ITDT Name Structure: &lt;device number&gt;.&lt;index&gt;.dump.a</td></tr><tr><td>1013000376.005.dum</td><td>Binary Tape dump file after counter data was updated. The file can be opened in Graphical Log Visualization or when double-clicked.
ITDT Name Structure: &lt;device number&gt;.&lt;index&gt;.dump.b</td></tr><tr><td>1013000376.005.dum</td><td>iTDA500g file in JSON (JavaScript Object Notation). The file can be opened in Graphical Log Visualization, when double-clicked or any other viewer.
ITDT Name Structure: &lt;device number&gt;.&lt;index&gt;.dump.&lt;pass|fail|abort&gt;.json</td></tr></table>

![](images/a83510e2eb2524bb44e31c41b4cde4c367e7b0101c8ff816dc5066b3a89a2075.jpg)  
Figure 55. Dump results

Note: When the memory dump function is run for tape libraries or autoloaders other than the 3584/TS3500/TS4500, the log file contains Log Sense and Mode Sense pages only. A Drive or 3584/TS3500/TS4500 dump contains more diagnostic information. (2)

# Firmware Update

The Firmware Update upgrades the firmware of tape drives and tape libraries.

The following site is available for the latest firmware files: https://www.ibm.com/support/fixcentral.

1. Press the button Select Product  
2. Product Group: System Storage  
3. Tape systems  
4. Tape drives or Tape auto loaders and libraries  
5. Select your product  
6. Press Continue  
7. Select the related fix and download the file(s).

To do a Firmware Update, run the following steps:

To do a Firmware Update, run the following steps:1. Select the device that you want to update.2. Select the **Update** menu item.3. A standard file dialog opens to select the path of where the firmware update is located. The path is either the default input path or the location of the last successfully opened firmware file.4. Press **OK** on this file dialog to start the update on the selected device.5. During the firmware update, a firmware update progress screen is displayed, indicating the progress of the update.

Attention: Once started, do not interrupt the firmware update.

![](images/979a7d2cd762f4b187f8080f8eaaa394539fc25395408965aaca2ad8aeef4a16.jpg)  
Figure 56. Firmware Update Screen

Note: If ITDT- GE detects a FIPS- certified drive firmware, it shows a warning dialog message. Before you continue, ensure that you use a FIPS- certified firmware to update the drive.

Note: For tape drives only: If the firmware file does not match with the device hardware version, a dialog is prompted.

The firmware update usually takes 3- 5 minutes, but it can take up to 45 minutes for libraries. After the firmware file is transferred and installed, the device will be rebooted.

![](images/e1901d6b6b7e3706feebbb12ca04608df27e052e956f887edc2f1e861cdf1257.jpg)  
Figure 57. Firmware Update Passed

Note: When the device is rediscovered, the new FW (firmware) is shown in the Info column of the Update Screen.

# Firmware Update - check for updates

ITDT- GE can Check for Device Updates for IBM tape drives and IBM tape libraries. You can choose to select either one device or all devices by selecting the corresponding option from the drop down menu under Update in the ITDT Control Center.

The program connects to IBM FixCentral and identify updates for the devices. If the connection cannot be established or another problem occurred, the problem description is shown at the bottom of "FixCentral" Test Tab.

![](images/6fbd9b2ae222d1ad300d432942c02a4a036340cfac4e0cdb1cac63dfac6a199a.jpg)  
Figure 58. Check for Device Updates - FixCentral components

In the sample above, the drive has two FixCentral components. Each component has several items; such as code files and textual meta information.

The code files (binary files) have a  $" + "$  in the icon to distinguish and can be downloaded either by doubleclicking or by using the right mouse button. The text files (for example, readme files) can be viewed in the same way. A separate Test Tab is opened and the information shown.

For tape drives an available code is colored:

GREEN: the code level is newer than the one of the drive.- RED: the code level is older than the one of the drive.- BLUE: the code level is the same as the one of the drive.

For automation drives (drives in a library), both devices are used. A code level for an automation drive is directly linked to a code level of a library.

![](images/ac6d75fe8036cc03d1b4d3c066f35fc0b2d68681379fc5e118d5e76d9e28bdd0.jpg)  
Figure 59. Check for Device Updates - code level

# Tape Usage

![](images/ec330da5e38e4472111441d20e07e854220aa1148ac5e6fe91e0978d168493b5.jpg)  
Figure 60. Tape Usage

The Tape Usage function retrieves statistical data and error counters from a cartridge.

1. After the device you want to test is selected, start the Tape Usage function by selecting Dump > Tape Usage Log from the actions toolbar.

2. ITDT-GE then shows the tape usage screen. If no cartridge is inserted, ITDT-GE prompts you to insert a cartridge. Either insert a cartridge and click OK or stop by clicking Abort.

# Manual Inspection Record Entry

A manual inspection record can be generated if the device does not show in the device list. This test is intended for devices that are not recognized or have a technical problem that cannot be determined by ITDT- GE.

If a tape drive cannot be identified by using a device scan, the user can manually create a test record for the drive. The system prompts the user to run the SCSI/FC Wrap test for the drive (see the service manual for the drive). The results of the wrap test can be entered along with extra inspection information. The results are saved into binary and text output files that have the same format as the output files generated by the test.

1. From the Main Program menu, select File > Manual Record. 
2. Enter the required information to complete the items in the screen.

a. Enter the device serial number. 
b. Enter the content of the Message Display. 
c. Optionally, enter any information text.

3. After all information is entered, click OK.

The information is stored in a binary file (which can be used for further analysis), and in a human- readable text file. Both files are stored in the ITDT- GE output folder.

# Graphical Edition - Copy Services

With Copy and Migration Services, tape content can either be copied or moved

- From a cartridge to another cartridge- From a cartridge to an image file, or- From an image file to a cartridge.

On UNIX operating systems, a warning to check the file size limit is shown before an image file is created, if the maximum file size is below 2 GB.

The tool offers different use cases like copying data or migrating data from one generation to another. That is, data can be copied from a Gen4 cartridge to a Gen5 cartridge. Even migration from an LTO to IBM Enterprise Tape Systems is supported. For an LTFS environment, the data can be copied or moved with the LTFS Physical copy that adjusts the LTFS parameter on the target to be unique. Such a copy can be used in the same LTFS environment as the source. To ensure maximum performance for tape to tape copy actions, ITDT allocates a read buffer that calculates to 2500 multiplied by the maximum supported system blocksize. If the system does not provide this buffer, the operation is aborted with MEMORY ALLOCATION FAILED. An infinite progress bar indicates the ongoing copy procedure. A dialog box confirms the completion.

# Tape Physical Copy

Tape Physical CopyCreates a physical copy of a tape cartridge either to another cartridge or to an image file. The created cartridge has the same physical layout and contents as the source cartridge. Tape Physical Copy supports LTO and 3592 tape drives and can therefore be used for data migration. A copy from a previous generation to a newer generation cartridge and from an LTO to a 3592 works. However, the amount of used data from the source device must be equal or less than the capacity of the destination device. If the source cartridge is partitioned, the target must support the same partitioning sizes and amount. The amount of transferred data and the current data transfer rate is displayed every 3 - 5 minutes.

# LTFS Physical Copy

LTFS Physical CopyCreates a physical copy of an LTFS formatted cartridge. The LTFS specified parameters volumeuuid and VCI are adjusted during this copy operation. The created cartridge has the same physical layout as the origin cartridge. Expect the volumeuuid to be identical to the contents of the two cartridges. The amount of transferred data and the current data transfer rate is displayed every 3 - 5 minutes. When 'LTFS Physical Copy' is used with a non- LTFS formatted cartridge, the behavior is identical to Tape Physical Copy. For LTFS formatted cartridges, the volume identifier can be changed for a physical target by specifying New LTFS Volume Label.

# Verify

VerifyVerifies the physical contents of two cartridges. The physical data layout and the binary data are compared.

# Usage

UsageBy opening the "Copy Services" perspective, the user can run a 'Scan...' to discover attached devices.

![](images/71f24a3aac44e977596dbc85f0150b9c33104694bb88b2b814850f51f5bc5a9c.jpg)  
Figure 61. Copy Services

A cartridge can be loaded or unloaded by double- clicking or pressing the right mouse button on the drive that is used for the copy tasks. When a cartridge is loaded, the cartridge Serial Number or the VOLSER in a library environment is shown. If a cartridge or an image file is selected on the left, it can be used as "Source" by pushing Select Source. After the 'Source" is selected, the target can be chosen. To select the target, either a different cartridge in a different drive or the image file folder can be selected. By pressing Select Target, the target is set on the right.

If the source and the target are selected, the Copy Services or a verification can be started regarding the used Mode: "Tape Physical Copy" on page 175, "LTFS Physical Copy" on page 176, or "Verify" on page 176.

# Graphical Edition - visualizing log files

ITDT- GE offers the opportunity to visualize the content of DUMP and ITDT log files (BLOB files).

Dump files can be retrieved with ITDT or any other tool that supports this function.

Dump files that are generated by ITDT have the suffix ".a" or ".b".- BLOB files are generated during the run of an ITDT test sequence, such as "Standard Test".- BLOB files have the suffix ".blz".- Both file types can be opened and visualized with ITDT- GE.

![](images/21cdfb697b85dc96628c841ef5493ece791801d25e8b69e59f551e1afd3a8926.jpg)  
Figure 62. Graphic Edition: log view

# Opening a Dump or BLOB file

A Dump or a BLOB file can be opened either with the menu command File/Open Log File or by switching to the Log Files perspective and pressing Open Log File .... In both cases, a File Selection dialog opens where files can be selected for display. After the successful opening of a file, the data is shown.

Two views are available for presenting the data - each at a separate panel.

- The Event List shows the events of this file on the left side. An event is a group of information and consists of 1 to n Elements shown on the right. By selecting one event list entry on the left, the corresponding data (event elements) is shown on the right.- The Report panel offers the opportunity to generate a sublist of the available 'Event List' entries.

# Graphical Edition - Tapeutil menu commands

After the initial startup, ITDT- GE shows three figures under the top menu. After the Tapeutil option is selected, the following page opens.

![](images/2d93d38ebc34c36c673f8e1d0a3486cb39545cd0a9c5052e009a0ae486f8708f.jpg)  
Figure 63. Tapeutil Control Center

On the left, the Tapeutil Control Center tree contains all Tapeutil commands for tape drives and tape libraries.

The commands from the General Commands and Service Aid Commands categories are duplicated into the two sections (to make the GUI navigation easier). After one category is expanded, the related commands open that allows users to select the command.

![](images/6f534b4a9fb6dee546e6edf90760d4e02e05040699854452a5dd14315f72f0bf.jpg)  
Figure 64. Command parameters

When the user presses Execute, the Results output is placed below the Parameter section:

![](images/8040811e52869d6fb99742b270c170518792932c543feecb1514e5c211a5a01e.jpg)  
Figure 65.Command parameter results

Figure 65. Command parameter resultsThe Open command has a Scan ... button in the Parameter view. Pressing Scan... runs a scan on the host and shows the attached devices in the Result View at the bottom. This information is helpful to identify the right Device Name for the open function field Device Name.

![](images/f7b64471c4dac98b9ee9ac84d018a47f19348f7f83781c7febc82e94ff5bab14.jpg)  
Figure 66. Generic tapeutil scan

![](images/682d609a480e541a2ffa3a9624804920ebc86898da674bad4ee03125759da4ad.jpg)  
Figure 67. DD tapeutil scan

This screen layout stays within the Tapeutil perspective until the program is closed. Outputs of subsequent operations are added to the Results field. Commands that fail are indicated with a red cross in the Status area. Commands that succeed are indicated with a green check mark. The status area can be cleared by pressing Clear.

General commands

- "Open" on page 181- "Close" on page 181- "Inquiry" on page 181- "Test Unit Ready" on page 182- "Reserve Device" on page 182- "Release Device" on page 182- "Request Sense" on page 182- "Log Sense" on page 182- "Mode Sense" on page 182- "Query Driver Version" on page 182- "Display All Paths" on page 182

Tape drive specific commands

- "Rewind" on page 182- "Forward Space Filemarks" on page 182- "Backward Space Filemarks" on page 183- "Servo Channel Calibration" on page 183- "Forward Space Records" on page 183- "Backward Space Records" on page 183- "Space to End of Data" on page 183- "Read and Write Tests" on page 183- "Read or Write Files" on page 185- "Erase" on page 186

- "Load Tape" on page 186- "Unload Tape" on page 186- "Write Filemarks" on page 186- "Synchronize Buffers" on page 186- "Query/Set Parameter" on page 186- "Query/Set Position" on page 186- "Query Encryption Status" on page 187- "Display Message" on page 187- "Display All Paths" on page 182- "Report Density Support" on page 187- "Test Encryption Path" on page 188

Tape library- specific commands

- "Element Information" on page 188- "Position to Element" on page 188- "Element Inventory" on page 189- "Exchange Medium" on page 189- "Move Medium" on page 189- "Initialize Element Status" on page 189- "Prevent/Allow Medium Removal" on page 189- "Initialize Element Status Range" on page 189- "Read Device IDs" on page 189- "Read Cartridge Location" on page 190

Service aid commands

- "Configure TCP/IP Ports" on page 190- "Dump/Force Dump/Dump" on page 190- "Firmware Update" on page 190

Note: When a command is issued in Tapeutil mode for ITDT GE, Execute must be pressed before the action takes place.

# Open

When you select the Open command:

1. ITDT checks if a device is already opened.  
2. Under Device Name:, enter the name of the device in the box.  
3. In the Open Mode menu, select how to open the device (rw, ro, wo, append).  
4. ITDT opens the device that you selected.

Note: Always use the Read Only mode when you are working with write- protected media.

# Close

When you select the Close command

1. ITDT checks if the device is already closed.  
2. ITDT closes the device.

# Inquiry

When you select the Inquiry command

1. You are prompted for page code.  
2. ITDT then displays a decoded format of a hexadecimal dump and prints a hexadecimal dump of the inquiry data.

# Test Unit Ready

When you select the Test Unit Ready (TUR) command, ITDT issues the Test Unit Ready ioctl command.

# Reserve Device

When you select the Reserve Device command, ITDT issues a reserve command for the device.

# Release Device

When you select the Release Device command, ITDT issues a release command for the device.

# Request Sense

When you select the Request Sense command

1. ITDT issues a Request Sense command.  
2. ITDT then displays a decoded format of hexadecimal dump sense data and prints hexadecimal dump sense data.

# Log Sense

When you select the Log Sense command

1. Enter the page number, in hexadecimal, in the Page-Code field.  
2. ITDT issues a Log Sense command and outputs a hexadecimal dump of that page.

# Mode Sense

When you select the Mode Sense command

1. Enter the page number, in hexadecimal, in the Page-Code field.  
2. ITDT issues a Mode Sense command and outputs a hexadecimal dump of that page.

# Query Driver Version

When you select the Query Driver Version command

1. ITDT issues the required command to get the driver version.  
2. ITDT prints the driver version.

# Display All Paths

When you select the Display All Paths command

1. ITDT issues an ioctl command.  
2. ITDT outputs decoded path information for all paths.

# Rewind

When you select the Rewind command, ITDT issues the ioctl rewind command for the device.

# Forward Space Filemarks

When you select the Forward Space Filemarks command

1. Enter the amount of filemarks to forward space, in the Filemark-Count box.  
2. ITDT issues (extrinsic) ioctl command.

182 IBM Tape Device Drivers Installation and User's Guide

3. The tape is positioned on the first block of the next file.

# Backward Space Filemarks

When you select the Backward Space Filemarks command

1. Enter the amount of filemarks to backward space, in the Filemark-Count box.  
2. ITDT issues (extrinsic) ioctl command.  
3. The tape is positioned on the last block of the previous file.

# Servo Channel Calibration

When you select the Servo Channel Calibration command:

1. ITDT issues the self diagnostic command for drive servo channel calibration.  
2. The channel calibration returns PASSED or FAILED regarding the result of the command.

# Forward Space Records

When you select the Forward Space Records command

1. Enter the amount of records to forward space, in the Record-Count box.  
2. ITDT issues (extrinsic) ioctl command.

# Backward Space Records

When you select the Backward Space Records command

1. Enter the amount of records to backward space, in the Record-Count box.  
2. ITDT issues (extrinsic) ioctl command.

# Space to End of Data

When you select the Space to End of Data (EOD) command, ITDT issues the (extrinsic) ioctl command.

# Read and Write Tests

When you select the Read and Write Tests command, ITDT runs the following functions (Read and Write Test, Read Only Test, and Write Only Test). Three parameter fields have default values already in them. Next, a Test menu that gives you the option of Read Data from Tape, Write Data to Tape, and Read/Write/Verify.

Note: The default is a block size of 10240 bytes, a count of 20 blocks, and a repetition of 1. If the block size is zero, variable mode is used. With a fixed block size, a data amount of (block size * blocks) is transferred with a single operation. This operation might get rejected if the total amount exceeds the transfer size of the system.

In SCSI Generic mode, the actual transfer size equals to blocksize. In IBM Device Driver mode, the transfer size is blocksize * blocks per read/write. In either case, the blocksize must be equal to or less than the maximum Host Bus Adapter supported transfer size. The maximum supported transfer size for SCSI Generic is 1048576 Bytes and for IBM Device Driver is 500 MB.

The following steps are run, depending on which test is selected.

- The Read/Write steps:

1. Issues a Read Position.  
2. Sets block size.  
3. Generates special pattern.  
4. Puts block id in bytes 0-3 of each block.  
5. Prints current block number, number of bytes and blocks.  
6. Issues write command.  
7. Prints updated statistics.  
8. If number of bytes written is different from requested bytes to write, stop (go to Step 19).  
9. Writes two filemarks.  
10. Backward spaces two filemarks.  
11. Backward spaces number of records written.  
12. Prints amount of data to read.  
13. Issues read command.  
14. If read error occurred or number of bytes read is different from requested number of bytes to read, go to Step 19.  
15. Compares data that is read with data written, show miscompares and if miscompares exist, stop (go to Step 19).  
16. If compare is OK, print OK message.  
17. Forward space one file mark.  
18. Repeat Steps 10 - 24 until all blocks are written, or go to Step 4 until all blocks are written.  
19. Prints current block id and total number of bytes written.

- The Read Only steps:

1. Issues a Read Position.  
2. Sets block size.  
3. Generates special pattern.  
4. Print amount of data to read.  
5. Issues read command.  
6. If read error occurred or number of bytes read is different from requested number of bytes to read, stop (go to Step 19).  
7. Compares data that is read with buffer data, show miscompares and if miscompares exist, stop (go to Step 19).  
8. If compare is OK, print OK message.  
9. Repeat Steps 10 - 15 until all blocks are written, or go to Step 4 until all blocks are written.  
10. Prints current block id and total number of bytes read.  
11. Backward spaces number of records written.  
12. Prints amount of data to read.  
13. Issues read command.  
14. If read error occurred or number of bytes read is different from requested number of bytes to read, go to Step 19.  
15. Compares data that is read with data written, show miscompares and if miscompares exist, stop (go to Step 19).  
16, If compare is Ok, print OK message.  
17. Forward space one file mark.  
18. Repeat Steps 10 - 24 until all blocks are written, or go to Step 4 until all blocks are written.

19. Prints current block id and total number of bytes written.

The Write Only steps:

1. Issues a Read Position.  
2. Sets block size.  
3. Generates special pattern.  
4. Put block id in bytes 0-3 of each block.  
5. Prints current block number, number of bytes and blocks.  
6. Issues write command.  
7. Prints updated statistics.  
8. If number of bytes written is different from requested bytes to write, stop (go to Step 10).  
9. Repeat Steps 5 - 9 until all blocks are written, or to Step 4 until all blocks are written.  
10. Print current block ID and total number of bytes written.

# Read or Write Files

Read or Write FilesWhen Read or Write Files is selected, a box under File Name: is where the path and name of the file is entered. Under that is a box named Number of records to read (0 for entire file). The default amount in the box is 100. Next, a menu bar under Test: gives you the choice of Read File from Tape or Write File to Tape. Once the Test is selected, Browse appears next to the File Name box to allow browsing for the needed file. When you select the Read or Write Files command, ITDT runs the following functions:

Read steps:

Read steps:  1. Prompts if to read a file from tape  2. You are prompted for destination file name  3. You are prompted for number of records to read (If you press Enter, the entire file is read.)  4. Prints the file name to be opened  5. Opens the file (r/w with large file support, 666 umask)  6. Issues Query Parameters ioctl command, if it fails, quit  7. Sets blocksize to maximum, variable blocksize mode  8. Calculates the number of blocks to read.  9. Prints number of records to read.  10. ITDT read from tape.  11. Writes to file, stop if data count is not equal to data count requested.  12. If more data to read, go to Step 10.  13. Prints statistics.

Write steps:

1. Prompts if to write a file to tape.  
2. You are prompted for the source file name.  
3. Prints the file name to be opened.  
4. Opens the file (r/o with large file support).  
5. Issues Query Parameters ioctl command, if it fails, quits.  
6. Sets blocksize to maximum, variable blocksize mode  
7. Prints write statement.  
8. Reads from file.  
9. Writes to tape, stop if data counts written is not equal to data count requested.  
10. Prints statistics.

# Erase

When you select the Erase command, ITDT issues the (extrinsic) ioctl command.

# Load Tape

ITDT issues a SCSI Load command to load a tape.

# Unload Tape

When you select the Unload Tape command

1. ITDT issues the (extrinsic) ioctl command.  
2. The tape rewinds and then unloads.

# Write Filemarks

When you select the Write Filemarks command

1. In the Filemark-Count box, enter the number of filemarks to write.

2. ITDT issues the (extrinsic) ioctl command.

# Synchronize Buffers

When you select the Synchronize Buffers command, ITDT issues the ioctl command.

# Query/Set Parameter

When you select the Query/Set Parameter command

1. ITDT shows the changeable parameters.

Note: The list of changeable parameters are operating system specific. For a list of changeable parameters, refer to Table 15 on page 137.

2. Select from the list of parameters that can be changed by clicking the choice.

3. ITDT requests prompt for parameter value (if required).

4. ITDT requests safety prompt (if required).

5. ITDT issues the ioctl command.

# Query/Set Position

When you select the Query/Set Position command

1. ITDT prints the current position and requests the new position.

Note: ITDT does not distinguish between logical and physical position. It shows the current position and queries for the one to set, then sets the new position.

2. Enter the block id for where the tape must go. This block id must be entered in decimal. When the tape is set, the block id is printed in decimal with hexadecimal in parentheses.

3. ITDT issues the Set Position ioctl and returns the pass or fail results.

4. ITDT prints decoded logical position details.

5. ITDT issues Query Physical Position ioctl command.

6. ITDT prints decoded physical position details.

7. You are prompted for position to set (logical or physical) or to stop.

8. You are prompted for the block id in decimal or hexadecimal.

9. ITDT prints a summary.

10. ITDT issues the Set Position ioctl command.

11. Repeat steps 2 - 5.

# Query Encryption Status

When you select the Query Encryption Status command

1. ITDT issues Get Encryption State ioctl command. 
2. ITDT displays encryption settings (Drive EC, Encryption Method, Encryption state).

# Display Message

When you select the Display Message command

1. ITDT provides Parameter boxes in which you can enter 1 or 2 messages up to 8 characters.

Note: Display Message works only on drives that have a display pane, the 3590 and 3592 drives.

2. In the Type: menu, select which message (0 or 1) you want shown and if you want it to flash. There is also an alternate (alt) selection that alternates between messages.

3. ITDT issues the ioctl command.

4. ITDT prints the displayed message.

# Display All Paths

When you select the Display All Paths command

1. ITDT issues an ioctl command. 
2. ITDT outputs decoded path information for all paths.

# Report Density Support

When you select the Report Density Support command

1. ITDT prints report status text for all supported media. 
2. ITDT issues Report Density Support ioctl command to retrieve all supported media.

3. ITDT prints all requested reports. Data is printed in a decoded way. Scroll the screen to print each one of the following status texts:

Density name Assigning organization Description Primary density code Secondary density code Write OK Duplicate Default Bits per MM Media Width Tracks Capacity (megabytes).

4. ITDT prints report status text for current media. 
5. ITDT issues Report Density Support ioctl command to retrieve current media. 
6. ITDT prints report data in a decoded way.

# Test Encryption Path

When you select the Test Encryption Path command

Note: Not supported for the HP- UX operating system.

1. ITDT prints status message that server configuration and connections are tested. 
2. ITDT issues the Encryption Diagnostics ioctl command, Ping Diag. 
3. ITDT prints number of servers available or error message. 
4. ITDT issues the Encryption Diagnostics ioctl command, Basic Encryption Diag. 
5. ITDT prints completion code or error message. 
6. ITDT issues the Encryption Diagnostics ioctl command, Full Encryption Diag. 
7. ITDT prints completion code or error message.

# Element Information

When you select the Element Information command:

1. ITDT issues the ioctl command. 
2. ITDT shows

Number of robots First robot address Number of slots First slot address Number of I/E elements First element address Number of drives First drive address

# Position to Element

When you select the Position to Element command:

1. In the Parameter boxes, the Transport element address must be entered, in decimal (picker).  
2. Insert the Destination element address in decimal.  
3. ITDT issues the ioctl command.

# Element Inventory

When you select the Element Inventory command:

1. ITDT issues the Element Info ioctl command.  
2. ITDT issues the Element Inventory ioctl command.  
3. ITDT displays decoded element inventory information.

# Exchange Medium

When you select the Exchange Medium command:

1. Insert source address into the Source address box in Decimal.  
2. Insert the first destination address in decimal in the First destination address box.  
3. Insert the second destination address in decimal in the Second destination address box.  
4. ITDT issues the ioctl command.

# Move Medium

When you select the Move Medium command:

1. Insert source element address into the Source element address box in Decimal.  
2. Insert the first destination element address in decimal in the First destination element address box.  
3. Insert the second destination element address in decimal in the Second destination element address box.  
4. ITDT issues the ioctl command.

# Initialize Element Status

When you select the Initialize Element Status command:

1. ITDT issues the ioctl command.  
2. ITDT prints the command summary.

# Prevent/Allow Medium Removal

When you select the Prevent/Allow Medium Removal command:

1. Use the menu to either prevent or allow.  
2. ITDT issues the ioctl command.

# Initialize Element Status Range

When you select the Initialize Element Status Range command:

1. Insert the first slot address in decimal in the provided box.  
2. Insert the number of slots in the provided box.  
3. ITDT issues the ioctl command.

# Read Device IDs

When you select the Read Device IDs command:

1. ITDT issues the Element Info ioctl command.  
2. If no drive is present, ITDT prints NO DRIVE PRESENT and exits.

3. ITDT prints information for all drives.

# Read Cartridge Location

When you select the Read Cartridge Location command:

Read Cartridge LocationWhen you select the Read Cartridge Location command:1. You are prompted for address of the first element.2. If address is zero, print the error message and exit.3. You are prompted for the number of elements.4. If the number of elements is zero, print the error message and exit.5. ITDT issues the Element Info ioctl command.6. ITDT verifies that the address range is valid. Otherwise, print the error message and exit.7. If no slots are found in Element Info data; print the error message and exit.8. ITDT issues the READ_CARTRIDGE_LOCATION ioctl command.9. ITDT prints decoded storage element information.

# Configure TCP/IP Ports

Configure TCP/IP PortsFor LTO 5, TS1140, and later drives, the ethernet port settings can be configured with the Configure TCP/IP Port command. The Configure TCP/IP Port command displays the current settings:

![](images/7af801fda5420a0a1cbdd34f54a8c68461079b4299d855c0d4388fa57b903a6b.jpg)  
Figure 68. Configure TCP/IP Ports command in the Graphical Edition

After you click Apply, the new values are set and the updated values display.

Note: Because earlier drive generations do not have an ethernet port, the Configure TCP/IP Ports command is rejected for these devices with the following message: TCP/IP configuration is not supported on this product.

# Dump/Force Dump/Dump

When you select the Dump/Force Dump/Dump command:

Dump/Force Dump/DumpWhen you select the Dump/Force Dump/Dump command:1. ITDT retrieves the dump.2. ITDT issues the Force Dump command.3. ITDT retrieves the second dump.4. ITDT displays the name of stored dump files and the output directory where they are stored. The dump filenames start with the serial number of the device.

# Firmware Update

Firmware UpdateWhen you select the Firmware Update command, browse to the microcode file to be used. ITDT runs the firmware update and displays progress status and result.

The following site is available for the latest firmware files: http://www.ibm.com/support/fixcentral/. Select System Storage > Tape Systems > Tape autoloaders and libraries or Tape drives.

# Verifying correct attachment of your devices

Before you start to use your devices for production work with your applications, or if you encounter difficulties with your devices, you might want to verify that the hardware, connections, and device drivers are working together properly. Before you can do this verification, you must do the following procedure -

1. Install your hardware as indicated in the appropriate hardware manuals.  
2. Power On your hardware and verify that the hardware is functioning properly by running commands according to the product documentation. See "IBM Tape Product Publications" on page xiii.  
3. Attach your hardware to the host system as indicated in the appropriate hardware manuals and as indicated in the appropriate chapters from this manual.  
4. Start your operating system as indicated in the appropriate chapters from this manual.  
5. Log in to the operating system as Administrator.  
6. If device drivers are used by your device other than the ones documented in this manual, disable the other device drivers, and install or enable the drivers that are documented in this manual.  
7. Start ITDT (for instructions see "Starting ITDT - Standard Edition" on page 79).  
8. Scan for devices. Any devices that show up are properly attached.

# Platform-specific help

There is a problem determination section for each platform.

AIX: "Problem determination" on page 30  - Linux: "Problem determination" on page 59  - Windows: "Problem determination" on page 71

# IBM technical support

If the problem persists after these procedures are followed, it is possible that an unexpected condition occurred in the driver's environment. In this case, contact your IBM service representative (1- 800- IBM- SERV) and provide the following information to help IBM re- create and resolve the problem:

1. Machine type and model of your IBM tape product  
2. Specific driver version  
3. Description of the problem  
4. System configuration  
5. Operation that was running at the time the problem was encountered

# Managing the microcode on the IBM tape drive

Microcode is computer software that is stored in nonvolatile storage on your tape device or library hardware. It controls the operation of your hardware. When your tape device or library hardware was manufactured, a microcode load was installed and shipped with your device.

If you are having trouble with your hardware, IBM service personnel ask what level of microcode you have on your hardware. If they believe that you need a new level of microcode, they might instruct you to install a newer level of microcode on your hardware. They can provide you with updated microcode.

You can query the current level of microcode by issuing commands on the front panel of your hardware. Consult the appropriate hardware reference manual for specific instructions on querying your microcode level.

If your device is connected to a host system that has device or library support, you can also query the last 4 digits of the current level of microcode with software. Refer to "IBM Tape Diagnostic Tool (ITDT)" on page 73. The unit must be powered On, configured properly, and ready. For information, refer to the appropriate chapter in this document (based on the operating system/platform) for details on how to have the device ready.

The following instructions are a guide to install another version of microcode on a tape drive.

1. Ensure that the tape drive is connected to a host system and that the tape device driver is powered-On and configured properly with no tape cartridge in the drive. Follow the instructions in "Verifying correct attachment of your devices" on page 191 to ensure that the drive is configured properly and ready. 
2. Open ITDT and follow the instructions for downloading microcode. These instructions are in both the SE and the GE versions. In SE, it is available in all sections; scan menu under Firmware update, the tape utility (71) section, and the scripting (uccode) command.

# Notices

References in this publication to IBM products, programs, or services do not imply that IBM intends to make these available in all countries (or regions) in which IBM operates.

Any references to an IBM program or other IBM product in this publication is not intended to state or imply that only IBM's program or other product may be used. Any functionally equivalent program that does not infringe any of IBM's intellectual property rights may be used instead of the IBM product. Evaluation and verification of operation in conjunction with other products, except those expressly designed by IBM, is the user's responsibility.

IBM may have patents or pending patent applications covering subject matter in this document. The furnishing of this document does not give you any license to these patents. You may send license inquiries, in writing, to:

IBM Director of Licensing  IBM Corporation  North Castle Drive  Armonk, NY 10504- 1785  U.S.A.

For license inquiries regarding double- byte character set (DBCS) information, contact the IBM Intellectual Property Department in your country or send inquiries, in writing, to:

Intellectual Property Licensing  Legal and Intellectual Property Law  IBM Japan, Ltd  19- 21, Nihonbashi- Hakozakicho, Chuo- ku  Tokyo 103- 8510, Japan

The following paragraph does not apply to the United Kingdom or any other country (or region) where such provisions are inconsistent with local law:

INTERNATIONAL BUSINESS MACHINES CORPORATION PROVIDES THIS PUBLICATION "AS IS" WITHOUT WARRANTY OF ANY KIND, EITHER EXPRESS OR IMPLIED, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED WARRANTIES OF NON- INFRINGEMENT, MERCHANTABILITY, OR FITNESS FOR A PARTICULAR PURPOSE. Some states (or regions) do not allow disclaimer of express or implied warranties in certain transactions, therefore, this statement cannot apply to you.

This information could include technical inaccuracies or typographical errors. Changes are periodically made to the information herein; these changes are incorporated in new editions of the publication. IBM may make improvements and/or changes in the products and/or programs described in this publication at any time without notice.

IBM may use or distribute any of the information you supply in any way it believes appropriate without incurring any obligation to you.

The ITDT- SE and ITDT- GE software uses Henry Spencer's regular expression library that is subject to the following copyright notice:

"Copyright 1992, 1993, 1994, 1997 Henry Spencer. All rights reserved. This software is not subject to any license of the American Telephone and Telegraph Company or of the Regents of the University of California.

Permission is granted to anyone to use this software for any purpose on any computer system, and to alter it and redistribute it, subject to the following restrictions:

1. The author is not responsible for the consequences of use of this software, no matter how awful, even if they arise from flaws in it.  
2. The origin of this software must not be misrepresented, either by explicit claim or by omission. Since few users ever read sources, credits must appear in the documentation.  
3. Altered versions must be plainly marked as such, and must not be misrepresented as being the original software. Since few users ever read sources, credits must appear in the documentation.  
4. This notice cannot be removed or altered.

# Trademarks

The following terms are trademarks of International Business Machines Corporation in the United States, other countries (or regions), or both:

<table><tr><td>AIX</td><td>IBMLink</td><td>RS/6000</td><td>System z</td></tr><tr><td>AIX 5L</td><td>Magstar</td><td>S/390</td><td>Tivoli</td></tr><tr><td>FICON®</td><td>Micro Channel</td><td>StorageSmart</td><td>TotalStorage™</td></tr><tr><td>HyperFactor®</td><td>Netfinity</td><td>System i®</td><td>Virtualization Engine</td></tr><tr><td>i5/OS</td><td>POWERS</td><td>System p</td><td>xSeries</td></tr><tr><td>iSeries</td><td>ProtectTIER®</td><td>System Storage</td><td>z9</td></tr><tr><td>IBM</td><td>pSeries</td><td>System x</td><td>zSeries</td></tr></table>

Adobe and Acrobat are either registered trademarks or trademarks of Adobe Systems Incorporated in the United States, and/or other countries.

Intel, Itanium, and Pentium are trademarks of Intel Corporation in the United States, other countries (or regions), or both.

Java and all Java- based trademarks are trademarks of Oracle, Inc. in the United States, other countries, or both.

Linux is a registered trademark of Linus Torvalds in the United States, other countries, or both.

Microsoft, Windows, Windows NT, and Windows 2000 are trademarks of Microsoft Corporation in the United States, other countries (or regions), or both.

UNIX is a registered trademark of The Open Group in the United States and other countries (or regions).

Other company, product, and service names may be trademarks or service marks of others.

# Index

# A

Aabout data encryption 8Accessing 74ACTRC utility 36Adding or removing devices 65AIX (Atape) 3, 11, 12, 12, 13, 14, 14, 15, 15, 15, 15, 20, 21, 22, 23, 23, 25, 27, 27, 27, 28, 28, 29, 29, 29, 29, 29, 29, 30, 30, 30, 31, 31, 31, 32, 32, 34, 34, 35, 35, 35, 36, 36, 36, 38, 38, 38, 39AIX device parameters 38Alternate pathing 15Archive mode unthread (AMU) 20ATRC utility 35autoloading 15automatic dump facility for 3590 and Magstar MP tapedrives 34Automatic failover 4

# B

Bblock size 15buffered mode 15bulk rekey 10

# C

Ccapacity scaling 20changeable parameters 47Checklist 10common utilities 38component tracing 35components created during installation 40compression 15configuration 29configuration parameters 45, 46, 47Configuration parameters 15configuring and unconfiguring path failover support 27configuring and unconfiguring primary and alternative devices 28configuring tape and medium changer devices 14Configuring Tape and Medium Changer devices 43, 43, 43connectivity 29Control path failover 25conventions used 40create an FMR tape 36

# D

DData flow 11, 39data path 38data path failover 3Data path failover 27Deconfiguring the 3490E, 3590, Magstar MP, or 7332 tapedevice 14

Deconfiguring the 3575,7331,7334,7336,or 7337 medium changer device 15 Detail data 32 detailed description 36 Device and volume information logging 30, 31, 31, 31 Device driver configuration 29 Device driver management 65 Devices not reported 64 disable procedure 65 drive dump 29 Dump device commands 30 Dump support 30 Dynamic Runtime Attributes 34

# E

EKM server logs 29emulate autoloader 15encryption 3error labels 32error log analysis 36error log templates 32error logging 29, 32

# F

Ffail degraded media 15field support 29force microcode dump 36

# G

Ggeneral information 8GKLM 10Guardium Key Lifecycle Manager 10

# H

Hhardware requirements 65Hardware requirements 12, 40, 64

# I

IInstallation and configuration instructions 12, 13, 14, 14, 15, 15, 40, 40, 40, 41, 42, 42, 43, 43, 43, 45, 65Installation overview 65Installation procedure 41Installation procedure 13installation procedures 65Introduction and product requirements 3, 39, 64, 73, 73iostat utility 39ITDT 73, 73, 74

# L

Llibrary requirements 8library- managed encryption planning 10Linux (lin_tape) 39, 40, 40, 40, 40, 41, 42, 42, 43, 43, 45, 45, 45, 46, 47, 51, 51, 51, 64Load balancing 3log file 31

logging 15 logical write protect 20

# M

logging 15 logical write protect 20Mmanaging encryption 8 Managing microcode on the tape drive 191 maximum size of the log file 15 media parameters 20 microcode load 36

# N

new logical name 15 nonchangeable parameters 46

# P

path failover 3 Path failover 3,4 Path failover support 27,27,28,28 Performance considerations 38,38,38,39 Persistent naming support 23 preinstallation considerations 13 primary and alternative paths 27 Problem determination 30,32,32,34,34,35,35,35,36 Product requirements 11,12,40,64 Product requirements and compatibility 65 Purpose 3,11,73,73

# Q

querying drive configuration 29 querying installed package 42 querying primary and alternate path configuration 28

# R

read dump 36 read error recovery time 15 record space mode 15 removal procedure 65 requirements library 8 tape 8 Reservation conflict logging 31 reservation key 15 reservation type 15 reset drive 36 retain reservation 15 rewind immediate 15

# S

SCSI status busy retry 15 sense data 29 server logs 29 SKLM 10 SMIT panels 29 software requirements 65 Software requirements 12,40 Special files 21,22,23,51,51,51

Special files for 3490E,3590,Magstar MP,or 7332 tape devices 22 Special files for 3575,7331,7334,7336,or 7337 medium changer devices 23 Special files for medium changer device 51 Special files for tape device 51 supported hardware 3 system encryption 15 system encryption for Write commands 15 System p 43 System z 43 System- managed encryption 29,29

# T

Tape drive service aids 36,36 Tape drive, media, and device driver parameters 15,15,20, 45,45,46,47 tape log utility 31 testing data encryption configuration 29 trace facility 35 trailer labels 15

# U

uninstall procedure 15,45 uninstalling the device drivers 65 updating procedure 42

# V

Verifying device attachment 191 volume ID for logging 20

# W

Windows (IBMtape) 65 Windows device driver 64,65 Windows NT 65

IBM.