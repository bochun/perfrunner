[clusters]
magma-nvme =
    172.23.100.135:kv
    172.23.100.136:kv
    172.23.100.137:kv
    172.23.100.138:kv

[clients]
hosts =
    172.23.100.139
credentials = root:couchbase

[storage]
data = /data

[credentials]
rest = Administrator:password
ssh = root:couchbase

[parameters]
OS = CentOS 7
CPU = Gold 6230 2.1GHz (80 vCPU)
Memory = 128 GB
Disk = Intel P4610 3.2TB NVMe x3, RAID0
