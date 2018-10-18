[clusters]
arke_basic =
    172.23.97.12:kv
    172.23.97.13:kv
    172.23.97.14:kv
    172.23.97.15:kv

[clients]
hosts =
    172.23.97.16
    172.23.97.17
credentials = root:couchbase

[storage]
data = /data

[credentials]
rest = Administrator:password
ssh = root:couchbase

[parameters]
OS = CentOS 7
CPU = Data: 2 x E5-2630 v2(24 vCPU)
Memory = Data: 64 GB
Disk = Samsung PM863 SSD
