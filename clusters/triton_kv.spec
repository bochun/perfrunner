[clusters]
triton =
    172.23.132.17:8091
    172.23.132.18:8091
    172.23.132.19:8091
    172.23.132.20:8091

[clients]
hosts =
    172.23.132.14
credentials = root:couchbase

[storage]
data = /data
index = /data

[credentials]
rest = Administrator:password
ssh = root:couchbase

[parameters]
OS = CentOS 7
CPU = Data: E5-2630 v4 (40 vCPU)
Memory = 64GB
Disk = SSD