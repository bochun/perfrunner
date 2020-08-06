[clusters]
hebe_c1 =
    172.23.100.190:kv,index,n1ql
hebe_c2 =
    172.23.100.204:kv,index,n1ql

[clients]
hosts =
    172.23.100.194
credentials = root:couchbase

[storage]
data = /data
index = /data

[credentials]
rest = Administrator:password
ssh = root:couchbase

[parameters]
OS = CentOS 7
CPU = E5-2680 v3 (48 vCPU)
Memory = 64GB
Disk = Samsung Pro 850
