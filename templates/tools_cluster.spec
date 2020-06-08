[clusters]
aws =
    {% for server in servers -%}
        {{server}}:kv
    {% endfor %}

[clients]
hosts =
    {% for client in clients -%}
        {{client}}
    {% endfor %}
credentials = root:couchbase

[storage]
data = /data
backup = s3://cb-backup-to-s3-perftest

[credentials]
rest = Administrator:password
ssh =  root:couchbase

[parameters]
OS = {{os_info}}
CPU = {{cpu_info}}
Memory = {{mem_quota}} GB
Disk = {{storage_info}}

