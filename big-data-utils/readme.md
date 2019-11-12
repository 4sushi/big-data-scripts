# Scripts

## landing_table.py

Description:

- (optional) pause/restart processus during landing table processing
- Copy data from landing table to historic table
- Remove/re-create landing table
- (optional) re-insert period data in landing table

Example:

```
$ python landing_table -h

$ python landing_table.py kudu_db.test kudu_db.test_landing --keep_data_landing --filter='time >= date_add(now(),-1)' 

$ python landing_table.py kudu_db.test kudu_db.test_landing --keep_data_landing --filter='time >= date_add(now(),-1)' --pid_list=18381,1232 --debug

$ python landing_table.py kudu_db.test kudu_db.other --keep_data_landing --filter='time >= date_add(now(),-1)' --pid_list=18381,1232 --unsafe
```

## write_hdfs.py

Description:

- Script to write content in HDFS, cut in multiple files (approximately 124mb per file)
- Compress content before write in HDFS
- Script created to use with twitter API

Example:

```
from write_hdfs import WriteHdfs

write_hdf = WriteHdfs(['http://X:50070', 'http://Y:50070'], log_level='DEBUG')
for i in range(1, 1000000):
    print(time.time())
    write_hdf.write_content('a' * 1024 * 1024 * 30 * i)
    time.sleep(0.5)
```
