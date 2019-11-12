from hdfs import InsecureClient

client = InsecureClient('http://X:50070', user='X')

hdfs_file  = 'tweets'

# Create file if not exist
hdfs_files_list = client.list('')
if hdfs_file not in hdfs_files_list:
    with client.write(hdfs_file) as writer:
        writer.write('')



with client.write(hdfs_file, append=True) as writer:
    writer.write("test1")
    writer.write("test2")
    writer.write("test3")
