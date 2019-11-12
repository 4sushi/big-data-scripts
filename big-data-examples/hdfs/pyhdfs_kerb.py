from hdfs.ext.kerberos import KerberosClient

client = KerberosClient('http://X:50070')
# Listing all files inside a directory.
fnames = client.list('.')
print(fnames)
