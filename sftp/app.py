
import pysftp

myHostname = "34.72.229.5" #ej:34.72.229.5
myUsername = "" 
myPassword = ""
cnopts = pysftp.CnOpts()
cnopts.hostkeys = None 

with pysftp.Connection(host=myHostname, username=myUsername, password=myPassword,cnopts=cnopts) as sftp:
    print("Connection succesfully stablished ... ")

    # Switch to a remote directory
    sftp.cwd('/html/')

    # Obtain structure of the remote directory '/var/www/vhosts'
    directory_structure = sftp.listdir_attr()

    # Print data
    for attr in directory_structure:
        print(attr.filename, attr)

# connection closed automatically at the end of the with statement