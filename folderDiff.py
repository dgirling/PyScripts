from sys import prefix
import boto3
import os.path

s3 = boto3.resource('s3')

#In.txt format:
#2022-04-08 11:37:13       2613 input-001/1000000000/1600000000000.txt

#Out.txt format:
#2022-05-03 12:32:58       1529 folder-aws-mc-001/999999999999-EntitiesDetection-11a11111111111111abc1111e11e11d/1000000000/1600000000000.txt.out


# get list of inbound files
CMbucket = 'dg-cm-missing-files' # name of bucket where in and out files are located
inboundFolder = 'input-001' # this is the top level folder, which contains all the keys and text files
outboundFolder = 'folder-aws-mc-001/999999999999-EntitiesDetection-11a11111111111111abc1111e11e11d/' # this is the top level folder from where CM results are written out to
newinboundFolder = 'newbatch' # files missing in output folder will be copied from inbound folder to 'newbatch' folder

bucket_ref = s3.Bucket(CMbucket)

fileNames_dict = dict() # store key/filename as hash for fast lookup

# enumerate over keys of outbound folder, and add to dictionary for look up later
for obj in bucket_ref.objects.filter(Prefix = outboundFolder):
    file_name = os.path.basename(obj.key)
    if (len(file_name) > 3):
        if (file_name[-3:].casefold() == "out".casefold()):
            fileKey = obj.key[-(len(obj.key)-len(outboundFolder)):][:-4] # clean up, remove, leaving folder/filename
            fileNames_dict[fileKey] = file_name


            
# enumerate over inbound folder, and check to see if key exists, if it does not, copy file to new location 
for obj in bucket_ref.objects.filter(Prefix = inboundFolder):
    file_name = os.path.basename(obj.key)
    if (len(file_name) > 3):
        if (file_name[-3:].casefold() == "txt".casefold()):
            fileKey = obj.key[-(len(obj.key)-len(inboundFolder)):][1:] # clean up, remove, leaving folder/filename
            # check in dict for match, if no match store list
            if not fileKey in fileNames_dict:
                copy_source = {
                    'Bucket': CMbucket,
                    'Key': obj.key
                }
                # un-comment copy command below if you want to copy missing files to a new folder                
                # s3.meta.client.copy(copy_source, CMbucket, newinboundFolder + '/' + fileKey)
                print ('Copying missing file ' + obj.key + ' to ' + newinboundFolder + '/' + fileKey)

 
