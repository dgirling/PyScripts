from sys import prefix
import boto3
import os.path
import chardet  # pip install chardet
from chardet.universaldetector import UniversalDetector

s3 = boto3.resource('s3')



#enumerate over folder contents within s3 bucket, identifying txt files and converting to UTF-8 as necessary and saving to new folder
CMbucket = 'va-dg-test' # name of bucket where files are located
inboundFolder = 'inbound' # this is the top level folder, which contains all the text files
newinboundUTF8Folder = 'newUTF8batch' # files missing in output folder will be copied from inbound folder to 'newUTF8batch' folder

bucket_ref = s3.Bucket(CMbucket)

detector = UniversalDetector()

for obj in bucket_ref.objects.filter(Prefix = inboundFolder):
    if not obj.key.lower().endswith('.txt'):
        continue # only read text files
    textCopy = obj.get()['Body'].read() # have to read object into memory to detect encoding. ugh!!
    encoded_as = chardet.detect(textCopy)
    if not encoded_as['encoding'].lower() == 'ascii': # save new file that is non ASCII Encoded as UTF-8.
        print ('Converting to UTF-8 :' + obj.key, encoded_as)
        content_text = textCopy.decode(encoded_as['encoding']) # decode to UTF-8 from encoding detected in blob
        
        # uncomment below to create a copy of the object that is now UTF-8 encoded and write to new folder
        #s3.Object(CMbucket, newinboundUTF8Folder + '/' + obj.key).put(Body=content_text) #create new file that is now UTF-8 encoded and save with same key name

