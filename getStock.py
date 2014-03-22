#!/usr/bin/python

import sys
import urllib
import zipfile
import pika
import os
import re
import glob

def download_file(filename):
	print "====== download ======"
	print "retrieving file " + 'http://www.bseindia.com/download/BhavCopy/Equity/' + filename
	resp = urllib.urlretrieve('http://www.bseindia.com/download/BhavCopy/Equity/' + filename, filename)
	print "====== download complete ======="

def unzip_file(zip_file, outdir):
	print "====== unzip ======"
	zf = zipfile.ZipFile(zip_file, "r")
	zf.extractall(outdir)
	print "====== unzip complete ======"

def post_msg():

	conn = pika.BlockingConnection(pika.ConnectionParameters('localhost'))
	channel = conn.channel()

	channel.queue_declare(queue='hello')
	channel.basic_publish(exchange='',
                      routing_key='hello',
                      body='Hello World!')
	print " [x] Sent 'Hello World!'"

	conn.close()

def del_file():
	print "======= delete ======="
	#flist = glob.glob("*.zip")
	for f in glob.glob('*.zip'):
		print "delete " + f
		os.remove(f)

	for f in glob.glob('*.CSV'):
		print "delete " + f
		os.remove(f)
	print "======= delete complete ======="

def main():
	date = sys.argv[1]
	m = re.match(r'\d{6}', date)
	if m:
		filename = 'eq'+ sys.argv[1] + '_csv.zip'
		#filename = 'eq210114_csv.zip'
		outdir = '/home/ss/stock/'

		download_file(filename)
		unzip_file(filename,outdir )
		post_msg()
		del_file()
	else:
		print "Invalid Input"
		exit(1)

if __name__ == '__main__':
	main()

