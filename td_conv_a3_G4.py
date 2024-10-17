# Convert A3 fill data to TSDB csv format
# KRX new data format (2023.01.21)

import os
import sys
import datetime
import codecs
import math
import csv

from collections import deque

# arg check
if len(sys.argv) == 1:
	print('Need arguments. python tsdb_conv.py YYYYMMDD')
	quit()

############################################
# tsdb_conv.py YYYYMMDD
# Convert A3 data to tsdb csv format
############################################

#FILE_NAME = 'data-2017-01-31.txt'
ARGV1 = sys.argv[1]
YY = ARGV1[:4]
MM = ARGV1[4:6]
DD = ARGV1[6:]
FILE_NAME = '/mdb/data/data-' + YY + '-' + MM + '-' + DD + '.txt'
print('Datafile: ' + FILE_NAME)

## Working directory
WDIR = '/home/trading/server/prep-data/tsdb/' + YY + MM + DD
ODIR = '/home/trading/server/prep-data/tsdb/a3/'
print('Working directory: ' + WDIR)
if not os.path.exists(WDIR):
	print('making working directory...')
	os.system('mkdir ' + WDIR)

RD_DATE = YY + '-' + MM + '-' + DD

#### ISIN Table and Dictionary
d_isin = {}

def get_field(line, length, acc_length):
	begin = acc_length - length
	try:
		ret = str(line[begin:begin+length].decode('cp949'))
		return ret
	except:
		return str(line[begin:begin+length])


# Read ISIN batch
# Check files if exist
BF_NAME = WDIR + '/TSDB_BATCH_' + RD_DATE + '.csv'
if os.system('ls ' + BF_NAME + ' > /dev/null') != 0:
	print('No batch file.')
	print('Run get_batch.py first.')
	quit()


# Make ISIN table
with open(BF_NAME, 'r') as bfile:
	b_reader = csv.reader(bfile, delimiter='|')
	for bline in b_reader:
		d_isin[bline[0]] = bline[1].strip()

print('Date: ' + RD_DATE + '  ISIN count: ' + str(len(d_isin)) + ' Ready to scan...')

prt_cnt = 0

# Market Hour
# Start
s_hour = 15
s_min = 58
s_sec = 0
# End
e_hour = 18
e_min = 5
e_sec = 0

# market hour data
start_time = datetime.timedelta(hours=s_hour, minutes=s_min, seconds=s_sec)
end_time = datetime.timedelta(hours=e_hour, minutes=e_min, seconds=e_sec)
print(start_time)
print(end_time)


out_file_name = WDIR + '/TSDB_TICK_G4_' + RD_DATE + '.csv'
out_f = open(out_file_name, 'w')
out_f.write('time,isin,price,quantity' + '\n')

# Read data file
with open(FILE_NAME, 'rb') as dfile:

	# Skip morning batch
	last_pos = 542216192
	dfile.seek(last_pos)

	print('Begin...')

	for rline in dfile:

#		if prt_cnt > 50:
#			print('****************')
#			quit()

		try:
			tstp = datetime.timedelta(microseconds = int(rline[0:11]))
		except:
			continue

			
		#mon_time = datetime.timedelta(hours=m_hour, minutes=m_min, seconds=m_sec)
		if tstp < start_time:
			continue
		if tstp > end_time:
			print('Time end')
			break

		st_data = rline[12:]
		tr_code = get_field(st_data, 2, 2)
		prod_type = get_field(st_data, 2, 4)	# first 2 digits of the second field
		isin_code = get_field(st_data, 12, 29)
		board_id = get_field(st_data, 2, 15)

		if prod_type != '01':
			continue


		# A3
		if tr_code == 'A3' and board_id == 'G4':

			#idx: find ISIN table index from dictionary
			try:
				idx = d_isin[isin_code]
			except:
				continue


			s_code = d_isin[isin_code]

			e_px = int(get_field(st_data, 11, 70))
			qty = int(get_field(st_data, 10, 80))

			# Get open price
			o_px = int(get_field(st_data, 11, 91))
			h_px = int(get_field(st_data, 11, 102))
			l_px = int(get_field(st_data, 11, 113))

			acc_qty = int(get_field(st_data, 12, 125))
			acc_amt = int(get_field(st_data, 18, 143))	# skip last 4 digits, decimal points and float numbers
			exec_sign = get_field(st_data, 1, 148)

			e_sign = 'O'
			if exec_sign == '1':
				e_sign = 'S'
			elif exec_sign == '2':
				e_sign = 'B'

			out_line = RD_DATE + ' ' + str(tstp) + ',' + isin_code + ',' + s_code + ','+ str(e_px) + ',' + str(qty) + ',' + str(o_px) + ',' + str(h_px) + ',' + str(l_px) + ',' + str(acc_qty) + ',' + str(acc_amt) + ',' + e_sign
			out_f.write(out_line + '\n')

#			print(out_line)
#			prt_cnt += 1

		elif tr_code == 'B6':
			#print(str(tstp) + ': ' + str(prt_cnt) + ' SCODE: ' + isin_code + ' ORDERBOOK UPDATE')
			continue

my_cmd = 'cp ' + out_file_name + ' ' + ODIR
os.system(my_cmd)
