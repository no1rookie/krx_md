from __future__ import division

import os
import sys
import datetime
import codecs
import math

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
FILE_NAME = '/mdb/cash/data-' + YY + '-' + MM + '-' + DD + '.txt'
print('Datafile: ' + FILE_NAME)

## Working directory
WDIR = '/home/trading/prep-data/tsdb/' + YY + MM + DD
BDIR = '/home/trading/prep-data/bat/'
print('Working directory: ' + WDIR)
if not os.path.exists(WDIR):
	print('making working directory...')
	os.system('mkdir ' + WDIR)

RD_DATE = YY + '-' + MM + '-' + DD

#### ISIN Table and Dictionary
bat_list = []
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
BF_NAME = BDIR + '/batch_' + RD_DATE
if os.system('ls ' + BF_NAME + ' > /dev/null') != 0:
	print('No batch file.')
	print('Run get_batch.py first.')
	quit()


# Make ISIN table
bf = codecs.open(BF_NAME, 'rb')
for bf_line in bf:
	bat_list.append(bf_line)

	# Make a dictionary for ISIN table
	d_isin.update({get_field(bf_line, 12, 17):len(bat_list)-1})

print('Date: ' + RD_DATE + '  ISIN count: ' + str(len(bat_list)) + ' Ready to scan...')

prt_cnt = 0

# Market Hour
# Start
s_hour = 9
s_min = 0
s_sec = 0
# End
e_hour = 15
e_min = 20
e_sec = 0

# market hour data
start_time = datetime.timedelta(hours=s_hour, minutes=s_min, seconds=s_sec)
end_time = datetime.timedelta(hours=e_hour, minutes=e_min, seconds=e_sec)
print(start_time)
print(end_time)


out_f = open(WDIR + '/TSDB_BIDASK_' + RD_DATE + '.csv', 'w')
out_f.write('time,isin,ask1_px,bid1_px,ask1_qty,bid1_qty,\
ask2_px,bid2_px,ask2_qty,bid2_qty,\
ask3_px,bid3_px,ask3_qty,bid3_qty,\
ask4_px,bid4_px,ask4_qty,bid4_qty,\
ask5_px,bid5_px,ask5_qty,bid5_qty,\
ask6_px,bid6_px,ask6_qty,bid6_qty,\
ask7_px,bid7_px,ask7_qty,bid7_qty,\
ask8_px,bid8_px,ask8_qty,bid8_qty,\
ask9_px,bid9_px,ask9_qty,bid9_qty,\
ask10_px,bid10_px,ask10_qty,bid10_qty' + '\n')

# Read data file
with open(FILE_NAME, 'rb') as dfile:

	# Skip morning batch
	last_pos = 542216192
	dfile.seek(last_pos)

	print('Begin...')
	qty_total = 0
	v_total = 0

	for rline in dfile:

#		if prt_cnt > 50:
#			print('****************')
#			quit()

		try:
			tstp = datetime.timedelta(microseconds = int(rline[0:11]))
		except:
			continue

			
		if tstp < start_time:
			continue
		if tstp > end_time:
			print('Time end')
			break

		st_data = rline[12:]
		tr_code = get_field(st_data, 2, 2)
		prod_type = get_field(st_data, 2, 4)
		isin_code = get_field(st_data, 12, 17)

		if prod_type != '01':
			continue


		if tr_code == 'B6':

			try:
				idx = d_isin[isin_code]
			except:
				continue

			gr_type = get_field(bat_list[idx], 2, 129)
			if gr_type != 'ST':
				continue

			ask1_px		=  int(get_field(st_data, 9, 43))
			bid1_px		=  int(get_field(st_data, 9, 52))
			ask1_qty	=  int(get_field(st_data, 9, 64))
			bid1_qty	=  int(get_field(st_data, 9, 76))
                                                                        
			ask2_px		=  int(get_field(st_data, 9, 85))
			ask2_qty	=  int(get_field(st_data, 9, 94))
			bid2_px		=  int(get_field(st_data, 9, 106))
			bid2_qty	=  int(get_field(st_data, 9, 118))

			ask3_px		=  int(get_field(st_data, 9, 127))
			ask3_qty	=  int(get_field(st_data, 9, 136))
			bid3_px		=  int(get_field(st_data, 9, 148))
			bid3_qty	=  int(get_field(st_data, 9, 160))

			ask4_px		=  int(get_field(st_data, 9, 169))
			ask4_qty	=  int(get_field(st_data, 9, 178))
			bid4_px		=  int(get_field(st_data, 9, 190))
			bid4_qty	=  int(get_field(st_data, 9, 202))

			ask5_px		=  int(get_field(st_data, 9, 211))
			ask5_qty	=  int(get_field(st_data, 9, 220))
			bid5_px		=  int(get_field(st_data, 9, 232))
			bid5_qty	=  int(get_field(st_data, 9, 244))

			ask6_px		=  int(get_field(st_data, 9, 253))
			ask6_qty	=  int(get_field(st_data, 9, 262))
			bid6_px		=  int(get_field(st_data, 9, 274))
			bid6_qty	=  int(get_field(st_data, 9, 286))

			ask7_px		=  int(get_field(st_data, 9, 295))
			ask7_qty	=  int(get_field(st_data, 9, 304))
			bid7_px		=  int(get_field(st_data, 9, 316))
			bid7_qty	=  int(get_field(st_data, 9, 328))

			ask8_px		=  int(get_field(st_data, 9, 337))
			ask8_qty	=  int(get_field(st_data, 9, 346))
			bid8_px		=  int(get_field(st_data, 9, 358))
			bid8_qty	=  int(get_field(st_data, 9, 370))

			ask9_px		=  int(get_field(st_data, 9, 379))
			ask9_qty	=  int(get_field(st_data, 9, 388))
			bid9_px		=  int(get_field(st_data, 9, 400))
			bid9_qty	=  int(get_field(st_data, 9, 412))

			ask10_px	=  int(get_field(st_data, 9, 421))
			ask10_qty      	=  int(get_field(st_data, 9, 430))
			bid10_px	=  int(get_field(st_data, 9, 442))
			bid10_qty      	=  int(get_field(st_data, 9, 454))

			out_line = RD_DATE + ' ' + str(tstp) + ',' + str(isin_code) + ',' + str(ask1_px) + ',' + str(bid1_px) + ',' + str(ask1_qty) + ',' + str(bid1_qty) \
										+ ',' + str(ask2_px) + ',' + str(bid2_px) + ',' + str(ask2_qty) + ',' + str(bid2_qty) \
										+ ',' + str(ask3_px) + ',' + str(bid3_px) + ',' + str(ask3_qty) + ',' + str(bid3_qty) \
										+ ',' + str(ask4_px) + ',' + str(bid4_px) + ',' + str(ask4_qty) + ',' + str(bid4_qty) \
										+ ',' + str(ask5_px) + ',' + str(bid5_px) + ',' + str(ask5_qty) + ',' + str(bid5_qty) \
										+ ',' + str(ask6_px) + ',' + str(bid6_px) + ',' + str(ask6_qty) + ',' + str(bid6_qty) \
										+ ',' + str(ask7_px) + ',' + str(bid7_px) + ',' + str(ask7_qty) + ',' + str(bid7_qty) \
										+ ',' + str(ask8_px) + ',' + str(bid8_px) + ',' + str(ask8_qty) + ',' + str(bid8_qty) \
										+ ',' + str(ask9_px) + ',' + str(bid9_px) + ',' + str(ask9_qty) + ',' + str(bid9_qty) \
										+ ',' + str(ask10_px) + ',' + str(bid10_px) + ',' + str(ask10_qty) + ',' + str(bid10_qty)
			out_f.write(out_line + '\n')
			#print(out_line)
			#prt_cnt += 1

		else:
			continue
