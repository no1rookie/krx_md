import os
import sys
import datetime
import codecs
import math
import csv

from collections import deque

# arg check
if len(sys.argv) == 1:
	print('Need arguments. python td_conv_c1.py YYYYMMDD')
	quit()

############################################
# td_conv_c1.py YYYYMMDD
# Convert C1 data to tsdb csv format
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
ODIR = '/home/trading/server/prep-data/tsdb/c1/'
print('Working directory: ' + WDIR)
if not os.path.exists(WDIR):
	print('making working directory...')
	os.system('mkdir ' + WDIR)

RD_DATE = YY + '-' + MM + '-' + DD

#### ISIN Table and Dictionary
d_isin = {}
isin_c1_update = {}		# C1 data comes three times, to bypass duplicated isin data

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
    next(b_reader, None)
    for bline in b_reader:
        d_isin[bline[0]] = {'scode':bline[1].strip(), \
                     'isin': bline[0], \
                     'market': bline[8], \
				     'inv_1000_s_q':0, 'inv_1000_s_a':0, 'inv_1000_b_q':0, 'inv_1000_b_a':0, \
				     'inv_2000_s_q':0, 'inv_2000_s_a':0, 'inv_2000_b_q':0, 'inv_2000_b_a':0, \
				     'inv_3000_s_q':0, 'inv_3000_s_a':0, 'inv_3000_b_q':0, 'inv_3000_b_a':0, \
				     'inv_3100_s_q':0, 'inv_3100_s_a':0, 'inv_3100_b_q':0, 'inv_3100_b_a':0, \
				     'inv_4000_s_q':0, 'inv_4000_s_a':0, 'inv_4000_b_q':0, 'inv_4000_b_a':0, \
				     'inv_5000_s_q':0, 'inv_5000_s_a':0, 'inv_5000_b_q':0, 'inv_5000_b_a':0, \
				     'inv_6000_s_q':0, 'inv_6000_s_a':0, 'inv_6000_b_q':0, 'inv_6000_b_a':0, \
				     'inv_7000_s_q':0, 'inv_7000_s_a':0, 'inv_7000_b_q':0, 'inv_7000_b_a':0, \
				     'inv_7100_s_q':0, 'inv_7100_s_a':0, 'inv_7100_b_q':0, 'inv_7100_b_a':0, \
				     'inv_8000_s_q':0, 'inv_8000_s_a':0, 'inv_8000_b_q':0, 'inv_8000_b_a':0, \
				     'inv_9000_s_q':0, 'inv_9000_s_a':0, 'inv_9000_b_q':0, 'inv_9000_b_a':0, \
				     'inv_9001_s_q':0, 'inv_9001_s_a':0, 'inv_9001_b_q':0, 'inv_9001_b_a':0}

print('Date: ' + RD_DATE + '  ISIN count: ' + str(len(d_isin)) + ' Ready to scan...')


# Grep A001 lines
my_cmd = 'grep -a \'C101S\|C101Q\' ' + FILE_NAME + ' > ' + WDIR + '/C101'
print(my_cmd)


# Check files if exist
if os.system('ls ' + WDIR + '/C101 > /dev/null') != 0:
	print('making file C101...')
	os.system(my_cmd)

out_f = open(WDIR + '/TSDB_INV_FLOW_' + RD_DATE + '.csv', 'w')
out_f.write('date,scode,isin,market,inv1000_sell_qty,inv1000_sell_amount, inv1000_buy_qty, inv1000_buy_amount, \
    inv2000_sell_qty, inv2000_sell_amount, inv2000_buy_qty, inv2000_buy_amount, \
    inv3000_sell_qty, inv3000_sell_amount, inv3000_buy_qty, inv3000_buy_amount, \
    inv3100_sell_qty, inv3100_sell_amount, inv3100_buy_qty, inv3100_buy_amount, \
    inv4000_sell_qty, inv4000_sell_amount, inv4000_buy_qty, inv4000_buy_amount, \
    inv5000_sell_qty, inv5000_sell_amount, inv5000_buy_qty, inv5000_buy_amount, \
    inv6000_sell_qty, inv6000_sell_amount, inv6000_buy_qty, inv6000_buy_amount, \
    inv7000_sell_qty, inv7000_sell_amount, inv7000_buy_qty, inv7000_buy_amount, \
    inv7100_sell_qty, inv7100_sell_amount, inv7100_buy_qty, inv7100_buy_amount, \
    inv8000_sell_qty, inv8000_sell_amount, inv8000_buy_qty, inv8000_buy_amount, \
    inv9000_sell_qty, inv9000_sell_amount, inv9000_buy_qty, inv9000_buy_amount, \
    inv9001_sell_qty, inv9001_sell_amount, inv9001_buy_qty, inv9001_buy_amount \
    ' + '\n')

# Read F001/PD file and get ISIN + PD Code + PD Time
# Check PD lines
fpdname = WDIR + '/C101'
ff001 = codecs.open(fpdname, 'rb')
for ff_line in ff001:
    f_rcv = ff_line[12:]
    isin_code = get_field(f_rcv, 12, 17)			# Stock ISIN code
    try:
        idx = d_isin[isin_code]
    except:
        continue

    market = get_field(f_rcv, 1, 5)			# Use last 1 digit of 2nd field, info code

    inv_code = get_field(f_rcv, 4, 27)			# A0: Batch data
    inv_s_q = int(get_field(f_rcv, 12, 39))
    inv_s_a = int(get_field(f_rcv, 18, 57))
    inv_b_q = int(get_field(f_rcv, 12, 73))
    inv_b_a = int(get_field(f_rcv, 18, 91))

    df_name_sq = 'inv_' + inv_code + '_s_q'
    df_name_sa = 'inv_' + inv_code + '_s_a'
    df_name_bq = 'inv_' + inv_code + '_b_q'
    df_name_ba = 'inv_' + inv_code + '_b_a'

    d_isin[isin_code][df_name_sq] = inv_s_q
    d_isin[isin_code][df_name_sa] = inv_s_a
    d_isin[isin_code][df_name_bq] = inv_b_q
    d_isin[isin_code][df_name_ba] = inv_b_a


for dline in d_isin:
    s_code = d_isin[dline]['scode']
    isin_code = d_isin[dline]['isin']
    market = d_isin[dline]['market']

    inv_1000_s_q = d_isin[dline]['inv_1000_s_q']
    inv_1000_s_a = d_isin[dline]['inv_1000_s_a']
    inv_1000_b_q = d_isin[dline]['inv_1000_b_q']
    inv_1000_b_a = d_isin[dline]['inv_1000_b_a']

    inv_2000_s_q = d_isin[dline]['inv_2000_s_q']
    inv_2000_s_a = d_isin[dline]['inv_2000_s_a']
    inv_2000_b_q = d_isin[dline]['inv_2000_b_q']
    inv_2000_b_a = d_isin[dline]['inv_2000_b_a']

    inv_3000_s_q = d_isin[dline]['inv_3000_s_q']
    inv_3000_s_a = d_isin[dline]['inv_3000_s_a']
    inv_3000_b_q = d_isin[dline]['inv_3000_b_q']
    inv_3000_b_a = d_isin[dline]['inv_3000_b_a']

    inv_3100_s_q = d_isin[dline]['inv_3100_s_q']
    inv_3100_s_a = d_isin[dline]['inv_3100_s_a']
    inv_3100_b_q = d_isin[dline]['inv_3100_b_q']
    inv_3100_b_a = d_isin[dline]['inv_3100_b_a']

    inv_4000_s_q = d_isin[dline]['inv_4000_s_q']
    inv_4000_s_a = d_isin[dline]['inv_4000_s_a']
    inv_4000_b_q = d_isin[dline]['inv_4000_b_q']
    inv_4000_b_a = d_isin[dline]['inv_4000_b_a']

    inv_5000_s_q = d_isin[dline]['inv_5000_s_q']
    inv_5000_s_a = d_isin[dline]['inv_5000_s_a']
    inv_5000_b_q = d_isin[dline]['inv_5000_b_q']
    inv_5000_b_a = d_isin[dline]['inv_5000_b_a']

    inv_6000_s_q = d_isin[dline]['inv_6000_s_q']
    inv_6000_s_a = d_isin[dline]['inv_6000_s_a']
    inv_6000_b_q = d_isin[dline]['inv_6000_b_q']
    inv_6000_b_a = d_isin[dline]['inv_6000_b_a']

    inv_7000_s_q = d_isin[dline]['inv_7000_s_q']
    inv_7000_s_a = d_isin[dline]['inv_7000_s_a']
    inv_7000_b_q = d_isin[dline]['inv_7000_b_q']
    inv_7000_b_a = d_isin[dline]['inv_7000_b_a']

    inv_7100_s_q = d_isin[dline]['inv_7100_s_q']
    inv_7100_s_a = d_isin[dline]['inv_7100_s_a']
    inv_7100_b_q = d_isin[dline]['inv_7100_b_q']
    inv_7100_b_a = d_isin[dline]['inv_7100_b_a']

    inv_8000_s_q = d_isin[dline]['inv_8000_s_q']
    inv_8000_s_a = d_isin[dline]['inv_8000_s_a']
    inv_8000_b_q = d_isin[dline]['inv_8000_b_q']
    inv_8000_b_a = d_isin[dline]['inv_8000_b_a']

    inv_9000_s_q = d_isin[dline]['inv_9000_s_q']
    inv_9000_s_a = d_isin[dline]['inv_9000_s_a']
    inv_9000_b_q = d_isin[dline]['inv_9000_b_q']
    inv_9000_b_a = d_isin[dline]['inv_9000_b_a']

    inv_9001_s_q = d_isin[dline]['inv_9001_s_q']
    inv_9001_s_a = d_isin[dline]['inv_9001_s_a']
    inv_9001_b_q = d_isin[dline]['inv_9001_b_q']
    inv_9001_b_a = d_isin[dline]['inv_9001_b_a']


    out_line = RD_DATE + ',' + s_code + ',' + isin_code + ',' + market + ',' \
        + str(inv_1000_s_q) + ',' + str(inv_1000_s_a) + ',' + str(inv_1000_b_q) + ',' + str(inv_1000_b_a) + ',' \
        + str(inv_2000_s_q) + ',' + str(inv_2000_s_a) + ',' + str(inv_2000_b_q) + ',' + str(inv_2000_b_a) + ',' \
        + str(inv_3000_s_q) + ',' + str(inv_3000_s_a) + ',' + str(inv_3000_b_q) + ',' + str(inv_3000_b_a) + ',' \
        + str(inv_3100_s_q) + ',' + str(inv_3100_s_a) + ',' + str(inv_3100_b_q) + ',' + str(inv_3100_b_a) + ',' \
        + str(inv_4000_s_q) + ',' + str(inv_4000_s_a) + ',' + str(inv_4000_b_q) + ',' + str(inv_4000_b_a) + ',' \
        + str(inv_5000_s_q) + ',' + str(inv_5000_s_a) + ',' + str(inv_5000_b_q) + ',' + str(inv_5000_b_a) + ',' \
        + str(inv_6000_s_q) + ',' + str(inv_6000_s_a) + ',' + str(inv_6000_b_q) + ',' + str(inv_6000_b_a) + ',' \
        + str(inv_7000_s_q) + ',' + str(inv_7000_s_a) + ',' + str(inv_7000_b_q) + ',' + str(inv_7000_b_a) + ',' \
        + str(inv_7100_s_q) + ',' + str(inv_7100_s_a) + ',' + str(inv_7100_b_q) + ',' + str(inv_7100_b_a) + ',' \
        + str(inv_8000_s_q) + ',' + str(inv_8000_s_a) + ',' + str(inv_8000_b_q) + ',' + str(inv_8000_b_a) + ',' \
        + str(inv_9000_s_q) + ',' + str(inv_9000_s_a) + ',' + str(inv_9000_b_q) + ',' + str(inv_9000_b_a) + ',' \
        + str(inv_9001_s_q) + ',' + str(inv_9001_s_a) + ',' + str(inv_9001_b_q) + ',' + str(inv_9001_b_a)
    out_f.write(out_line + '\n')

out_file_name = WDIR + '/TSDB_INV_FLOW_' + RD_DATE + '.csv'
my_cmd = 'cp ' + out_file_name + ' ' + ODIR
os.system(my_cmd)
