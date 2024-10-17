# Convert A0 Data to TSDB csv format
# A0 Data in KRX new protocol (2023.01.21)


import os
import sys
import datetime
import codecs
import math
import shutil

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
ODIR = '/home/trading/server/prep-data/tsdb/a0/'
#BDIR = '/home/trading/prep-data/bat/'
print('Working directory: ' + WDIR)
if not os.path.exists(WDIR):
	print('making working directory...')
	os.system('mkdir ' + WDIR)

RD_DATE = YY + '-' + MM + '-' + DD

#### ISIN Table and Dictionary
#bat_list = []
#d_isin = {}

def get_field(line, length, acc_length):
	begin = acc_length - length
	try:
		ret = str(line[begin:begin+length].decode('cp949'))
		return ret
	except:
		return str(line[begin:begin+length])


# Grep A001 lines
my_cmd = 'grep -a \'A001S\|A001Q\' ' + FILE_NAME + ' > ' + WDIR + '/A001'
print(my_cmd)

#OFILE_NAME = 'batch_' + RD_DATE

# Check files if exist
if os.system('ls ' + WDIR + '/A001 > /dev/null') != 0:
	print('making file A001...')
	os.system(my_cmd)


out_f = open(WDIR + '/TSDB_BATCH_' + RD_DATE + '.csv', 'w')
#out_f.write('f1,f2,f3,f4,f5,f6,f7,f8,f9,f10,f11,f12,f13,f14,f15,f16,f17,f18,f19,f20,f21,f22,f23,f24,f25,f26,f27,f28,f29,f30,f31,f32,f33,f34,f35,f36,f37,f38,f39,f40,f41,f42,f43,f44,f45,f46,f47,f48,f49,f50,f51,f52,f53,f54,f55,f56,f57,f58,f59,f60,f61,f62,f63,f64,f65,f66,f67,f68,f69,f70,f71,f72,f73,f74,f75,f76,f77,f78,f79,f80,f81,f82,f83,f84,f85,f86,f87,f88,f89,f90,f91,f92,f93,f94,f95,f96,f97,f98,f99,f100,f101,f102,f103,f104,f105,f106,f107,f108,f109,f110,f111,f112,f113,f114,f115,f116,f117,f118,f119,f120,f121,f122,f123,f124,f125,f126,f127,f128,f129,f130,f131,f132,f133,f134,f135,f136,f137,f138,f139,f140,f141,f142,f143,f144,f145,f146,f147,f148,f149,f150,f151,f152,f153,f154,f155,f156,f157,f158,f159,f160,f161,f162,f163,f164,f165,f166,f167,f168,f169' + '\n')
out_f.write('isin|short_code|name_korean|name_english|security_type|industry_id|listed_date|listed_qty|listed_market|stock_type|is_spac' + '\n')

fpdname = WDIR + '/A001'
stk_list = []
ISIN_FOUND = 0
ff001 = codecs.open(fpdname, 'rb')
for ff_line in ff001:

	f_rcv = ff_line[12:]

	tr_code = get_field(f_rcv, 2, 2)			# A0: Batch data
	isin_code = get_field(f_rcv, 12, 39)			# Stock ISIN code
	group_id = get_field(f_rcv, 2, 144)			# Instrument Group ID: ST(Stock), DR(DR), EF(ETF), FS(Foreign Stock)

	if tr_code != 'A0':
		continue

	if group_id != 'ST':
		continue

	# Check ISIN
	for i in range(len(stk_list)):
		if isin_code == stk_list[i][:12]:
			ISIN_FOUND = 1
			break

	if ISIN_FOUND == 1:
		ISIN_FOUND = 0
		continue

	a0_f1 = get_field(f_rcv, 2, 2)
	a0_f2 = get_field(f_rcv, 3, 5)
	a0_f3 = get_field(f_rcv, 8, 13)
	a0_f4 = get_field(f_rcv, 6, 19)
	a0_f5 = get_field(f_rcv, 8, 27)
	a0_f6 = get_field(f_rcv, 12, 39)	# ISIN code
	a0_f7 = get_field(f_rcv, 6, 45)
	a0_f8 = get_field(f_rcv, 9, 54)		# Short code
	a0_f9 = get_field(f_rcv, 40, 94)	# Korean name
	a0_f10 = get_field(f_rcv, 40, 134)	# English name
	a0_f11 = get_field(f_rcv, 5, 139)
	a0_f12 = get_field(f_rcv, 3, 142)
	a0_f13 = get_field(f_rcv, 2, 144)	# Group ID
	a0_f14 = get_field(f_rcv, 1, 145)
	a0_f15 = get_field(f_rcv, 2, 147)
	a0_f16 = get_field(f_rcv, 2, 149)
	a0_f17 = get_field(f_rcv, 1, 150)
	a0_f18 = get_field(f_rcv, 2, 152)
	a0_f19 = get_field(f_rcv, 1, 153)
	a0_f20 = get_field(f_rcv, 1, 154)
	a0_f21 = get_field(f_rcv, 1, 155)
	a0_f22 = get_field(f_rcv, 2, 157)
	a0_f23 = get_field(f_rcv, 1, 158)
	a0_f24 = get_field(f_rcv, 1, 159)
	a0_f25 = get_field(f_rcv, 1, 160)
	a0_f26 = get_field(f_rcv, 1, 161)
	a0_f27 = get_field(f_rcv, 1, 162)
	a0_f28 = get_field(f_rcv, 10, 172)		# Industry ID
	a0_f29 = get_field(f_rcv, 1, 173)
	a0_f30 = get_field(f_rcv, 1, 174)
	a0_f31 = get_field(f_rcv, 1, 175)
	a0_f32 = get_field(f_rcv, 11, 186)
	a0_f33 = get_field(f_rcv, 1, 187)
	a0_f34 = get_field(f_rcv, 11, 198)
	a0_f35 = get_field(f_rcv, 12, 210)
	a0_f36 = get_field(f_rcv, 22, 232)
	a0_f37 = get_field(f_rcv, 11, 243)
	a0_f38 = get_field(f_rcv, 11, 254)
	a0_f39 = get_field(f_rcv, 11, 265)
	a0_f40 = get_field(f_rcv, 11, 276)
	a0_f41 = get_field(f_rcv, 11, 287)
	a0_f42 = get_field(f_rcv, 8, 295)		# Listed date
	a0_f43 = get_field(f_rcv, 16, 311)		# Listed quantity
	a0_f44 = get_field(f_rcv, 1, 312)
	a0_f45 = get_field(f_rcv, 8, 320)
	a0_f46 = get_field(f_rcv, 8, 328)
	a0_f47 = get_field(f_rcv, 8, 336)
	a0_f48 = get_field(f_rcv, 8, 344)
	a0_f49 = get_field(f_rcv, 13, 357)
	a0_f50 = get_field(f_rcv, 22, 379)
	a0_f51 = get_field(f_rcv, 1, 380)
	a0_f52 = get_field(f_rcv, 5, 385)
	a0_f53 = get_field(f_rcv, 5, 390)
	a0_f54 = get_field(f_rcv, 5, 395)
	a0_f55 = get_field(f_rcv, 5, 400)
	a0_f56 = get_field(f_rcv, 5, 405)
	a0_f57 = get_field(f_rcv, 2, 407)
	a0_f58 = get_field(f_rcv, 1, 408)		# 0: Common stock, 1: Preferred stock old, 2: Preferred stock new
	a0_f59 = get_field(f_rcv, 1, 409)
	a0_f60 = get_field(f_rcv, 11, 420)
	a0_f61 = get_field(f_rcv, 11, 431)
	a0_f62 = get_field(f_rcv, 11, 442)
	a0_f63 = get_field(f_rcv, 11, 453)
	a0_f64 = get_field(f_rcv, 11, 464)
	a0_f65 = get_field(f_rcv, 1, 465)
	a0_f66 = get_field(f_rcv, 12, 477)
	a0_f67 = get_field(f_rcv, 3, 480)
	a0_f68 = get_field(f_rcv, 3, 483)
	a0_f69 = get_field(f_rcv, 1, 484)
	a0_f70 = get_field(f_rcv, 1, 485)
	a0_f71 = get_field(f_rcv, 1, 486)
	a0_f72 = get_field(f_rcv, 1, 487)
	a0_f73 = get_field(f_rcv, 1, 488)
	a0_f74 = get_field(f_rcv, 1, 489)
	a0_f75 = get_field(f_rcv, 1, 490)
	a0_f76 = get_field(f_rcv, 13, 503)
	a0_f77 = get_field(f_rcv, 1, 504)
	a0_f78 = get_field(f_rcv, 1, 505)		# SPAC or not
	a0_f79 = get_field(f_rcv, 1, 506)
	a0_f80 = get_field(f_rcv, 13, 519)
	a0_f81 = get_field(f_rcv, 1, 520)
	a0_f82 = get_field(f_rcv, 8, 528)
	a0_f83 = get_field(f_rcv, 1, 529)
	a0_f84 = get_field(f_rcv, 1, 530)
	a0_f85 = get_field(f_rcv, 8, 538)
	a0_f86 = get_field(f_rcv, 2, 540)
	a0_f87 = get_field(f_rcv, 8, 548)
	a0_f88 = get_field(f_rcv, 8, 556)
	a0_f89 = get_field(f_rcv, 1, 557)
	a0_f90 = get_field(f_rcv, 2, 559)
	a0_f91 = get_field(f_rcv, 6, 565)
	a0_f92 = get_field(f_rcv, 3, 568)
	a0_f93 = get_field(f_rcv, 2, 570)
	a0_f94 = get_field(f_rcv, 2, 572)
	a0_f95 = get_field(f_rcv, 6, 578)
	a0_f96 = get_field(f_rcv, 6, 584)
	a0_f97 = get_field(f_rcv, 5, 589)
	a0_f98 = get_field(f_rcv, 1, 590)
	a0_f99 = get_field(f_rcv, 1, 591)
	a0_f100 = get_field(f_rcv, 1, 592)
	a0_f101 = get_field(f_rcv, 23, 615)
	a0_f102 = get_field(f_rcv, 1, 616)
	a0_f103 = get_field(f_rcv, 1, 617)
	a0_f104 = get_field(f_rcv, 1, 618)
	a0_f105 = get_field(f_rcv, 1, 619)
	a0_f106 = get_field(f_rcv, 1, 620)

	#a0_f6 = get_field(f_rcv, 12, 39)	# ISIN code
	#a0_f8 = get_field(f_rcv, 9, 54)		# Short code
	#a0_f9 = get_field(f_rcv, 40, 94)	# Korean name
	#a0_f10 = get_field(f_rcv, 40, 134)	# English name
	#a0_f13 = get_field(f_rcv, 2, 144)	# Group ID
	#a0_f28 = get_field(f_rcv, 10, 172)		# Industry ID
	#a0_f42 = get_field(f_rcv, 8, 295)		# Listed date
	#a0_f43 = get_field(f_rcv, 16, 311)		# Listed quantity

	#a0_f58 = get_field(f_rcv, 1, 408)		# 0: Common stock, 1: Preferred stock old, 2: Preferred stock new
	#a0_f78 = get_field(f_rcv, 1, 505)		# SPAC or not

	tmp_str = isin_code
	market = get_field(f_rcv, 1, 5)

	a0_str = a0_f6 + '|' + a0_f8 + '|' + a0_f9 + '|' + a0_f10 + '|' + a0_f13 + '|' + a0_f28 + '|' + a0_f42 + '|' + a0_f43 + '|' + market + '|' + a0_f58 + '|' + a0_f78
	out_f.write(a0_str + '\n')
	stk_list.append(tmp_str)


	print(a0_str)
	#print(get_field(f_rcv, 9, 339))



# Check ISIN counts
print('Date: ' + RD_DATE + '  ISIN count: ' + str(len(stk_list)))

out_file_name = WDIR + '/TSDB_BATCH_' + RD_DATE + '.csv'
my_cmd = 'cp ' + out_file_name + ' ' + ODIR
os.system(my_cmd)
