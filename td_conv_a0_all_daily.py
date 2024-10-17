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


out_f = open(WDIR + '/TSDB_BATCH_ALL_' + RD_DATE + '.csv', 'w')

### Record Header
header_str = ('date,product,market,isin_code,short_code,korean_name,english_name,group_id,ex_right_div,base_price_changed,market_warning_code,unfaithful_pd,stop_trading,industry_id,small_mid_cap_category,company_size_code'
	',base_price,prevday_close_price,prevday_accumulated_trade_count,prevday_accumulated_trade_amount,price_upper_limit,price_lower_limit,collateral_price,listed_date,listed_quantity,clear_sale_period,stock_loan_trade_available'
	',capital_increase_type,stock_type,country_code,short_sell_available,is_spac,market_overheat_warning_code\n'
)
out_f.write(header_str)

###### Field names
# date
# product
# market
# isin_code
# short_code
# korean_name
# english_name
# group_id
# ex_right_div
# base_price_changed
# market_warning_code
# unfaithful_pd
# stop_trading
# industry_id
# small_mid_cap_category
# company_size_code'
# base_price
# prevday_close_price
# prevday_accumulated_trade_count
# prevday_accumulated_trade_amount
# price_upper_limit
# price_lower_limit
# collateral_price
# listed_date
# listed_quantity
# clear_sale_period
# stock_loan_trade_available'
# capital_increase_type
# stock_type
# country_code
# short_sell_available
# is_spac
# market_overheat_warning_code

# Date
# Product
# Market
# ISIN code
# Short code
# Korean name
# English name
# Group ID
# EX-RD (Right, Dividend, etc)
# Whether base price has changed
# Market warning code
# Unfaithful PD
# Stop trading
# Industry ID
# Small-mid cap category
# Company size code
# Base price
# Previous day close price
# Previous day accumulated trade count
# Previous day accumulated trade amount/volume
# Price upper limit
# Price lower limit
# Collateral price
# Listed date
# Listed quantity
# Clear sale period
# Stock loan trade available or not
# Capital increase type
# 0: Common stock, 1: Preferred stock old, 2: Preferred stock new
# Country code
# Short sell available or not
# SPAC or not
# Market overheat warning code

fpdname = WDIR + '/A001'
stk_list = []
ISIN_FOUND = 0

#ff001 = codecs.open(fpdname, 'rb')
with open(fpdname, 'rb') as ff001:
	for ff_line in ff001:

		f_rcv = ff_line[12:]
		tr_code = get_field(f_rcv, 2, 2)			# A0: Batch data
		isin_code = get_field(f_rcv, 12, 39)			# Stock ISIN code
		group_id = get_field(f_rcv, 2, 144)			# Instrument Group ID: ST(Stock), DR(DR), EF(ETF), FS(Foreign Stock)

		if tr_code != 'A0':
			continue

		#if group_id != 'ST':
		#	continue

		# Check ISIN
		for i in range(len(stk_list)):
			if isin_code == stk_list[i][:12]:
				ISIN_FOUND = 1
				break

		if ISIN_FOUND == 1:
			ISIN_FOUND = 0
			continue

		a0_f1 = get_field(f_rcv, 2, 2)
		a0_f2 = get_field(f_rcv, 3, 5)			# Market(last 1 digit)
		if a0_f2[:2] != '01':
			continue

		a0_f3 = get_field(f_rcv, 8, 13)
		a0_f4 = get_field(f_rcv, 6, 19)
		a0_f5 = get_field(f_rcv, 8, 27)
		a0_f6 = get_field(f_rcv, 12, 39)		# ISIN code

		if a0_f6 == '999999999999':
			continue

		a0_f7 = get_field(f_rcv, 6, 45)
		a0_f8 = get_field(f_rcv, 9, 54)			# Short code
		a0_f9 = get_field(f_rcv, 40, 94)		# Korean name
		a0_f10 = get_field(f_rcv, 40, 134)		# English name
		a0_f11 = get_field(f_rcv, 5, 139)
		a0_f12 = get_field(f_rcv, 3, 142)
		a0_f13 = get_field(f_rcv, 2, 144)		# Group ID
		a0_f14 = get_field(f_rcv, 1, 145)
		a0_f15 = get_field(f_rcv, 2, 147)		# EX-RD (Right, Dividend, etc)
		a0_f16 = get_field(f_rcv, 2, 149)
		a0_f17 = get_field(f_rcv, 1, 150)
		a0_f18 = get_field(f_rcv, 2, 152)
		a0_f19 = get_field(f_rcv, 1, 153)		# Whether base price has changed
		a0_f20 = get_field(f_rcv, 1, 154)
		a0_f21 = get_field(f_rcv, 1, 155)
		a0_f22 = get_field(f_rcv, 2, 157)		# Market warning code
		a0_f23 = get_field(f_rcv, 1, 158)
		a0_f24 = get_field(f_rcv, 1, 159)
		a0_f25 = get_field(f_rcv, 1, 160)		# Unfaithful PD
		a0_f26 = get_field(f_rcv, 1, 161)
		a0_f27 = get_field(f_rcv, 1, 162)		# Stop trading
		a0_f28 = get_field(f_rcv, 10, 172)		# Industry ID
		a0_f29 = get_field(f_rcv, 1, 173)		# Small-mid cap category
		a0_f30 = get_field(f_rcv, 1, 174)		# Company size code
		a0_f31 = get_field(f_rcv, 1, 175)
		a0_f32 = get_field(f_rcv, 11, 186)		# Base price
		a0_f33 = get_field(f_rcv, 1, 187)
		a0_f34 = get_field(f_rcv, 11, 198)		# Previous day close price
		a0_f35 = get_field(f_rcv, 12, 210)		# Previous day accumulated trade count
		a0_f36 = get_field(f_rcv, 22, 232)		# Previous day accumulated trade amount/volume
		a0_f37 = get_field(f_rcv, 11, 243)		# Price upper limit
		a0_f38 = get_field(f_rcv, 11, 254)		# Price lower limit
		a0_f39 = get_field(f_rcv, 11, 265)		# Collateral price
		a0_f40 = get_field(f_rcv, 11, 276)
		a0_f41 = get_field(f_rcv, 11, 287)
		a0_f42 = get_field(f_rcv, 8, 295)		# Listed date
		a0_f43 = get_field(f_rcv, 16, 311)		# Listed quantity
		a0_f44 = get_field(f_rcv, 1, 312)		# Clear sale period
		a0_f45 = get_field(f_rcv, 8, 320)
		a0_f46 = get_field(f_rcv, 8, 328)
		a0_f47 = get_field(f_rcv, 8, 336)
		a0_f48 = get_field(f_rcv, 8, 344)
		a0_f49 = get_field(f_rcv, 13, 357)
		a0_f50 = get_field(f_rcv, 22, 379)
		a0_f51 = get_field(f_rcv, 1, 380)		# Stock loan trade available or not
		a0_f52 = get_field(f_rcv, 5, 385)
		a0_f53 = get_field(f_rcv, 5, 390)
		a0_f54 = get_field(f_rcv, 5, 395)
		a0_f55 = get_field(f_rcv, 5, 400)
		a0_f56 = get_field(f_rcv, 5, 405)
		a0_f57 = get_field(f_rcv, 2, 407)		# Capital increase type
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
		a0_f68 = get_field(f_rcv, 3, 483)		# Country code
		a0_f69 = get_field(f_rcv, 1, 484)
		a0_f70 = get_field(f_rcv, 1, 485)
		a0_f71 = get_field(f_rcv, 1, 486)
		a0_f72 = get_field(f_rcv, 1, 487)
		a0_f73 = get_field(f_rcv, 1, 488)
		a0_f74 = get_field(f_rcv, 1, 489)
		a0_f75 = get_field(f_rcv, 1, 490)		# Short sell available or not
		a0_f76 = get_field(f_rcv, 13, 503)
		a0_f77 = get_field(f_rcv, 1, 504)
		a0_f78 = get_field(f_rcv, 1, 505)		# SPAC or not
		a0_f79 = get_field(f_rcv, 1, 506)
		a0_f80 = get_field(f_rcv, 13, 519)
		a0_f81 = get_field(f_rcv, 1, 520)
		a0_f82 = get_field(f_rcv, 8, 528)
		a0_f83 = get_field(f_rcv, 1, 529)		# Market overheat warning code
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

		a0_str = ''
		a0_str = a0_str + RD_DATE
		for i in range(1,106):
			if (i == 2):  		# Market(last 1 digit)
				a0_str = a0_str + '|' + a0_f2[:2] + '|' + a0_f2[2:3]

			elif (i == 6  		# ISIN code
			 or i == 8  		# Short code
			 or i == 9  		# Korean name
			 or i == 10 		# English name
			 or i == 13 		# Group ID
			 or i == 15 		# EX-RD (Right, Dividend, etc)
			 or i == 19 		# Whether base price has changed
			 or i == 22 		# Market warning code
			 or i == 25 		# Unfaithful PD
			 or i == 27 		# Stop trading
			 or i == 28 		# Industry ID
			 or i == 29 		# Small-mid cap category
			 or i == 30 		# Company size code
			 or i == 32 		# Base price
			 or i == 34 		# Previous day close price
			 or i == 35 		# Previous day accumulated trade count
			 or i == 36 		# Previous day accumulated trade amount/volume
			 or i == 37 		# Price upper limit
			 or i == 38 		# Price lower limit
			 or i == 39 		# Collateral price
			 or i == 42 		# Listed date
			 or i == 43 		# Listed quantity
			 or i == 44 		# Clear sale period
			 or i == 51 		# Stock loan trade available or not
			 or i == 57 		# Capital increase type
			 or i == 58 		# 0: Common stock, 1: Preferred stock old, 2: Preferred stock new
			 or i == 68 		# Country code
			 or i == 75 		# Short sell available or not
			 or i == 78 		# SPAC or not
			 or i == 83): 		# Market overheat warning code
				exec(f'a0_str = a0_str + \'|\' + a0_f{i}')

		out_f.write(a0_str + '\n')
		tmp_str = isin_code
		stk_list.append(tmp_str)
		print(a0_str)

# Check ISIN counts
print('Date: ' + RD_DATE + '  ISIN count: ' + str(len(stk_list)))

out_file_name = WDIR + '/TSDB_BATCH_ALL_' + RD_DATE + '.csv'
my_cmd = 'cp ' + out_file_name + ' ' + ODIR
os.system(my_cmd)
