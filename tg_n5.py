from nsetools import Nse
import pymongo
import pandas_market_calendars as mcal
from datetime import date, timedelta, datetime

# initialize mongodb
myclient = pymongo.MongoClient()
mydb = myclient["Nifty50"]
mycol = mydb["nifty_gainers"]

# initialize Nse
nse = Nse()
tg5 = nse.get_top_gainers()
# print(tg5)

# dates
nyse = mcal.get_calendar('NSE')
current_d = date.today()
# current_d = date(2022, 8, 19)
day_diff = timedelta(10)
prev_d = current_d - day_diff

early = nyse.schedule(start_date=prev_d, end_date=current_d)
m_date = str(early['market_open'][-1])
m_date = m_date[0:10]
m_date = datetime.strptime(m_date, '%Y-%m-%d').strftime('%d-%B-%Y')

now = datetime.now()
current_time = now.strftime(" %H:%M")

m_date = str(m_date) + str(current_time)
print(m_date)

# fetching stock data

# mycol.delete_many({})    uncomment to remove previous data when storing new

mylist = []


def send_data():
    for i in range(5):
        # diff = (tg5[i]['openPrice'] - tg5[i]['previousPrice'] )/ tg5[i]['previousPrice']
        ltp = tg5[i]['ltp']
        closes = tg5[i]['previousPrice']
        per_gain = round((ltp - closes) * 100 / closes, 2)
        per_gain = format(per_gain, '.2f')
        # top_5 = {"date": m_date, "symbol": tg5[i]['symbol'], "close": tg5[i]['previousPrice'],
        #          "ltp": tg5[i]['ltp'],
        #          '% gain': per_gain}

        top_5 = {"symbol": tg5[i]['symbol'], "close": tg5[i]['previousPrice'],
                 "ltp": tg5[i]['ltp'],
                 'gain': per_gain}

        mylist.append(top_5)

    # mycol.insert_many(mylist)
    mycol.insert_one({'date': m_date, 'results': mylist})

    print("--Data insertion complete for {}!!".format(m_date))


temp_empty = mycol.find()
check_empty = list(temp_empty)

if len(check_empty) == 0:
    send_data()

else:
    latest_doc = mycol.find().sort("_id", pymongo.DESCENDING).limit(1)
    last_entry = latest_doc[0]["date"]

    if m_date[0:15] == last_entry[0:15]:
        print("Entry already exists for {}!!".format(m_date[0:15]))
    else:
        send_data()
