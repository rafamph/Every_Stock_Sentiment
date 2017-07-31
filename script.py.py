import operator
import pickle
from time import sleep
import requests
from ftplib import FTP
from io import BytesIO
from pprint import pprint as pp


def list_of_stocks():
    data = BytesIO()
    with FTP("ftp.nasdaqtrader.com") as ftp:  # use context manager to avoid
        ftp.login()  # leaving connection open by mistake
        ftp.retrbinary("RETR /SymbolDirectory/nasdaqlisted.txt", data.write)
    data.seek(0)  # need to go back to the beginning to get content
    nasdaq = data.read().decode()  # convert bytes back to string

    def solve(s):
        s = s.split('\n', 1)[-1]
        if s.find('\n') == -1:
            return ''
        return s.rsplit('\n', 1)[0]

    def remove_last_line_from_string(s):
        return s[:s.rfind('\n')]

    nasdaq = solve(nasdaq)
    nasdaq = remove_last_line_from_string(nasdaq)

    data = BytesIO()
    with FTP("ftp.nasdaqtrader.com") as ftp:  # use context manager to avoid
        ftp.login()  # leaving connection open by mistake
        ftp.retrbinary("RETR /SymbolDirectory/otherlisted.txt", data.write)
    data.seek(0)  # need to go back to the beginning to get content
    other = data.read().decode()  # convert bytes back to string

    other = solve(other)

    other = remove_last_line_from_string(other)

    stocks = other + nasdaq

    stocks = ('\n'.join(share.split('|')[0] for share in stocks.splitlines()))

    return stocks


def stocks_sentiment(z):
    z = z.upper()

    link = 'https://api.stocktwits.com/api/2/streams/symbol/' + z + '.json'
    while True:
        a = requests.get(link)
        a = a.json()
        if a['response']['status'] == 200:
            break
        elif a['response']['status'] == 404:
            return a['errors'][0]
        else:
            print(a['errors'][0])
            print('Sleeping one hour')
            sleep(60*60)
            continue
    from collections import Counter
    sentiment_dict = Counter()
    for message in a['messages']:
        if 'entities' in message:
            if 'sentiment' in message['entities']:
                sentiment = message['entities']['sentiment']
                if sentiment is not None:
                    sentiment = sentiment['basic']
                    sentiment_dict[sentiment] += 1

    sentiment_dict = dict(sentiment_dict)
    sentiment_dict['stock_name'] = z
    return sentiment_dict


def DUMP(object, key):
    pickle.dump(object, open(key + '.dump', 'wb'))


def LOAD(key):
    return pickle.load(open(key + '.dump', 'rb'))


def main():
    try:
        dump = LOAD('DUMP')
    except:
        dump = {}
        print('there is no dump')
    else:
        print('dump is loaded')

    all_stocks = list_of_stocks().split()
    print("All Stocks: ", len(all_stocks))

    all_stocks = [i for i in all_stocks if i not in dump]
    print("Rest Stocks: ", len(all_stocks))

    # pp("all stocks\n", all_stocks)


    for i, each in enumerate(all_stocks, 1):
        each_stat = stocks_sentiment(each)
        print(i, " / ", len(all_stocks), each_stat)
        dump[each] = each_stat
        DUMP(dump, 'DUMP')

    stocks = {i: dump[i] for i in dump if dump[i].get('message') is None}

    stocks = {i: stocks[i].get('Bullish', 0) for i in stocks if stocks[i].get('Bearish') is None}

    sorted_x = sorted(stocks.items(), key=operator.itemgetter(1))

    print("Stock, Bullish \n")
    pp(sorted_x)

    print("Last 5 stocks with highest Bullish")
    pp(sorted_x[-5:])


if __name__ == '__main__': main()
