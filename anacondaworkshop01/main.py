from util import Util
import conf

if __name__ == '__main__':
    for date in conf.DATES:
        for fund, pattern in conf.FUNDS.items():
           print(fund + " " + pattern)
