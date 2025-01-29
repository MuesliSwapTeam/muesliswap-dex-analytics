from . import util
from pprint import pprint
 
if __name__ == "__main__":
    print("############################################")
    print(" Daily volume: ")
    pprint(util.get_daily_volume())
    print("############################################")
    print(" Monthly volume: ")
    pprint(util.get_monthly_volume())
    print("############################################")
    print(" Yearly volume: ")
    pprint(util.get_yearly_volume())