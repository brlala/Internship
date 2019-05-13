import pickle
from collections import defaultdict

# from bson.objectid import ObjectId
from bson.son import SON
from bson.tz_util import FixedOffset
from datetime import datetime, date, timedelta
from pymongo import MongoClient

import pprint
pp = pprint.PrettyPrinter(indent=4)

import time
import json

client = MongoClient("mongodb://")
database_fact = client["fact"]
collection_message = database_fact["message"]

# ====== Getting all bot ID(s) ===========
# TODO discuss what bot we should consider
# TODO make the code loop through all bots (if needed to scale)
database_dimension = client["dimension"]
collection_bot = database_dimension["bot"]

query = {}
projection = {"_id": 1}

cursor_bot = collection_bot.find(query, projection=projection)
bot_list = set()
for bot in cursor_bot:
    bot_list.add(bot['_id'])


# ======= Util methods ==========
def minutes_to_milliseconds(minute):
    return minute * 60 * 1000


def nested_dict():
    return defaultdict(dict)


def get_total_conversation():
    # ======= Variables for query input =========
    start_date = datetime(year=2019, month=1, day=29)  # "2019-01-29 00:00:00.000000"
    time_zone = "+0800"
    region = "Asia/Singapore"  # taken from tzlist wiki, either region or time "+08:00" will work
    interval_between_message_as_convo = minutes_to_milliseconds(3)  # minutes

    formatted_date = datetime.strptime(str(start_date), "%Y-%m-%d %H:%M:%S").replace(
        tzinfo=FixedOffset(480, time_zone))  # 2019-01-29 00:00:00+08:00

    #  ===== METHODOLOGY =====
    # 1. Filter by start date and bot receiver ID
    # 2. Group by sender_id and push them to an array { messages: [ msg1, msg2] }
    # 3. pack the array and add index into it { each_message_with_index: [ (msg1, index), (msg2, index)] }
    # 4. pack as array pair with the next entry { pairs: [ { current: msg1, prev: msg1 }, {current: msg2, prev: msg1} ] }
    # 5. unwind the list [ { current: msg1, prev: msg1 }, {current: msg2, prev: msg1} ] into elements
    # 6. add datefields(timezone already considered) { day: 1, month: 29, year:2019 { current: msg1, prev: msg1 } }
    # 7. get the difference between dates { day: 1, month: 29, year:2019 time_difference: 18460 }
    # 8. group by date then count the interval with more than time_difference specified
    # { day: 1, month: 29, year:2019, more_than_time_interval: 4 }
    # p/s note that the 4 here means there are GAPS between conversation more than the time_interval, we would need to offset this by 1
    # for each user, as when the user starts a conversation, there will be no GAPS between convo.
    # 9. group by date, month, year and sum the count
    # 10. move the field date, month, year to root level
    # 11. sort by year, month, date

    pipeline = [
        {
            "$match": {
                "created_at": {
                    "$gte": formatted_date
                },
                "$or": [
                    {
                        "receiver_id": bot_list.pop()
                    },
                    {
                        "receiver_id": bot_list.pop()
                    }
                ]
            }
        },
        {
            "$group": {
                "_id": "$sender_id",
                "messages": {
                    "$push": "$created_at"
                }
            }
        },
        {
            "$addFields": {
                "each_message_with_index": {
                    "$zip": {
                        "inputs": [
                            "$messages",
                            {
                                "$range": [
                                    0,
                                    {
                                        "$size": "$messages"
                                    }
                                ]
                            }
                        ]
                    }
                }
            }
        },
        {
            "$project": {
                "pairs": {
                    "$map": {
                        "input": "$each_message_with_index",
                        "in": {
                            "current": {
                                "$arrayElemAt": [
                                    "$$this",
                                    0
                                ]
                            },
                            "prev": {
                                "$arrayElemAt": [
                                    "$messages",
                                    {
                                        "$max": [
                                            0,
                                            {
                                                "$subtract": [
                                                    {
                                                        "$arrayElemAt": [
                                                            "$$this",
                                                            1
                                                        ]
                                                    },
                                                    1
                                                ]
                                            }
                                        ]
                                    }
                                ]
                            }
                        }
                    }
                }
            }
        },
        {
            "$unwind": {
                "path": "$pairs"
            }
        },
        {
            "$addFields": {
                "day": {
                    "$dayOfMonth": {
                        "date": "$pairs.current",
                        "timezone": region
                    }
                },
                "month": {
                    "$month": {
                        "date": "$pairs.current",
                        "timezone": region
                    }
                },
                "year": {
                    "$year": {
                        "date": "$pairs.current",
                        "timezone": region
                    }
                }
            }
        },
        {
            "$addFields": {
                "time_difference": {
                    "$subtract": [
                        "$pairs.current",
                        "$pairs.prev"
                    ]
                }
            }
        },
        {
            "$project": {
                "_id": 1,
                "day": 1,
                "month": 1,
                "year": 1,
                "more_than_time_interval": {
                    "$cond": [
                        {
                            "$or": [
                                {
                                    "$gt": [
                                        "$time_difference",
                                        interval_between_message_as_convo
                                    ]
                                },
                                {
                                    "$eq": [
                                        "$time_difference",
                                        0
                                    ]
                                }
                            ]
                        },
                        1,
                        0
                    ]
                }
            }
        },
        {
            "$group": {
                "_id": {
                    "day": "$day",
                    "month": "$month",
                    "year": "$year"
                },
                "convo_count": {
                    "$sum": "$more_than_time_interval"
                }
            }
        },
        {
            "$addFields": {
                "day": "$_id.day",
                "month": "$_id.month",
                "year": "$_id.year"
            }
        },
        {
            "$sort": SON([("year", 1), ("month", 1), ("day", 1)])
        }
    ]

    cursor = collection_message.aggregate(
        pipeline,
        allowDiskUse=False
    )

    today = date.today()
    offset = (today.weekday() - 5) % 7
    last_sunday = today - timedelta(days=offset)

    convo_results = defaultdict(nested_dict)
    total_convo = 0
    # {'month': 3, 'day': 29, 'convo_count': 281.0, 'year': 2019}
    for entries in cursor:
        year = int(entries['year'])
        month = int(entries['month'])
        day = int(entries['day'])
        entry_message_count = entries['convo_count']
        convo_results[year][month][day] = entry_message_count

        # other variables
        total_convo += entry_message_count

        # getting yearly message
        yearly_total = convo_results[year].get('yearly_total_message')
        yearly_total = 0 if yearly_total is None else yearly_total
        new_yearly_total = yearly_total + entry_message_count
        convo_results[year]['yearly_total_message'] = new_yearly_total

        # getting monthly
        monthly_total = convo_results[year][month].get('monthly_total_message')
        monthly_total = 0 if monthly_total is None else monthly_total
        new_monthly_total = monthly_total + entry_message_count
        convo_results[year][month]['monthly_total_message'] = new_monthly_total

        # getting weekly
        convo_date = date(year=year, month=month, day=day)

        if convo_date > last_sunday:
            weekly_total = convo_results.get('wtd')
            weekly_total = 0 if weekly_total is None else weekly_total
            new_weekly_total = weekly_total + entry_message_count
            convo_results['wtd'] = new_weekly_total

    # adding cumulative for each month, taking logic out for readability
    cumulative = 0
    for cumulative_month in range(1, month + 1):
        cumulative += convo_results[year][cumulative_month]['monthly_total_message']
        convo_results[year][cumulative_month]['cumulative_by_year'] = cumulative

    # compiling stats at root level, this is redundant and can be removed
    # TODO to check whethere we want to reset
    convo_results['mtd'] = convo_results[year][month]['monthly_total_message']
    convo_results['today'] = convo_results[year][month][day]
    convo_results['total'] = total_convo
    convo_results['ytd'] = convo_results[year]['yearly_total_message']

    return json.loads(json.dumps(convo_results))


# start_stopwatch = time.time()
# results = get_total_conversation()
# end_stopwatch = time.time()
# pp.pprint(results)

# print(end_stopwatch - start_stopwatch)  # 1.52 seconds on a slow slow computer :<